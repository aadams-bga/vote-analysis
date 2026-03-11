import os
import pandas as pd
import requests
from supabase import create_client, Client
from datetime import datetime

# Supabase Configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables are required.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Data Source
CSV_URL = "https://ilga.gov/documents/reports/static/104MemberVotes.csv"

def get_margin_required(vote_type, vote_date_str):
    """
    Logic: 1/1 to 5/31 inclusive = "Simple Majority". 6/1 to 12/31 = "3/5s Majority". 
    Exceptions: If vote_type is "Appointment Message" or "Procedural", it ALWAYS requires a "Simple Majority".
    """
    if vote_type in ["Appointment Message", "Procedural"]:
        return "Simple Majority"
    
    try:
        vote_date = datetime.strptime(vote_date_str, "%m/%d/%Y")
        month = vote_date.month
        day = vote_date.day
        
        # 1/1 to 5/31
        if month < 6:
            return "Simple Majority"
        else:
            return "3/5s Majority"
    except Exception:
        return "Simple Majority" # Default fallback

def run_etl():
    print(f"Downloading data from {CSV_URL}...")
    response = requests.get(CSV_URL)
    response.raise_for_status()
    
    # Save temp file
    with open("temp_votes.csv", "wb") as f:
        f.write(response.content)
    
    df = pd.read_csv("temp_votes.csv")
    
    # Clean column names (strip whitespace)
    df.columns = [c.strip() for c in df.columns]

    # Expected columns based on prompt: bill_number, member_name, vote_cast, vote_date, vote_type, party, city, district
    # Note: Actual CSV column names might differ, this script assumes the mapping.
    
    members_data = []
    bills_data = []
    votes_data = []

    # Process Members and Bills first to handle relationships
    # This is a simplified version. In a real scenario, we'd handle deduplication more robustly.
    
    unique_members = df[['member_name', 'party', 'city', 'district']].drop_duplicates()
    for _, row in unique_members.iterrows():
        # Calculate Metrics per member
        member_votes = df[df['member_name'] == row['member_name']]
        
        # Metric 1: count of 'excused' + 'absent'
        m1 = len(member_votes[member_votes['vote_cast'].str.lower().isin(['excused', 'absent'])])
        
        # Metric 2: count of 'excused' + 'absent' + 'no vote' + 'not voting'
        m2 = len(member_votes[member_votes['vote_cast'].str.lower().isin(['excused', 'absent', 'no vote', 'not voting'])])
        
        members_data.append({
            "name": row['member_name'],
            "party": row['party'],
            "city": row['city'],
            "district": row['district'],
            "metric1_absences": m1,
            "metric2_absences": m2,
            "active_status": True
        })

    unique_bills = df[['bill_number', 'vote_date', 'vote_type']].drop_duplicates()
    for _, row in unique_bills.iterrows():
        margin = get_margin_required(row['vote_type'], str(row['vote_date']))
        bills_data.append({
            "bill_number": row['bill_number'],
            "vote_date": row['vote_date'],
            "vote_type": row['vote_type'],
            "margin_required": margin,
            "passed": None # This would normally be calculated from vote totals
        })

    print("Upserting members...")
    supabase.table("members").upsert(members_data, on_conflict="name,district").execute()
    
    print("Upserting bills...")
    supabase.table("bills").upsert(bills_data, on_conflict="bill_number").execute()

    # Get IDs for foreign keys
    res_members = supabase.table("members").select("id, name, district").execute()
    member_map = {(m['name'], m['district']): m['id'] for m in res_members.data}

    res_bills = supabase.table("bills").select("id, bill_number").execute()
    bill_map = {b['bill_number']: b['id'] for b in res_bills.data}

    for _, row in df.iterrows():
        m_id = member_map.get((row['member_name'], row['district']))
        b_id = bill_map.get(row['bill_number'])
        
        if m_id and b_id:
            votes_data.append({
                "bill_id": b_id,
                "member_id": m_id,
                "vote_cast": row['vote_cast']
            })

    print(f"Upserting {len(votes_data)} votes...")
    # Batch upsert to avoid request size limits
    batch_size = 1000
    for i in range(0, len(votes_data), batch_size):
        supabase.table("votes").upsert(votes_data[i:i+batch_size], on_conflict="bill_id,member_id").execute()

    print("ETL Pipeline completed successfully.")

if __name__ == "__main__":
    run_etl()
