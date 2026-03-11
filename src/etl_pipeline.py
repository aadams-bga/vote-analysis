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
    Exceptions: If vote_type contains "Appointment Message" or "Procedural", it ALWAYS requires a "Simple Majority".
    """
    if any(keyword in str(vote_type) for keyword in ["Appointment Message", "Procedural"]):
        return "Simple Majority"
    
    try:
        # Expected format from CSV: YYYY-MM-DD
        vote_date = datetime.strptime(str(vote_date_str), "%Y-%m-%d")
        month = vote_date.month
        
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
    
    # Read CSV with proper headers
    df = pd.read_csv("temp_votes.csv")
    
    # Replace NaN with None for JSON compliance
    df = df.where(pd.notnull(df), None)
    
    # Clean column names
    df.columns = [c.strip() for c in df.columns]
    print(f"Columns found: {df.columns.tolist()}")

    # Mapping based on actual CSV headers: 
    # GA, Member, Chamber, Session, Legislation, Vote Type, Vote Date, Vote
    
    # Standardize data for processing
    df['member_name'] = df['Member']
    df['bill_number'] = df['Legislation']
    df['vote_cast'] = df['Vote']
    df['v_date'] = df['Vote Date']
    df['v_type'] = df['Vote Type']

    members_data = []
    bills_data = []
    votes_data = []

    # Process Members
    unique_members = df['member_name'].unique()
    for member in unique_members:
        member_votes = df[df['member_name'] == member]
        
        # Metric 1: count of 'excused' + 'absent'
        m1 = len(member_votes[member_votes['vote_cast'].str.lower().isin(['excused', 'absent'])])
        
        # Metric 2: count of 'excused' + 'absent' + 'no vote' + 'not voting'
        m2 = len(member_votes[member_votes['vote_cast'].str.lower().isin(['excused', 'absent', 'no vote', 'not voting'])])
        
        # Since party/city/district aren't in this CSV, we use placeholders or keep existing if updating
        members_data.append({
            "name": member,
            "metric1_absences": m1,
            "metric2_absences": m2,
            "active_status": True
            # Note: party, city, district will remain NULL unless previously set
        })

    # Process Bills
    unique_bills = df[['bill_number', 'v_date', 'v_type']].drop_duplicates()
    for _, row in unique_bills.iterrows():
        margin = get_margin_required(row['v_type'], row['v_date'])
        bills_data.append({
            "bill_number": row['bill_number'],
            "vote_date": row['v_date'],
            "vote_type": row['v_type'],
            "margin_required": margin,
            "passed": None # Calculated from totals in a real scenario
        })

    print(f"Upserting {len(members_data)} members...")
    # Using 'name' as unique identifier since 'district' isn't available in this CSV
    # We might need to adjust the DB unique constraint if names aren't unique enough
    supabase.table("members").upsert(members_data, on_conflict="name").execute()
    
    print(f"Upserting {len(bills_data)} bills...")
    supabase.table("bills").upsert(bills_data, on_conflict="bill_number").execute()

    # Get IDs for foreign keys
    res_members = supabase.table("members").select("id, name").execute()
    member_map = {m['name']: m['id'] for m in res_members.data}

    res_bills = supabase.table("bills").select("id, bill_number").execute()
    bill_map = {b['bill_number']: b['id'] for b in res_bills.data}

    for _, row in df.iterrows():
        m_id = member_map.get(row['member_name'])
        b_id = bill_map.get(row['bill_number'])
        
        if m_id and b_id:
            votes_data.append({
                "bill_id": b_id,
                "member_id": m_id,
                "vote_cast": row['vote_cast']
            })

    print(f"Upserting {len(votes_data)} votes...")
    batch_size = 1000
    for i in range(0, len(votes_data), batch_size):
        supabase.table("votes").upsert(votes_data[i:i+batch_size], on_conflict="bill_id,member_id").execute()

    print("ETL Pipeline completed successfully.")

if __name__ == "__main__":
    run_etl()
