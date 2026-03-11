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
    if any(keyword in str(vote_type) for keyword in ["Appointment Message", "Procedural"]):
        return "Simple Majority"
    try:
        vote_date = datetime.strptime(str(vote_date_str), "%Y-%m-%d")
        return "Simple Majority" if vote_date.month < 6 else "3/5s Majority"
    except Exception:
        return "Simple Majority"

def run_etl():
    print(f"Downloading data from {CSV_URL}...")
    response = requests.get(CSV_URL)
    response.raise_for_status()
    
    with open("temp_votes.csv", "wb") as f:
        f.write(response.content)
    
    df = pd.read_csv("temp_votes.csv")
    df = df.where(pd.notnull(df), None)
    df.columns = [c.strip() for c in df.columns]

    # Map headers
    df['member_name'] = df['Member']
    df['bill_number'] = df['Legislation']
    df['vote_cast'] = df['Vote']
    df['v_date'] = df['Vote Date']
    df['v_type'] = df['Vote Type']

    # Process Members
    unique_members = df['member_name'].unique()
    members_data = []
    for member in unique_members:
        member_votes = df[df['member_name'] == member]
        m1 = len(member_votes[member_votes['vote_cast'].str.lower().isin(['excused', 'absent'])])
        m2 = len(member_votes[member_votes['vote_cast'].str.lower().isin(['excused', 'absent', 'no vote', 'not voting'])])
        members_data.append({"name": member, "metric1_absences": m1, "metric2_absences": m2, "active_status": True})

    # Process Bills (Deduplicate by unique combination)
    unique_bills = df[['bill_number', 'v_date', 'v_type']].drop_duplicates()
    bills_data = []
    for _, row in unique_bills.iterrows():
        margin = get_margin_required(row['v_type'], row['v_date'])
        bills_data.append({
            "bill_number": row['bill_number'],
            "vote_date": row['v_date'],
            "vote_type": row['v_type'],
            "margin_required": margin,
            "passed": None
        })

    print(f"Upserting {len(members_data)} members...")
    supabase.table("members").upsert(members_data, on_conflict="name").execute()
    
    print(f"Upserting {len(bills_data)} bills...")
    supabase.table("bills").upsert(bills_data, on_conflict="bill_number,vote_type,vote_date").execute()

    # Get IDs for foreign keys
    res_members = supabase.table("members").select("id, name").execute()
    member_map = {m['name']: m['id'] for m in res_members.data}

    res_bills = supabase.table("bills").select("id, bill_number, vote_type, vote_date").execute()
    bill_map = {(b['bill_number'], b['vote_type'], str(b['vote_date'])): b['id'] for b in res_bills.data}

    votes_data = []
    for _, row in df.iterrows():
        m_id = member_map.get(row['member_name'])
        b_id = bill_map.get((row['bill_number'], row['v_type'], str(row['v_date'])))
        
        if m_id and b_id:
            votes_data.append({"bill_id": b_id, "member_id": m_id, "vote_cast": row['vote_cast']})

    print(f"Upserting {len(votes_data)} votes...")
    batch_size = 1000
    for i in range(0, len(votes_data), batch_size):
        supabase.table("votes").upsert(votes_data[i:i+batch_size], on_conflict="bill_id,member_id").execute()

    print("ETL Pipeline completed successfully.")

if __name__ == "__main__":
    run_etl()
