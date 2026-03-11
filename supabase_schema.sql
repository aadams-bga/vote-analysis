-- Supabase Schema for ILGA Legislative Tracking

-- Table: members
CREATE TABLE IF NOT EXISTS members (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    party TEXT,
    city TEXT,
    district TEXT,
    active_status BOOLEAN DEFAULT TRUE,
    metric1_absences INTEGER DEFAULT 0,
    metric2_absences INTEGER DEFAULT 0,
    UNIQUE(name)
);

-- Table: bills
CREATE TABLE IF NOT EXISTS bills (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bill_number TEXT NOT NULL,
    vote_date DATE,
    vote_type TEXT,
    margin_required TEXT,
    passed BOOLEAN,
    UNIQUE(bill_number, vote_type, vote_date)
);

-- Table: votes
CREATE TABLE IF NOT EXISTS votes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bill_id UUID REFERENCES bills(id) ON DELETE CASCADE,
    member_id UUID REFERENCES members(id) ON DELETE CASCADE,
    vote_cast TEXT, -- 'Yes', 'No', 'Present', 'Excused', 'Absent', 'No Vote', 'Not Voting'
    UNIQUE(bill_id, member_id)
);

-- Insert dummy data for Carol Ammons
INSERT INTO members (name, party, city, district, active_status)
VALUES ('Carol Ammons', 'Democrat', 'Urbana', '103rd District', TRUE)
ON CONFLICT (name) DO NOTHING;

-- Insert fake bills and votes for testing
DO $$
DECLARE
    carol_id UUID;
    bill1_id UUID;
    bill2_id UUID;
BEGIN
    SELECT id INTO carol_id FROM members WHERE name = 'Carol Ammons' LIMIT 1;
    
    INSERT INTO bills (bill_number, vote_date, vote_type, margin_required, passed)
    VALUES ('HB1234', '2024-03-10', 'Third Reading', 'Simple Majority', TRUE)
    ON CONFLICT (bill_number, vote_type, vote_date) DO UPDATE SET bill_number = EXCLUDED.bill_number
    RETURNING id INTO bill1_id;

    INSERT INTO bills (bill_number, vote_date, vote_type, margin_required, passed)
    VALUES ('SB0001', '2024-06-15', 'Third Reading', '3/5s Majority', FALSE)
    ON CONFLICT (bill_number, vote_type, vote_date) DO UPDATE SET bill_number = EXCLUDED.bill_number
    RETURNING id INTO bill2_id;

    INSERT INTO votes (bill_id, member_id, vote_cast)
    VALUES (bill1_id, carol_id, 'Yes')
    ON CONFLICT (bill_id, member_id) DO NOTHING;

    INSERT INTO votes (bill_id, member_id, vote_cast)
    VALUES (bill2_id, carol_id, 'No')
    ON CONFLICT (bill_id, member_id) DO NOTHING;
END $$;
