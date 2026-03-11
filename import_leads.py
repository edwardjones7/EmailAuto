#!/usr/bin/env python3
"""
Run once to import all_leads_master.xlsx into SQLite.
Usage: python import_leads.py
"""
import re
import sqlite3
from pathlib import Path

import openpyxl

import os
_data_dir = os.getenv("DATA_DIR", str(Path(__file__).parent))
DB_PATH = Path(_data_dir) / "leads.db"
XLSX_PATH = Path(__file__).parent / "all_leads_master.xlsx"

DEFAULT_TEMPLATE = {
    "name": "Web Services — Website Issues",
    "subject": "Quick note about {{business_name}}'s website",
    "body": """<p>Hi {{owner_name}},</p>

<p>I came across <strong>{{business_name}}</strong> while researching {{niche}} businesses in {{city}} and noticed your website has an issue: <em>{{website_issue}}</em>.</p>

<p>For a business with your reputation, that could be quietly costing you customers who search online and move on. We help local businesses fix exactly these problems — fast, affordable, and done in under 2 weeks.</p>

<p>Would you be open to a quick 10-minute call to see if we can help?</p>

<p>Best,<br>[Your Name]</p>""",
}


def clean_business_name(name: str) -> str:
    if not name:
        return ""
    name = str(name)
    name = re.split(r"\s+Recommended\b", name)[0]
    name = re.sub(r"\s+\d+\s+\w+.*$", "", name).strip()
    return name


def compute_priority(row: dict) -> int:
    score = 0

    if row.get("email"):
        score += 10  # has email — most actionable

    issue = str(row.get("website_issue") or "")
    if "No website listed" in issue:
        score += 5  # no site = clear pitch
    elif "No mobile viewport" in issue or "thin" in issue.lower():
        score += 4  # fixable issue
    elif "error" in issue.lower() or "HTTP" in issue:
        score += 2  # broken site
    elif "unreachable" in issue:
        score += 1

    try:
        rating = float(row.get("rating") or 0)
        if rating >= 4.5:
            score += 3
        elif rating >= 4.0:
            score += 2
        elif rating >= 3.5:
            score += 1
    except (ValueError, TypeError):
        pass

    try:
        reviews = int(row.get("reviews") or 0)
        if reviews >= 100:
            score += 3
        elif reviews >= 50:
            score += 2
        elif reviews >= 20:
            score += 1
    except (ValueError, TypeError):
        pass

    return score


SCHEMA = """
CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY,
    business_name TEXT,
    business_name_clean TEXT,
    city TEXT,
    state TEXT,
    niche TEXT,
    rating REAL,
    reviews INTEGER,
    website TEXT,
    phone TEXT,
    email TEXT,
    owner_name TEXT,
    listing_url TEXT,
    website_issue TEXT,
    source TEXT,
    captured_at TEXT,
    status TEXT DEFAULT 'new',
    notes TEXT,
    imported_at TEXT,
    last_contacted TIMESTAMP,
    times_contacted INTEGER DEFAULT 0,
    priority_score INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS email_templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS sent_emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id INTEGER REFERENCES leads(id),
    tracking_id TEXT UNIQUE,
    subject TEXT,
    body TEXT,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    opened_at TIMESTAMP,
    opened_count INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);
CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email);
CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
CREATE INDEX IF NOT EXISTS idx_leads_niche ON leads(niche);
CREATE INDEX IF NOT EXISTS idx_leads_priority ON leads(priority_score);
"""


def import_leads():
    print(f"Loading {XLSX_PATH} ...")
    wb = openpyxl.load_workbook(str(XLSX_PATH), read_only=True)
    ws = wb["leads"]
    all_rows = list(ws.iter_rows(values_only=True))
    headers = all_rows[0]
    data_rows = all_rows[1:]
    print(f"Found {len(data_rows)} rows")

    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(SCHEMA)

    inserted = skipped = 0
    for row in data_rows:
        row_dict = dict(zip(headers, row))
        lead_id = row_dict.get("id")
        if lead_id and conn.execute("SELECT 1 FROM leads WHERE id = ?", (lead_id,)).fetchone():
            skipped += 1
            continue

        business_name = str(row_dict.get("business_name") or "")
        clean_name = clean_business_name(business_name)
        priority = compute_priority(row_dict)

        conn.execute(
            """INSERT OR IGNORE INTO leads
               (id, business_name, business_name_clean, city, state, niche,
                rating, reviews, website, phone, email, owner_name,
                listing_url, website_issue, source, captured_at,
                status, notes, imported_at, priority_score)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                lead_id,
                business_name,
                clean_name,
                row_dict.get("city"),
                row_dict.get("state"),
                row_dict.get("niche"),
                row_dict.get("rating"),
                row_dict.get("reviews"),
                row_dict.get("website"),
                row_dict.get("phone"),
                row_dict.get("email"),
                row_dict.get("owner_name"),
                row_dict.get("listing_url"),
                row_dict.get("website_issue"),
                row_dict.get("source"),
                str(row_dict.get("captured_at") or ""),
                row_dict.get("status") or "new",
                row_dict.get("notes"),
                str(row_dict.get("imported_at") or ""),
                priority,
            ),
        )
        inserted += 1

    conn.commit()

    # Seed a default template if none exist
    if conn.execute("SELECT COUNT(*) FROM email_templates").fetchone()[0] == 0:
        conn.execute(
            "INSERT INTO email_templates (name, subject, body) VALUES (?, ?, ?)",
            (DEFAULT_TEMPLATE["name"], DEFAULT_TEMPLATE["subject"], DEFAULT_TEMPLATE["body"]),
        )
        conn.commit()
        print("Seeded default email template")

    conn.close()
    print(f"Done — inserted: {inserted}, skipped (already in DB): {skipped}")


if __name__ == "__main__":
    import_leads()
