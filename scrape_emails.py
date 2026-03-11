#!/usr/bin/env python3
"""
Scrape email addresses from lead websites and populate the database.
Usage: python scrape_emails.py
"""

import re
import sqlite3
import sys
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning

warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DB_PATH = Path(__file__).parent / "leads.db"

# Pages to check per site (in order)
CONTACT_PATHS = ["/", "/contact", "/contact-us", "/about", "/about-us"]

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# Email domains to discard — noise, not real contacts
JUNK_DOMAINS = {
    "sentry.io", "sentry-cdn.com", "example.com", "example.org",
    "w3.org", "schema.org", "google.com", "googleapis.com",
    "facebook.com", "twitter.com", "instagram.com", "tiktok.com",
    "apple.com", "microsoft.com", "wixpress.com", "squarespace.com",
    "shopify.com", "wordpress.org", "jquery.com", "cloudflare.com",
    "amazonaws.com", "doubleclick.net", "yourdomain.com",
    "domain.com", "email.com", "yoursite.com",
}

# website_issue values that mean the site is definitively down
SKIP_ISSUE_PATTERNS = [
    "No website listed",
    "Website unreachable",
    "Website error HTTP 404",
    "Website error HTTP 410",
    "Website error HTTP 500",
    "Website error HTTP 503",
    "Social/3rd-party-only",
    "Social-only",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# Prefixes that suggest a generic contact email (preferred over personal)
CONTACT_PREFIXES = [
    "contact", "info", "hello", "hi", "mail", "email",
    "enquir", "inquiry", "general", "admin", "office", "reach",
    "support", "help", "team",
]


def get_leads():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    skip_clauses = " AND ".join(
        [f"(website_issue IS NULL OR website_issue NOT LIKE ?)" for _ in SKIP_ISSUE_PATTERNS]
    )
    params = [f"%{p}%" for p in SKIP_ISSUE_PATTERNS]

    rows = conn.execute(
        f"""
        SELECT id, website, website_issue
        FROM leads
        WHERE website IS NOT NULL
          AND website != ''
          AND (email IS NULL OR email = '')
          AND {skip_clauses}
        ORDER BY priority_score DESC
        """,
        params,
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def is_valid_email(email: str) -> bool:
    if not email or "@" not in email or len(email) > 80:
        return False
    local, domain = email.rsplit("@", 1)
    if not local or not domain or "." not in domain:
        return False
    if domain in JUNK_DOMAINS:
        return False
    # Skip emails where domain looks like a file extension or CDN path
    if re.search(r"\.(png|jpg|gif|svg|webp|css|js|woff|ttf)$", domain):
        return False
    # Must have a real TLD
    tld = domain.rsplit(".", 1)[-1]
    if len(tld) < 2 or len(tld) > 6:
        return False
    return True


def extract_emails(html: str, soup: BeautifulSoup) -> list[str]:
    emails = []

    # 1. mailto: links — most reliable
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().startswith("mailto:"):
            email = href[7:].split("?")[0].strip().lower()
            emails.append(email)

    # 2. Regex over raw HTML
    for match in EMAIL_RE.findall(html):
        emails.append(match.lower().strip("."))

    # Deduplicate preserving order, filter junk
    seen = set()
    result = []
    for e in emails:
        if e not in seen and is_valid_email(e):
            seen.add(e)
            result.append(e)
    return result


def best_email(emails: list[str]) -> str | None:
    if not emails:
        return None
    # Prefer generic contact emails
    for email in emails:
        prefix = email.split("@")[0]
        if any(prefix.startswith(p) for p in CONTACT_PREFIXES):
            return email
    return emails[0]


def scrape_lead(lead: dict) -> tuple[int, str | None]:
    base = lead["website"].rstrip("/")

    # Quick sanity check on the URL
    parsed = urlparse(base)
    if not parsed.scheme or not parsed.netloc:
        return lead["id"], None

    session = requests.Session()
    session.headers.update(HEADERS)

    all_emails: list[str] = []

    for path in CONTACT_PATHS:
        url = base if path == "/" else base + path
        try:
            r = session.get(url, timeout=8, allow_redirects=True)
            if r.status_code != 200:
                if path == "/":
                    break  # homepage dead, skip rest
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            page_emails = extract_emails(r.text, soup)
            all_emails.extend(page_emails)

            if all_emails:
                break  # got something — stop checking more pages

        except Exception:
            if path == "/":
                break  # can't reach homepage at all

    return lead["id"], best_email(all_emails)


def save_email(lead_id: int, email: str):
    conn = sqlite3.connect(str(DB_PATH))
    with conn:
        conn.execute(
            "UPDATE leads SET email = ?, priority_score = priority_score + 10 WHERE id = ?",
            (email, lead_id),
        )
    conn.close()


def main():
    leads = get_leads()
    total = len(leads)

    if total == 0:
        print("No leads to scrape — all either have emails or no workable website.")
        return

    print(f"Scraping {total} sites with 15 threads...\n")

    found = 0
    done = 0
    workers = 15

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scrape_lead, lead): lead for lead in leads}

        for future in as_completed(futures):
            done += 1
            lead_id, email = future.result()

            if email:
                save_email(lead_id, email)
                found += 1
                pct = round(found / done * 100, 1)
                print(f"  [{done:>4}/{total}]  ✓  #{lead_id:<5}  {email}  ({pct}% hit rate)")
            else:
                # Print progress every 100 misses
                if done % 100 == 0:
                    pct = round(found / done * 100, 1) if done > 0 else 0
                    print(f"  [{done:>4}/{total}]  ...  {found} found so far  ({pct}% hit rate)")

    print(f"\n{'─'*50}")
    print(f"  Done.  {found} emails found from {total} sites scraped.")
    print(f"  Hit rate: {round(found / total * 100, 1) if total else 0}%")
    print(f"  Run 'python main.py' and check the Emails tab.")


if __name__ == "__main__":
    main()
