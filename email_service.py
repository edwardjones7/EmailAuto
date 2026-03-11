import os
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from database import get_db

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")


def clean_business_name(name: str) -> str:
    if not name:
        return ""
    name = str(name)
    # Split on 'Recommended' keyword (common in scraped data)
    name = re.split(r"\s+Recommended\b", name)[0]
    # Remove trailing address patterns (e.g. "1225 N Pacific St, ...")
    name = re.sub(r"\s+\d+\s+\w+.*$", "", name).strip()
    return name


def render_template(template: str, lead: dict) -> str:
    niche = (lead.get("niche") or "business").replace("-", " ")
    vars = {
        "business_name": clean_business_name(lead.get("business_name", ""))
        or lead.get("business_name", "your business"),
        "owner_name": lead.get("owner_name") or "Business Owner",
        "city": lead.get("city") or "",
        "state": lead.get("state") or "",
        "niche": niche,
        "website": lead.get("website") or "your website",
        "website_issue": lead.get("website_issue") or "website issues",
        "phone": lead.get("phone") or "",
    }
    result = template
    for key, value in vars.items():
        result = result.replace(f"{{{{{key}}}}}", str(value))
    return result


def get_smtp_settings() -> dict:
    db = get_db()
    rows = db.execute(
        "SELECT key, value FROM settings WHERE key LIKE 'smtp_%'"
    ).fetchall()
    db.close()
    return {row["key"]: row["value"] for row in rows}


def send_email(
    to_email: str,
    subject: str,
    body_html: str,
    tracking_id: str | None = None,
    smtp_settings: dict | None = None,
) -> None:
    if smtp_settings is None:
        smtp_settings = get_smtp_settings()

    host = smtp_settings.get("smtp_host", "mail.privateemail.com")
    port = int(smtp_settings.get("smtp_port", "587"))
    username = smtp_settings.get("smtp_username", "")
    password = smtp_settings.get("smtp_password", "")
    from_name = smtp_settings.get("smtp_from_name", "")

    if not username or not password:
        raise ValueError("SMTP credentials not configured in Settings")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{username}>" if from_name else username
    msg["To"] = to_email

    if tracking_id:
        pixel = f'<img src="{BASE_URL}/track/open/{tracking_id}" width="1" height="1" style="display:none;" />'
        body_html = body_html + pixel

    plain = re.sub(r"<[^>]+>", "", body_html)
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(body_html, "html"))

    with smtplib.SMTP(host, port, timeout=15) as server:
        server.ehlo()
        server.starttls()
        server.login(username, password)
        server.sendmail(username, to_email, msg.as_string())
