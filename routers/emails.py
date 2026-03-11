import uuid
from datetime import datetime
from typing import List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from database import get_db
from email_service import get_smtp_settings, render_template, send_email

router = APIRouter()


class SendBatchRequest(BaseModel):
    lead_ids: List[int]
    subject: str
    body: str


class PreviewRequest(BaseModel):
    subject: str
    body: str


class EnhanceRequest(BaseModel):
    subject: str
    body: str
    lead_id: int


@router.post("/send")
async def send_batch(req: SendBatchRequest):
    db = get_db()
    smtp = get_smtp_settings()

    placeholders = ",".join("?" * len(req.lead_ids))
    leads = db.execute(
        f"SELECT * FROM leads WHERE id IN ({placeholders})", req.lead_ids
    ).fetchall()

    results: dict = {"sent": [], "failed": []}

    for lead in leads:
        lead = dict(lead)
        if not lead.get("email"):
            results["failed"].append({"id": lead["id"], "error": "No email address"})
            continue

        subject = render_template(req.subject, lead)
        body = render_template(req.body, lead)
        tracking_id = str(uuid.uuid4())

        try:
            send_email(
                lead["email"], subject, body,
                tracking_id=tracking_id,
                smtp_settings=smtp,
            )
            with db:
                db.execute(
                    "INSERT INTO sent_emails (lead_id, tracking_id, subject, body) VALUES (?, ?, ?, ?)",
                    (lead["id"], tracking_id, subject, body),
                )
                db.execute(
                    """UPDATE leads
                       SET status = 'sent',
                           last_contacted = ?,
                           times_contacted = times_contacted + 1
                       WHERE id = ?""",
                    (datetime.utcnow().isoformat(), lead["id"]),
                )
            results["sent"].append(lead["id"])
        except Exception as e:
            results["failed"].append({"id": lead["id"], "error": str(e)})

    db.close()
    return results


@router.post("/preview")
async def preview_email(req: PreviewRequest, lead_id: int):
    db = get_db()
    lead = db.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    db.close()
    if not lead:
        raise HTTPException(404, "Lead not found")
    lead = dict(lead)
    return {
        "subject": render_template(req.subject, lead),
        "body": render_template(req.body, lead),
    }


@router.post("/enhance")
async def enhance_with_ai(req: EnhanceRequest):
    db = get_db()
    lead = db.execute("SELECT * FROM leads WHERE id = ?", (req.lead_id,)).fetchone()
    settings_rows = db.execute("SELECT key, value FROM settings").fetchall()
    db.close()

    if not lead:
        raise HTTPException(404, "Lead not found")

    settings = {r["key"]: r["value"] for r in settings_rows}
    api_key = settings.get("groq_api_key")
    if not api_key:
        raise HTTPException(400, "Groq API key not configured in Settings")

    lead = dict(lead)
    from email_service import clean_business_name

    try:
        import json
        from groq import Groq

        client = Groq(api_key=api_key)

        prompt = f"""You are writing a cold outreach email for a web development agency targeting local small businesses.

Lead info:
- Business: {clean_business_name(lead.get("business_name", ""))}
- Type: {(lead.get("niche") or "").replace("-", " ")}
- Location: {lead.get("city")}, {lead.get("state")}
- Website issue: {lead.get("website_issue")}
- Rating: {lead.get("rating")} stars ({lead.get("reviews")} reviews)

Current draft:
Subject: {req.subject}
Body: {req.body}

Rewrite the subject and body to be more personalized, natural, and conversational.
Reference their specific situation and website issue. Keep the body under 120 words.
Do not use overly salesy language. Be direct and genuine.
Preserve any HTML tags in the body.

Return ONLY valid JSON: {{"subject": "...", "body": "..."}}"""

        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
        )
        text = chat.choices[0].message.content.strip()
        result = json.loads(text)
        return result
    except Exception as e:
        raise HTTPException(500, f"AI enhancement failed: {str(e)}")


@router.get("/sent")
async def get_sent(page: int = 1, per_page: int = 50):
    db = get_db()
    offset = (page - 1) * per_page
    total = db.execute("SELECT COUNT(*) FROM sent_emails").fetchone()[0]
    rows = db.execute(
        """SELECT se.id, se.lead_id, se.tracking_id, se.subject, se.sent_at,
                  se.opened_at, se.opened_count,
                  l.business_name_clean, l.email, l.niche, l.city, l.state
           FROM sent_emails se
           JOIN leads l ON se.lead_id = l.id
           ORDER BY se.sent_at DESC
           LIMIT ? OFFSET ?""",
        (per_page, offset),
    ).fetchall()
    db.close()
    return {
        "total": total,
        "page": page,
        "pages": max(1, (total + per_page - 1) // per_page),
        "sent": [dict(r) for r in rows],
    }


@router.get("/stats")
async def get_stats():
    db = get_db()
    total = db.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    with_email = db.execute(
        "SELECT COUNT(*) FROM leads WHERE email IS NOT NULL AND email != ''"
    ).fetchone()[0]
    sent = db.execute("SELECT COUNT(*) FROM leads WHERE status = 'sent'").fetchone()[0]
    opened = db.execute("SELECT COUNT(*) FROM leads WHERE status = 'opened'").fetchone()[0]
    replied = db.execute("SELECT COUNT(*) FROM leads WHERE status = 'replied'").fetchone()[0]
    converted = db.execute("SELECT COUNT(*) FROM leads WHERE status = 'converted'").fetchone()[0]
    total_sent_emails = db.execute("SELECT COUNT(*) FROM sent_emails").fetchone()[0]
    total_opened_emails = db.execute(
        "SELECT COUNT(*) FROM sent_emails WHERE opened_at IS NOT NULL"
    ).fetchone()[0]
    by_niche = db.execute(
        """SELECT niche, COUNT(*) as count,
                  SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as with_email,
                  SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as sent_count
           FROM leads WHERE niche IS NOT NULL
           GROUP BY niche ORDER BY count DESC LIMIT 12"""
    ).fetchall()
    db.close()
    return {
        "total": total,
        "with_email": with_email,
        "sent": sent,
        "opened": opened,
        "replied": replied,
        "converted": converted,
        "total_sent_emails": total_sent_emails,
        "total_opened_emails": total_opened_emails,
        "open_rate": round(total_opened_emails / total_sent_emails * 100, 1)
        if total_sent_emails > 0
        else 0,
        "by_niche": [dict(r) for r in by_niche],
    }
