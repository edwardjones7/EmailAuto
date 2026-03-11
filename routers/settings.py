import smtplib

from fastapi import APIRouter
from pydantic import BaseModel

from database import get_db

router = APIRouter()

MASKED = "••••••••"


class SettingsPayload(BaseModel):
    settings: dict


@router.get("")
async def get_settings():
    db = get_db()
    rows = db.execute("SELECT key, value FROM settings").fetchall()
    db.close()
    result = {r["key"]: r["value"] for r in rows}
    if result.get("smtp_password"):
        result["smtp_password"] = MASKED
    if result.get("groq_api_key"):
        result["groq_api_key"] = MASKED
    return result


@router.post("")
async def save_settings(data: SettingsPayload):
    db = get_db()
    with db:
        for key, value in data.settings.items():
            # Don't overwrite masked secrets
            if value in (MASKED, "") and key in ("smtp_password", "groq_api_key"):
                continue
            db.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, str(value) if value is not None else ""),
            )
    db.close()
    return {"ok": True}


@router.post("/test-smtp")
async def test_smtp():
    db = get_db()
    rows = db.execute(
        "SELECT key, value FROM settings WHERE key LIKE 'smtp_%'"
    ).fetchall()
    db.close()
    s = {r["key"]: r["value"] for r in rows}

    host = s.get("smtp_host", "mail.privateemail.com")
    port = int(s.get("smtp_port", "587"))
    username = s.get("smtp_username", "")
    password = s.get("smtp_password", "")

    if not username or not password:
        return {"ok": False, "message": "SMTP credentials not saved yet"}

    try:
        with smtplib.SMTP(host, port, timeout=10) as server:
            server.ehlo()
            server.starttls()
            server.login(username, password)
        return {"ok": True, "message": "Connection successful"}
    except Exception as e:
        return {"ok": False, "message": str(e)}
