from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from database import get_db
from email_service import render_template

router = APIRouter()


class TemplateBody(BaseModel):
    name: str
    subject: str
    body: str


@router.get("")
async def list_templates():
    db = get_db()
    rows = db.execute(
        "SELECT * FROM email_templates ORDER BY created_at DESC"
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


@router.post("")
async def create_template(data: TemplateBody):
    db = get_db()
    with db:
        cursor = db.execute(
            "INSERT INTO email_templates (name, subject, body) VALUES (?, ?, ?)",
            (data.name, data.subject, data.body),
        )
    db.close()
    return {"id": cursor.lastrowid}


@router.put("/{template_id}")
async def update_template(template_id: int, data: TemplateBody):
    db = get_db()
    with db:
        db.execute(
            "UPDATE email_templates SET name=?, subject=?, body=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (data.name, data.subject, data.body, template_id),
        )
    db.close()
    return {"ok": True}


@router.delete("/{template_id}")
async def delete_template(template_id: int):
    db = get_db()
    with db:
        db.execute("DELETE FROM email_templates WHERE id = ?", (template_id,))
    db.close()
    return {"ok": True}


@router.get("/{template_id}/preview")
async def preview_template(template_id: int, lead_id: Optional[int] = None):
    db = get_db()
    tmpl = db.execute(
        "SELECT * FROM email_templates WHERE id = ?", (template_id,)
    ).fetchone()
    if not tmpl:
        raise HTTPException(404, "Template not found")
    lead = None
    if lead_id:
        lead = db.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    db.close()

    if lead:
        return {
            "subject": render_template(tmpl["subject"], dict(lead)),
            "body": render_template(tmpl["body"], dict(lead)),
        }
    return {"subject": tmpl["subject"], "body": tmpl["body"]}
