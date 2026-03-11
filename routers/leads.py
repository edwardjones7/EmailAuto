from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from database import get_db

router = APIRouter()


@router.get("")
async def get_leads(
    page: int = 1,
    per_page: int = 50,
    has_email: Optional[str] = None,   # 'true' | 'false' | ''
    niche: Optional[str] = None,
    state: Optional[str] = None,
    status: Optional[str] = None,
    website_issue_type: Optional[str] = None,
    min_rating: Optional[float] = None,
    min_reviews: Optional[int] = None,
    search: Optional[str] = None,
    sort_by: str = "priority_score",
    sort_order: str = "desc",
):
    db = get_db()
    where = ["1=1"]
    params = []

    if has_email == "true":
        where.append("email IS NOT NULL AND email != ''")
    elif has_email == "false":
        where.append("(email IS NULL OR email = '')")

    if niche:
        where.append("niche = ?")
        params.append(niche)

    if state:
        where.append("state = ?")
        params.append(state)

    if status:
        where.append("status = ?")
        params.append(status)

    if website_issue_type:
        issue_map = {
            "no_website": "website_issue = 'No website listed'",
            "viewport": "website_issue LIKE '%viewport%'",
            "thin": "website_issue LIKE '%thin%'",
            "error": "website_issue LIKE '%error%' OR website_issue LIKE '%HTTP%'",
            "unreachable": "website_issue LIKE '%unreachable%'",
        }
        if website_issue_type in issue_map:
            where.append(f"({issue_map[website_issue_type]})")

    if min_rating is not None:
        where.append("CAST(rating AS REAL) >= ?")
        params.append(min_rating)

    if min_reviews is not None:
        where.append("reviews >= ?")
        params.append(min_reviews)

    if search:
        where.append(
            "(business_name_clean LIKE ? OR business_name LIKE ? OR email LIKE ? OR city LIKE ?)"
        )
        s = f"%{search}%"
        params.extend([s, s, s, s])

    where_clause = " AND ".join(where)

    valid_sorts = {
        "priority_score", "rating", "reviews",
        "business_name_clean", "last_contacted", "status", "times_contacted",
    }
    if sort_by not in valid_sorts:
        sort_by = "priority_score"
    order = "DESC" if sort_order.lower() == "desc" else "ASC"

    total = db.execute(
        f"SELECT COUNT(*) FROM leads WHERE {where_clause}", params
    ).fetchone()[0]

    offset = (page - 1) * per_page
    rows = db.execute(
        f"SELECT * FROM leads WHERE {where_clause} ORDER BY {sort_by} {order} LIMIT ? OFFSET ?",
        params + [per_page, offset],
    ).fetchall()

    db.close()
    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": max(1, (total + per_page - 1) // per_page),
        "leads": [dict(r) for r in rows],
    }


@router.get("/filters")
async def get_filter_options():
    db = get_db()
    niches = db.execute(
        "SELECT DISTINCT niche FROM leads WHERE niche IS NOT NULL ORDER BY niche"
    ).fetchall()
    states = db.execute(
        "SELECT DISTINCT state FROM leads WHERE state IS NOT NULL ORDER BY state"
    ).fetchall()
    db.close()
    return {
        "niches": [r[0] for r in niches],
        "states": [r[0] for r in states],
    }


@router.get("/{lead_id}")
async def get_lead(lead_id: int):
    db = get_db()
    row = db.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    db.close()
    if not row:
        raise HTTPException(404, "Lead not found")
    return dict(row)


class LeadUpdate(BaseModel):
    status: Optional[str] = None
    notes: Optional[str] = None
    email: Optional[str] = None


class LeadCreate(BaseModel):
    business_name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    niche: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    rating: Optional[float] = None
    reviews: Optional[int] = None
    website_issue: Optional[str] = None
    notes: Optional[str] = None


@router.post("")
async def create_lead(data: LeadCreate):
    db = get_db()
    clean = data.business_name.strip()
    # Remove common suffixes for clean name
    import re
    clean = re.sub(r"\b(LLC|Inc\.?|Corp\.?|Co\.?|Ltd\.?)\b", "", clean, flags=re.IGNORECASE).strip(" ,.")
    with db:
        cur = db.execute(
            """INSERT INTO leads
               (business_name, business_name_clean, email, phone, website,
                niche, city, state, rating, reviews, website_issue, notes, status, priority_score)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'new', 0)""",
            (data.business_name, clean, data.email or "", data.phone or "",
             data.website or "", data.niche or "", data.city or "", data.state or "",
             data.rating, data.reviews, data.website_issue or "", data.notes or ""),
        )
        lead_id = cur.lastrowid
    db.close()
    return {"ok": True, "id": lead_id}


@router.delete("/dedup")
async def dedup_emails():
    db = get_db()
    with db:
        db.execute("""
            DELETE FROM leads
            WHERE email IS NOT NULL AND email != ''
              AND id NOT IN (
                SELECT MIN(id) FROM leads
                WHERE email IS NOT NULL AND email != ''
                GROUP BY LOWER(TRIM(email))
              )
        """)
    removed = db.execute("SELECT changes()").fetchone()[0]
    db.close()
    return {"ok": True, "removed": removed}


@router.patch("/{lead_id}")
async def update_lead(lead_id: int, data: LeadUpdate):
    updates = {k: v for k, v in data.model_dump().items() if v is not None}
    if not updates:
        return {"ok": True}
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    db = get_db()
    with db:
        db.execute(
            f"UPDATE leads SET {set_clause} WHERE id = ?",
            list(updates.values()) + [lead_id],
        )
    db.close()
    return {"ok": True}
