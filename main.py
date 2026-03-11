import base64
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles

from database import get_db, init_db
from routers import emails, leads, settings, templates

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Email Automation", lifespan=lifespan)

app.include_router(leads.router, prefix="/api/leads", tags=["leads"])
app.include_router(emails.router, prefix="/api/emails", tags=["emails"])
app.include_router(templates.router, prefix="/api/templates", tags=["templates"])
app.include_router(settings.router, prefix="/api/settings", tags=["settings"])


# Expose BASE_URL to frontend so tracking pixel uses the real domain
@app.get("/api/config")
async def get_config():
    return {"base_url": BASE_URL}


# 1x1 transparent PNG (base64)
PIXEL = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
)


@app.get("/track/open/{tracking_id}")
async def track_open(tracking_id: str):
    db = get_db()
    with db:
        db.execute(
            """UPDATE sent_emails
               SET opened_at = COALESCE(opened_at, CURRENT_TIMESTAMP),
                   opened_count = opened_count + 1
               WHERE tracking_id = ?""",
            (tracking_id,),
        )
        row = db.execute(
            "SELECT lead_id FROM sent_emails WHERE tracking_id = ?", (tracking_id,)
        ).fetchone()
        if row:
            db.execute(
                "UPDATE leads SET status = 'opened' WHERE id = ? AND status = 'sent'",
                (row["lead_id"],),
            )
    db.close()
    return Response(content=PIXEL, media_type="image/png")


app.mount("/", StaticFiles(directory="static", html=True), name="static")


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
