"""
Startup script for Railway deployment.
Runs import_leads.py on first boot (empty DB), then starts the server.
"""
import os
import sys
from pathlib import Path

data_dir = os.getenv("DATA_DIR", str(Path(__file__).parent))
db_path = Path(data_dir) / "leads.db"

# Auto-import leads if this is a fresh deployment
if not db_path.exists() or db_path.stat().st_size < 10_000:
    print("Fresh database — importing leads...", flush=True)
    # Ensure data dir exists
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    import import_leads
    import_leads.import_leads()
    print("Import complete.", flush=True)
else:
    print(f"Database found at {db_path} ({db_path.stat().st_size // 1024}KB)", flush=True)

# Start uvicorn
port = int(os.getenv("PORT", 8000))
print(f"Starting server on port {port}...", flush=True)

os.execv(
    sys.executable,
    [sys.executable, "-m", "uvicorn", "main:app",
     "--host", "0.0.0.0",
     "--port", str(port)],
)
