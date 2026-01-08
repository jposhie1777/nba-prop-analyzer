# debug_code.py
from fastapi import APIRouter, HTTPException
from pathlib import Path
import os

router = APIRouter(
    prefix="/debug/code",
    tags=["debug"],
)

ALLOWED_FILES = {
    "main.py",
    "live_stream.py",
    "box_scores_snapshot.py",
    "box_test.py",
    "db.py",
    "Dockerfile",
    "requirements.txt",
}

BASE_DIR = Path(__file__).resolve().parent

@router.get("")
def list_files():
    return {"files": sorted(ALLOWED_FILES)}

@router.get("/{filename}")
def view_file(filename: str):
    #if os.getenv("ENV", "prod") == "prod":
        #raise HTTPException(status_code=404)

    if filename not in ALLOWED_FILES:
        raise HTTPException(status_code=403)

    path = BASE_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404)

    return {
        "filename": filename,
        "content": path.read_text(),
    }


# 🔑 THIS IS THE KEY
def register(app):
    """
    Attach debug routes AFTER app exists.
    """
    app.include_router(router)