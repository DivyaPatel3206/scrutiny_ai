import os
import uuid
from pathlib import Path

from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from db import get_conn, fetchall, execute, init_db

# -------------------------------
# App Initialization
# -------------------------------

app = FastAPI(title="Tally Clone + AI Scrutiny + Invoice Scanner")

# -------------------------------
# Paths
# -------------------------------

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"

# Vercel writable directory
UPLOAD_DIR = Path("/tmp/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------
# Templates
# -------------------------------

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# -------------------------------
# Initialize Database (safe)
# -------------------------------

@app.on_event("startup")
def startup():
    init_db()

# -------------------------------
# Home Page
# -------------------------------

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "screen": "dashboard"
        }
    )

# -------------------------------
# Add Ledger
# -------------------------------

@app.post("/ledger/add")
def add_ledger(name: str = Form(...), type: str = Form(...)):
    execute(
        "INSERT INTO ledgers(name,type) VALUES (?,?)",
        (name, type)
    )
    return RedirectResponse("/", status_code=303)

# -------------------------------
# List Ledgers
# -------------------------------

@app.get("/ledgers")
def get_ledgers():
    data = fetchall("SELECT * FROM ledgers")
    return {"ledgers": data}

# -------------------------------
# Upload Invoice
# -------------------------------

@app.post("/upload-invoice")
async def upload_invoice(file: UploadFile = File(...)):

    file_id = str(uuid.uuid4())
    file_path = UPLOAD_DIR / f"{file_id}_{file.filename}"

    with open(file_path, "wb") as buffer:
        buffer.write(await file.read())

    return JSONResponse({
        "message": "Invoice uploaded",
        "file": str(file_path)
    })

# -------------------------------
# Health Check
# -------------------------------

@app.get("/health")
def health():
    return {"status": "running"}
