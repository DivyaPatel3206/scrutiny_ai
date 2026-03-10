import uuid
from pathlib import Path

from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from db import fetchall, execute, init_db

app = FastAPI(title="Tally Clone + AI Scrutiny + Invoice Scanner")

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

UPLOAD_DIR = Path("/tmp/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


@app.on_event("startup")
def startup():
    init_db()


@app.get("/", response_class=HTMLResponse)
def home(request: Request, screen: str = "dashboard", type: str = "Journal"):
    companies = fetchall("SELECT * FROM companies ORDER BY id DESC")
    ledgers = fetchall("SELECT * FROM ledgers ORDER BY id DESC")

    context = {
        "request": request,
        "screen": screen,
        "selected_voucher_type": type,
        "active_company": None,
        "companies": companies,
        "ledgers": ledgers,
        "vouchers": [],
        "risk_alerts": [],
        "compliance_items": [],
        "risky_vouchers": [],
        "scanner_result": None,
        "summary": {
            "company_count": len(companies),
            "ledger_count": len(ledgers),
            "voucher_count": 0,
            "debit_total": 0.0,
        },
        "stats": {
            "total": 0,
            "high_count": 0,
            "medium_count": 0,
            "low_count": 0,
            "avg_score": 0.0,
        },
        "duplicate_invoices": 0,
        "high_cash_entries": 0,
        "missing_gstin_count": 0,
        "ai_summary": "No AI analysis available yet.",
        "default_ledger_groups": [
            "Capital Account",
            "Current Assets",
            "Current Liabilities",
            "Sales Accounts",
            "Purchase Accounts",
            "Bank Accounts",
            "Indirect Income",
            "Indirect Expenses",
        ],
    }

    return templates.TemplateResponse("index.html", context)


@app.post("/company")
def create_company(
    name: str = Form(...),
    mailing_name: str = Form(""),
    address: str = Form(""),
    state: str = Form(""),
    country: str = Form("India"),
    phone: str = Form(""),
    email: str = Form(""),
    financial_year_start: str = Form(""),
    books_from: str = Form(""),
    currency: str = Form("₹"),
    maintain_inventory: str = Form("Yes"),
    enable_gst: str = Form("Yes"),
):
    execute("""
        INSERT INTO companies (
            name, mailing_name, address, state, country, phone, email,
            financial_year_start, books_from, currency, maintain_inventory, enable_gst
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        name, mailing_name, address, state, country, phone, email,
        financial_year_start, books_from, currency, maintain_inventory, enable_gst
    ))
    return RedirectResponse(url="/?screen=company", status_code=303)


@app.post("/ledger/add")
def add_ledger(name: str = Form(...), type: str = Form(...)):
    execute(
        "INSERT INTO ledgers(name, type) VALUES (?, ?)",
        (name, type)
    )
    return RedirectResponse(url="/?screen=ledger", status_code=303)


@app.get("/ledgers")
def get_ledgers():
    data = fetchall("SELECT * FROM ledgers")
    return {"ledgers": data}


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


@app.get("/health")
def health():
    return {"status": "running"}
