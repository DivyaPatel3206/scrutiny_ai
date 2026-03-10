from collections import defaultdict
import re
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from db import get_conn, init_db

app = FastAPI(title="Tally Clone + AI Scrutiny + HSN + Invoice Scanner")
templates = Jinja2Templates(directory="templates")

DEFAULT_LEDGER_GROUPS = [
    "Capital Account",
    "Current Assets",
    "Current Liabilities",
    "Sales Accounts",
    "Purchase Accounts",
    "Bank Accounts",
    "Indirect Income",
    "Indirect Expenses"
]


@app.on_event("startup")
def startup():
    init_db()


def clean_text(value):
    return str(value or "").strip()


def to_float(value, default=0.0):
    try:
        return float(value or default)
    except (TypeError, ValueError):
        return float(default)


def get_active_company_id(request: Request):
    raw = request.cookies.get("active_company_id")
    if raw and raw.isdigit():
        return int(raw)
    return None


def get_active_company(request: Request):
    company_id = get_active_company_id(request)
    if not company_id:
        return None
    with get_conn() as conn:
        company = conn.execute("SELECT * FROM companies WHERE id = ?", (company_id,)).fetchone()
        return company


def list_companies():
    with get_conn() as conn:
        return conn.execute("SELECT * FROM companies ORDER BY id DESC").fetchall()


def list_ledgers(company_id):
    if not company_id:
        return []
    with get_conn() as conn:
        return conn.execute("""
            SELECT * FROM ledgers
            WHERE company_id = ?
            ORDER BY ledger_name COLLATE NOCASE ASC
        """, (company_id,)).fetchall()


def get_ledger_map(company_id):
    rows = list_ledgers(company_id)
    return {row["id"]: row for row in rows}


def list_vouchers(company_id):
    if not company_id:
        return []

    with get_conn() as conn:
        vouchers = conn.execute("""
            SELECT * FROM vouchers
            WHERE company_id = ?
            ORDER BY id DESC
        """, (company_id,)).fetchall()

        if not vouchers:
            return []

        voucher_ids = [v["id"] for v in vouchers]
        placeholders = ",".join(["?"] * len(voucher_ids))

        entries = conn.execute(f"""
            SELECT ve.*, l.ledger_name, l.group_name, v.company_id
            FROM voucher_entries ve
            JOIN ledgers l ON l.id = ve.ledger_id
            JOIN vouchers v ON v.id = ve.voucher_id
            WHERE ve.voucher_id IN ({placeholders})
            ORDER BY ve.voucher_id DESC, ve.id ASC
        """, tuple(voucher_ids)).fetchall()

        grouped_entries = defaultdict(list)
        for e in entries:
            grouped_entries[e["voucher_id"]].append(e)

        return [{"voucher": v, "entries": grouped_entries.get(v["id"], [])} for v in vouchers]


def dashboard_summary(company_id):
    with get_conn() as conn:
        company_count = conn.execute("SELECT COUNT(*) AS c FROM companies").fetchone()["c"]

        if not company_id:
            return {
                "company_count": company_count,
                "ledger_count": 0,
                "voucher_count": 0,
                "debit_total": 0.0,
                "credit_total": 0.0
            }

        ledger_count = conn.execute(
            "SELECT COUNT(*) AS c FROM ledgers WHERE company_id = ?",
            (company_id,)
        ).fetchone()["c"]

        voucher_count = conn.execute(
            "SELECT COUNT(*) AS c FROM vouchers WHERE company_id = ?",
            (company_id,)
        ).fetchone()["c"]

        debit_total = conn.execute("""
            SELECT COALESCE(SUM(ve.debit), 0) AS total
            FROM voucher_entries ve
            JOIN vouchers v ON v.id = ve.voucher_id
            WHERE v.company_id = ?
        """, (company_id,)).fetchone()["total"]

        credit_total = conn.execute("""
            SELECT COALESCE(SUM(ve.credit), 0) AS total
            FROM voucher_entries ve
            JOIN vouchers v ON v.id = ve.voucher_id
            WHERE v.company_id = ?
        """, (company_id,)).fetchone()["total"]

        return {
            "company_count": company_count,
            "ledger_count": ledger_count,
            "voucher_count": voucher_count,
            "debit_total": float(debit_total or 0),
            "credit_total": float(credit_total or 0)
        }


def build_ai_summary(stats, duplicate_invoices, high_cash_entries, missing_gstin_count):
    lines = []
    if (stats["high_count"] or 0) > 0:
        lines.append(f"{stats['high_count']} high-risk vouchers found.")
    if duplicate_invoices > 0:
        lines.append(f"{duplicate_invoices} duplicate invoice patterns detected.")
    if high_cash_entries > 0:
        lines.append(f"{high_cash_entries} high cash entries need review.")
    if missing_gstin_count > 0:
        lines.append(f"{missing_gstin_count} GSTIN-related alerts found.")
    if not lines:
        lines.append("No major scrutiny issues detected.")
    return " ".join(lines)


def lookup_hsn(product_name: str):
    name = clean_text(product_name).lower()
    if not name:
        return None

    with get_conn() as conn:
        exact = conn.execute("""
            SELECT product_name, hsn_code, gst_rate
            FROM hsn_master
            WHERE LOWER(TRIM(product_name)) = ?
            LIMIT 1
        """, (name,)).fetchone()
        if exact:
            return exact

        fuzzy = conn.execute("""
            SELECT product_name, hsn_code, gst_rate
            FROM hsn_master
            WHERE LOWER(product_name) LIKE ?
            ORDER BY LENGTH(product_name) ASC
            LIMIT 1
        """, (f"%{name}%",)).fetchone()
        if fuzzy:
            return fuzzy

        reverse_fuzzy = conn.execute("""
            SELECT product_name, hsn_code, gst_rate
            FROM hsn_master
            WHERE ? LIKE '%' || LOWER(product_name) || '%'
            ORDER BY LENGTH(product_name) DESC
            LIMIT 1
        """, (name,)).fetchone()
        return reverse_fuzzy


def calculate_tax_from_product(product_name: str, base_amount: float):
    base_amount = float(base_amount or 0)
    row = lookup_hsn(product_name)
    if not row:
        return {
            "matched": False,
            "product_name": product_name,
            "hsn_code": "",
            "gst_rate": 0.0,
            "tax_amount": 0.0,
            "total_amount": round(base_amount, 2),
        }

    gst_rate = float(row["gst_rate"])
    tax_amount = base_amount * gst_rate / 100.0
    total_amount = base_amount + tax_amount

    return {
        "matched": True,
        "product_name": row["product_name"],
        "hsn_code": row["hsn_code"],
        "gst_rate": gst_rate,
        "tax_amount": round(tax_amount, 2),
        "total_amount": round(total_amount, 2),
    }


def pick_default_ledgers(company_id, voucher_type):
    ledgers = list_ledgers(company_id)
    if len(ledgers) < 2:
        return None, None

    cash_or_bank = None
    sales_or_purchase = None

    for row in ledgers:
        lname = clean_text(row["ledger_name"]).lower()
        gname = clean_text(row["group_name"]).lower()

        if not cash_or_bank and ("cash" in lname or "bank" in lname or "bank" in gname):
            cash_or_bank = row

        if voucher_type == "Purchase" and (gname == "purchase accounts" or "purchase" in lname):
            sales_or_purchase = row

        if voucher_type == "Sales" and (gname == "sales accounts" or "sales" in lname):
            sales_or_purchase = row

    if not cash_or_bank:
        cash_or_bank = ledgers[0]
    if not sales_or_purchase:
        sales_or_purchase = ledgers[1] if len(ledgers) > 1 else ledgers[0]

    return sales_or_purchase, cash_or_bank


def analyze_voucher(company_id, voucher_data, entries):
    flags = []
    explanations = []
    suggestions = []
    score_parts = []
    risk_score = 0
    ai_category = "General"

    voucher_number = clean_text(voucher_data.get("voucher_number"))
    voucher_type = clean_text(voucher_data.get("type"))
    narration = clean_text(voucher_data.get("narration"))
    party_name = clean_text(voucher_data.get("party_name"))
    payment_mode = clean_text(voucher_data.get("payment_mode")).lower()
    invoice_number = clean_text(voucher_data.get("invoice_number"))
    total_tax_amount = to_float(voucher_data.get("tax_amount"))

    total_debit = sum(to_float(e.get("debit")) for e in entries)
    total_credit = sum(to_float(e.get("credit")) for e in entries)
    total_amount = max(total_debit, total_credit)

    with get_conn() as conn:
        duplicate_voucher_no = conn.execute("""
            SELECT COUNT(*) AS c
            FROM vouchers
            WHERE company_id = ?
              AND LOWER(TRIM(voucher_number)) = LOWER(TRIM(?))
        """, (company_id, voucher_number)).fetchone()["c"]

        avg_row = conn.execute("""
            SELECT AVG(total_amt) AS avg_amt
            FROM (
                SELECT v.id, SUM(CASE WHEN ve.debit > ve.credit THEN ve.debit ELSE ve.credit END) AS total_amt
                FROM vouchers v
                JOIN voucher_entries ve ON ve.voucher_id = v.id
                WHERE v.company_id = ? AND v.type = ?
                GROUP BY v.id
                ORDER BY v.id DESC
                LIMIT 20
            )
        """, (company_id, voucher_type)).fetchone()
        avg_amt = float(avg_row["avg_amt"] or 0)

        duplicate_invoice = 0
        if invoice_number:
            duplicate_invoice = conn.execute("""
                SELECT COUNT(*) AS c
                FROM vouchers
                WHERE company_id = ?
                  AND LOWER(TRIM(invoice_number)) = LOWER(TRIM(?))
                  AND type = ?
            """, (company_id, invoice_number, voucher_type)).fetchone()["c"]

    ledger_map = get_ledger_map(company_id)
    has_cash_bank = False
    has_missing_gstin = False

    for item in entries:
        ledger = ledger_map.get(item["ledger_id"])
        if not ledger:
            continue

        gname = clean_text(ledger.get("group_name")).lower()
        lname = clean_text(ledger.get("ledger_name")).lower()
        gst_applicable = clean_text(ledger.get("gst_applicable"))

        if "bank" in gname or "bank" in lname or "cash" in lname:
            has_cash_bank = True

        if voucher_type in ("Sales", "Purchase") and gst_applicable == "Yes" and not clean_text(ledger.get("gst_number")):
            has_missing_gstin = True

    if "cash" in payment_mode or "bank" in payment_mode or "upi" in payment_mode:
        has_cash_bank = True

    if duplicate_voucher_no > 0:
        flags.append("Duplicate voucher number")
        explanations.append("Same voucher number already exists in this company.")
        suggestions.append("Use a unique voucher number.")
        score_parts.append("Duplicate voucher number: +20")
        risk_score += 20
        ai_category = "Duplicate"

    if invoice_number and duplicate_invoice > 0:
        flags.append("Duplicate invoice number")
        explanations.append("Same invoice number already exists for the same voucher type.")
        suggestions.append("Check whether this bill is entered twice.")
        score_parts.append("Duplicate invoice: +30")
        risk_score += 30
        ai_category = "Duplicate"

    if avg_amt > 0 and total_amount > avg_amt * 3:
        flags.append("Abnormally high voucher amount")
        explanations.append("Voucher amount is much higher than recent average for the same voucher type.")
        suggestions.append("Verify amount, invoice, and supporting documents.")
        score_parts.append("Unusual amount spike: +25")
        risk_score += 25
        ai_category = "Amount Anomaly"

    if has_cash_bank and total_amount > 50000:
        flags.append("High cash/bank transaction")
        explanations.append("High-value voucher linked with cash/bank flow detected.")
        suggestions.append("Keep bank proof, narration, and supporting invoice ready.")
        score_parts.append("High cash/bank transaction: +20")
        risk_score += 20
        if ai_category == "General":
            ai_category = "Cash Flow"

    if voucher_type in ("Sales", "Purchase") and not narration:
        flags.append("Missing narration")
        explanations.append("Sales/Purchase voucher has no narration.")
        suggestions.append("Add meaningful narration.")
        score_parts.append("Missing narration: +6")
        risk_score += 6

    if voucher_type in ("Sales", "Purchase") and not party_name:
        flags.append("Missing party name")
        explanations.append("Sales/Purchase voucher has no party name.")
        suggestions.append("Add customer/vendor name.")
        score_parts.append("Missing party name: +10")
        risk_score += 10
        if ai_category == "General":
            ai_category = "Missing Data"

    if voucher_type in ("Sales", "Purchase") and not invoice_number:
        flags.append("Missing invoice number")
        explanations.append("Sales/Purchase voucher has no invoice number.")
        suggestions.append("Add invoice reference number.")
        score_parts.append("Missing invoice number: +12")
        risk_score += 12
        if ai_category == "General":
            ai_category = "Missing Data"

    if voucher_type in ("Sales", "Purchase") and total_amount > 0 and total_tax_amount == 0:
        flags.append("Tax amount is zero")
        explanations.append("Sales/Purchase voucher has zero tax amount.")
        suggestions.append("Check HSN/GST mapping and invoice tax details.")
        score_parts.append("Zero tax amount: +8")
        risk_score += 8
        if ai_category == "General":
            ai_category = "GST"

    if has_missing_gstin:
        flags.append("Missing GST number on GST-applicable ledger")
        explanations.append("GST-applicable ledger linked to this voucher has no GST number.")
        suggestions.append("Update ledger GSTIN.")
        score_parts.append("Missing GSTIN: +18")
        risk_score += 18
        ai_category = "GST"

    if abs(total_debit - total_credit) > 0.009:
        flags.append("Voucher not balanced")
        explanations.append("Debit total and credit total are not equal.")
        suggestions.append("Correct debit/credit lines before saving.")
        score_parts.append("Unbalanced voucher: +40")
        risk_score += 40
        ai_category = "Accounting"

    if risk_score >= 50:
        risk_level = "High"
    elif risk_score >= 20:
        risk_level = "Medium"
    else:
        risk_level = "Low"

    if not flags:
        flags = ["No major scrutiny issue"]
        explanations = ["No important anomaly detected by current rules."]
        suggestions = ["Keep documents ready."]
        score_parts = ["No risk additions"]
        risk_score = 0
        risk_level = "Low"
        ai_category = "General"

    return {
        "risk_level": risk_level,
        "risk_score": min(risk_score, 100),
        "flags": " | ".join(flags),
        "explanation": " ".join(explanations),
        "suggested_fix": " ".join(suggestions),
        "score_breakdown": " | ".join(score_parts),
        "category": ai_category,
        "total_amount": round(total_amount, 2)
    }


def sync_ai_transactions(company_id, voucher_id):
    with get_conn() as conn:
        voucher = conn.execute("""
            SELECT * FROM vouchers WHERE id = ? AND company_id = ?
        """, (voucher_id, company_id)).fetchone()

        if not voucher:
            return

        conn.execute("DELETE FROM ai_transactions WHERE voucher_id = ?", (voucher_id,))

        entries = conn.execute("""
            SELECT ve.*, l.ledger_name, l.gst_number
            FROM voucher_entries ve
            JOIN ledgers l ON l.id = ve.ledger_id
            WHERE ve.voucher_id = ?
        """, (voucher_id,)).fetchall()

        for e in entries:
            amount = max(float(e["debit"] or 0), float(e["credit"] or 0))
            conn.execute("""
                INSERT INTO ai_transactions (
                    company_id, voucher_id, date, voucher_type, voucher_number,
                    party_name, ledger_name, amount, tax_amount, payment_mode,
                    gstin, invoice_number, source_table, risk_flags
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                company_id,
                voucher_id,
                voucher["date"],
                voucher["type"],
                voucher["voucher_number"],
                voucher["party_name"],
                e["ledger_name"],
                amount,
                float(voucher["tax_amount"] or 0),
                voucher["payment_mode"],
                e["gst_number"],
                voucher["invoice_number"],
                "vouchers",
                voucher["ai_flags"]
            ))


def ai_dashboard(company_id):
    if not company_id:
        zero_stats = {
            "total": 0,
            "high_count": 0,
            "medium_count": 0,
            "low_count": 0,
            "avg_score": 0
        }
        return {
            "stats": zero_stats,
            "risky_vouchers": [],
            "duplicate_invoices": 0,
            "high_cash_entries": 0,
            "missing_gstin_count": 0,
            "risk_alerts": [],
            "compliance_items": [],
            "ai_summary": "No active company selected."
        }

    with get_conn() as conn:
        stats = conn.execute("""
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN ai_risk_level = 'High' THEN 1 ELSE 0 END), 0) AS high_count,
                COALESCE(SUM(CASE WHEN ai_risk_level = 'Medium' THEN 1 ELSE 0 END), 0) AS medium_count,
                COALESCE(SUM(CASE WHEN ai_risk_level = 'Low' THEN 1 ELSE 0 END), 0) AS low_count,
                COALESCE(AVG(ai_risk_score), 0) AS avg_score
            FROM vouchers
            WHERE company_id = ?
        """, (company_id,)).fetchone()

        risky_vouchers = conn.execute("""
            SELECT * FROM vouchers
            WHERE company_id = ?
            ORDER BY ai_risk_score DESC, id DESC
            LIMIT 15
        """, (company_id,)).fetchall()

        duplicate_invoices = conn.execute("""
            SELECT COUNT(*) AS c FROM (
                SELECT LOWER(TRIM(invoice_number)) AS invoice_number, type
                FROM vouchers
                WHERE company_id = ?
                  AND invoice_number IS NOT NULL
                  AND TRIM(invoice_number) <> ''
                GROUP BY LOWER(TRIM(invoice_number)), type
                HAVING COUNT(*) > 1
            )
        """, (company_id,)).fetchone()["c"]

        high_cash_entries = conn.execute("""
            SELECT COUNT(*) AS c
            FROM vouchers
            WHERE company_id = ?
              AND ai_flags LIKE '%High cash/bank transaction%'
        """, (company_id,)).fetchone()["c"]

        missing_gstin_count = conn.execute("""
            SELECT COUNT(*) AS c
            FROM vouchers
            WHERE company_id = ?
              AND ai_flags LIKE '%Missing GST number%'
        """, (company_id,)).fetchone()["c"]

        risk_alerts = conn.execute("""
            SELECT *
            FROM vouchers
            WHERE company_id = ? AND ai_risk_score > 0
            ORDER BY ai_risk_score DESC, id DESC
            LIMIT 50
        """, (company_id,)).fetchall()

        compliance_items = conn.execute("""
            SELECT *
            FROM vouchers
            WHERE company_id = ?
              AND (
                ai_flags LIKE '%Missing GST%'
                OR ai_flags LIKE '%Missing narration%'
                OR ai_flags LIKE '%Missing party name%'
                OR ai_flags LIKE '%Missing invoice number%'
                OR ai_flags LIKE '%Tax amount is zero%'
              )
            ORDER BY id DESC
            LIMIT 50
        """, (company_id,)).fetchall()

    ai_summary = build_ai_summary(stats, duplicate_invoices, high_cash_entries, missing_gstin_count)

    return {
        "stats": stats,
        "risky_vouchers": risky_vouchers,
        "duplicate_invoices": duplicate_invoices,
        "high_cash_entries": high_cash_entries,
        "missing_gstin_count": missing_gstin_count,
        "risk_alerts": risk_alerts,
        "compliance_items": compliance_items,
        "ai_summary": ai_summary
    }


def parse_invoice_text(invoice_text: str):
    text = invoice_text or ""

    def find_first(patterns, default=""):
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return clean_text(match.group(1))
        return default

    invoice_number = find_first([
        r"invoice\s*no[:\-]?\s*([A-Z0-9\-\/]+)",
        r"invoice\s*number[:\-]?\s*([A-Z0-9\-\/]+)",
        r"inv[:\-]?\s*([A-Z0-9\-\/]+)",
    ])

    party_name = find_first([
        r"vendor[:\-]?\s*([A-Za-z0-9 &\.\-]+)",
        r"supplier[:\-]?\s*([A-Za-z0-9 &\.\-]+)",
        r"party[:\-]?\s*([A-Za-z0-9 &\.\-]+)",
        r"customer[:\-]?\s*([A-Za-z0-9 &\.\-]+)",
    ])

    product_name = find_first([
        r"item[:\-]?\s*([A-Za-z0-9 \-&\.]+)",
        r"product[:\-]?\s*([A-Za-z0-9 \-&\.]+)",
        r"description[:\-]?\s*([A-Za-z0-9 \-&\.]+)",
    ])

    voucher_date = find_first([
        r"date[:\-]?\s*([0-9]{4}\-[0-9]{2}\-[0-9]{2})",
        r"date[:\-]?\s*([0-9]{2}\/[0-9]{2}\/[0-9]{4})",
        r"date[:\-]?\s*([0-9]{2}\-[0-9]{2}\-[0-9]{4})",
    ])

    base_amount_raw = find_first([
        r"taxable\s*value[:\-]?\s*₹?\s*([0-9,]+(?:\.[0-9]+)?)",
        r"amount[:\-]?\s*₹?\s*([0-9,]+(?:\.[0-9]+)?)",
        r"subtotal[:\-]?\s*₹?\s*([0-9,]+(?:\.[0-9]+)?)",
    ])

    hsn_code = find_first([
        r"hsn[:\-]?\s*([0-9]{4,8})",
        r"hsn\s*code[:\-]?\s*([0-9]{4,8})",
    ])

    gst_rate_raw = find_first([
        r"gst[:\-]?\s*([0-9]+(?:\.[0-9]+)?)\s*%",
        r"tax[:\-]?\s*([0-9]+(?:\.[0-9]+)?)\s*%",
    ])

    payment_mode = find_first([
        r"payment\s*mode[:\-]?\s*([A-Za-z ]+)",
        r"mode[:\-]?\s*([A-Za-z ]+)",
    ], default="Bank")

    def parse_amount(value):
        if not value:
            return 0.0
        try:
            return float(value.replace(",", "").strip())
        except ValueError:
            return 0.0

    base_amount = parse_amount(base_amount_raw)
    gst_rate = to_float(gst_rate_raw)

    return {
        "invoice_number": invoice_number,
        "party_name": party_name,
        "product_name": product_name,
        "date": voucher_date,
        "base_amount": base_amount,
        "hsn_code": hsn_code,
        "gst_rate": gst_rate,
        "payment_mode": payment_mode.strip() or "Bank",
        "raw_text": text.strip(),
    }


def build_context(request: Request, screen="dashboard", selected_voucher_type="", scanner_result=None, message=""):
    active_company = get_active_company(request)
    company_id = active_company["id"] if active_company else None
    data = ai_dashboard(company_id)

    return {
        "request": request,
        "screen": screen,
        "selected_voucher_type": selected_voucher_type,
        "active_company": active_company,
        "companies": list_companies(),
        "ledgers": list_ledgers(company_id),
        "vouchers": list_vouchers(company_id),
        "summary": dashboard_summary(company_id),
        "default_ledger_groups": DEFAULT_LEDGER_GROUPS,
        "stats": data["stats"],
        "risky_vouchers": data["risky_vouchers"],
        "duplicate_invoices": data["duplicate_invoices"],
        "high_cash_entries": data["high_cash_entries"],
        "missing_gstin_count": data["missing_gstin_count"],
        "risk_alerts": data["risk_alerts"],
        "compliance_items": data["compliance_items"],
        "ai_summary": data["ai_summary"],
        "scanner_result": scanner_result,
        "message": message
    }


@app.get("/", response_class=HTMLResponse)
def home(request: Request, screen: str = "dashboard", type: str = "", message: str = ""):
    return templates.TemplateResponse(
        "index.html",
        build_context(request, screen=screen, selected_voucher_type=type, message=message)
    )


@app.get("/api/hsn-lookup")
def api_hsn_lookup(product: str = "", amount: float = 0):
    result = calculate_tax_from_product(product, amount)
    return JSONResponse(result)


@app.post("/company")
def create_company_route(
    request: Request,
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
    with get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO companies (
                name, mailing_name, address, state, country, phone, email,
                financial_year_start, books_from, currency, maintain_inventory, enable_gst
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            clean_text(name),
            clean_text(mailing_name),
            clean_text(address),
            clean_text(state),
            clean_text(country) or "India",
            clean_text(phone),
            clean_text(email),
            financial_year_start,
            books_from,
            clean_text(currency) or "₹",
            maintain_inventory,
            enable_gst
        ))
        company_id = cur.lastrowid

    response = RedirectResponse(url="/?screen=company&message=Company%20created%20successfully", status_code=303)
    response.set_cookie("active_company_id", str(company_id))
    return response


@app.get("/company/select/{company_id}")
def select_company(company_id: int):
    with get_conn() as conn:
        company = conn.execute("SELECT id FROM companies WHERE id = ?", (company_id,)).fetchone()

    response = RedirectResponse(url="/?screen=dashboard", status_code=303)
    if company:
        response.set_cookie("active_company_id", str(company_id))
    else:
        response.delete_cookie("active_company_id")
    return response


@app.get("/company/delete/{company_id}")
def delete_company_route(request: Request, company_id: int):
    with get_conn() as conn:
        conn.execute("DELETE FROM companies WHERE id = ?", (company_id,))

    response = RedirectResponse(url="/?screen=company&message=Company%20deleted", status_code=303)
    if request.cookies.get("active_company_id") == str(company_id):
        response.delete_cookie("active_company_id")
    return response


@app.post("/ledger")
def create_ledger_route(
    request: Request,
    ledger_name: str = Form(...),
    group_name: str = Form(...),
    opening_balance: float = Form(0),
    balance_type: str = Form("Debit"),
    gst_applicable: str = Form("No"),
    gst_number: str = Form(""),
    address: str = Form(""),
    phone: str = Form(""),
    email: str = Form(""),
):
    active_company = get_active_company(request)
    if not active_company:
        return RedirectResponse(url="/?screen=ledger&message=Select%20a%20company%20first", status_code=303)

    ledger_name = clean_text(ledger_name)
    if not ledger_name:
        return RedirectResponse(url="/?screen=ledger&message=Ledger%20name%20is%20required", status_code=303)

    with get_conn() as conn:
        existing = conn.execute("""
            SELECT COUNT(*) AS c
            FROM ledgers
            WHERE company_id = ? AND LOWER(TRIM(ledger_name)) = LOWER(TRIM(?))
        """, (active_company["id"], ledger_name)).fetchone()["c"]

        if existing > 0:
            return RedirectResponse(url="/?screen=ledger&message=Ledger%20already%20exists", status_code=303)

        conn.execute("""
            INSERT INTO ledgers (
                company_id, ledger_name, group_name, opening_balance,
                balance_type, gst_applicable, gst_number, address, phone, email
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            active_company["id"],
            ledger_name,
            clean_text(group_name),
            to_float(opening_balance),
            clean_text(balance_type) or "Debit",
            clean_text(gst_applicable) or "No",
            clean_text(gst_number),
            clean_text(address),
            clean_text(phone),
            clean_text(email)
        ))

    return RedirectResponse(url="/?screen=ledger&message=Ledger%20created%20successfully", status_code=303)


@app.get("/ledger/delete/{ledger_id}")
def delete_ledger_route(ledger_id: int):
    with get_conn() as conn:
        used = conn.execute("""
            SELECT COUNT(*) AS c
            FROM voucher_entries
            WHERE ledger_id = ?
        """, (ledger_id,)).fetchone()["c"]

        if used > 0:
            return RedirectResponse(url="/?screen=ledger&message=Ledger%20is%20used%20in%20vouchers%20and%20cannot%20be%20deleted", status_code=303)

        conn.execute("DELETE FROM ledgers WHERE id = ?", (ledger_id,))

    return RedirectResponse(url="/?screen=ledger&message=Ledger%20deleted", status_code=303)


@app.post("/voucher")
async def create_voucher_route(request: Request):
    active_company = get_active_company(request)
    if not active_company:
        return RedirectResponse(url="/?screen=voucher&message=Select%20a%20company%20first", status_code=303)

    form = await request.form()

    voucher_number = clean_text(form.get("voucherNumber"))
    voucher_date = clean_text(form.get("date"))
    voucher_type = clean_text(form.get("type"))
    narration = clean_text(form.get("narration"))
    party_name = clean_text(form.get("party_name"))
    payment_mode = clean_text(form.get("payment_mode"))
    invoice_number = clean_text(form.get("invoice_number"))
    product_name = clean_text(form.get("product_name"))
    hsn_code = clean_text(form.get("hsn_code"))

    base_amount = to_float(form.get("base_amount"))
    gst_rate = to_float(form.get("gst_rate"))
    tax_amount = to_float(form.get("tax_amount"))
    total_amount = to_float(form.get("total_amount"))

    if product_name and (gst_rate == 0 or not hsn_code):
        hsn_result = calculate_tax_from_product(product_name, base_amount)
        if hsn_result["matched"]:
            hsn_code = hsn_result["hsn_code"]
            gst_rate = hsn_result["gst_rate"]
            tax_amount = hsn_result["tax_amount"]
            total_amount = hsn_result["total_amount"]

    ledger_ids = form.getlist("ledger_id")
    debits = form.getlist("debit")
    credits = form.getlist("credit")

    cleaned_entries = []
    for ledger_id, debit, credit in zip(ledger_ids, debits, credits):
        if not ledger_id:
            continue
        d = to_float(debit)
        c = to_float(credit)
        if d <= 0 and c <= 0:
            continue
        cleaned_entries.append({
            "ledger_id": int(ledger_id),
            "debit": d,
            "credit": c
        })

    if len(cleaned_entries) < 2:
        return RedirectResponse(url="/?screen=voucher&message=At%20least%20two%20voucher%20lines%20are%20required", status_code=303)

    debit_total = sum(x["debit"] for x in cleaned_entries)
    credit_total = sum(x["credit"] for x in cleaned_entries)

    if round(debit_total, 2) != round(credit_total, 2):
        return RedirectResponse(url="/?screen=voucher&message=Debit%20and%20Credit%20must%20match", status_code=303)

    voucher_data = {
        "voucher_number": voucher_number,
        "date": voucher_date,
        "type": voucher_type,
        "narration": narration,
        "party_name": party_name,
        "payment_mode": payment_mode,
        "invoice_number": invoice_number,
        "tax_amount": tax_amount
    }

    ai = analyze_voucher(active_company["id"], voucher_data, cleaned_entries)

    with get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO vouchers (
                company_id, voucher_number, date, type, narration,
                party_name, payment_mode, invoice_number,
                product_name, hsn_code, gst_rate, tax_amount, base_amount, total_amount,
                ai_risk_level, ai_risk_score, ai_flags, ai_explanation,
                ai_suggested_fix, ai_score_breakdown, review_status, ai_category
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            active_company["id"],
            voucher_number,
            voucher_date,
            voucher_type,
            narration,
            party_name,
            payment_mode,
            invoice_number,
            product_name,
            hsn_code,
            gst_rate,
            tax_amount,
            base_amount,
            total_amount,
            ai["risk_level"],
            ai["risk_score"],
            ai["flags"],
            ai["explanation"],
            ai["suggested_fix"],
            ai["score_breakdown"],
            "Pending",
            ai["category"]
        ))
        voucher_id = cur.lastrowid

        for item in cleaned_entries:
            conn.execute("""
                INSERT INTO voucher_entries (voucher_id, ledger_id, debit, credit)
                VALUES (?, ?, ?, ?)
            """, (voucher_id, item["ledger_id"], item["debit"], item["credit"]))

    sync_ai_transactions(active_company["id"], voucher_id)
    return RedirectResponse(url="/?screen=voucher&message=Voucher%20saved%20successfully", status_code=303)


@app.post("/scanner/preview", response_class=HTMLResponse)
def scanner_preview(request: Request, invoice_text: str = Form(...), voucher_type: str = Form("Purchase")):
    parsed = parse_invoice_text(invoice_text)
    hsn_result = calculate_tax_from_product(parsed["product_name"], parsed["base_amount"])

    scanner_result = {
        "voucher_type": voucher_type,
        "invoice_text": invoice_text,
        "invoice_number": parsed["invoice_number"],
        "party_name": parsed["party_name"],
        "product_name": parsed["product_name"],
        "date": parsed["date"],
        "payment_mode": parsed["payment_mode"] or "Bank",
        "base_amount": parsed["base_amount"],
        "hsn_code": parsed["hsn_code"] or hsn_result["hsn_code"],
        "gst_rate": parsed["gst_rate"] or hsn_result["gst_rate"],
        "tax_amount": hsn_result["tax_amount"] if hsn_result["matched"] else 0,
        "total_amount": hsn_result["total_amount"] if hsn_result["matched"] else parsed["base_amount"],
        "matched": hsn_result["matched"],
    }

    return templates.TemplateResponse(
        "index.html",
        build_context(request, screen="scanner", scanner_result=scanner_result)
    )


@app.post("/scanner/create")
def scanner_create(
    request: Request,
    voucher_type: str = Form("Purchase"),
    invoice_number: str = Form(""),
    party_name: str = Form(""),
    product_name: str = Form(""),
    date: str = Form(""),
    payment_mode: str = Form("Bank"),
    base_amount: float = Form(0),
    hsn_code: str = Form(""),
    gst_rate: float = Form(0),
    tax_amount: float = Form(0),
    total_amount: float = Form(0),
):
    active_company = get_active_company(request)
    if not active_company:
        return RedirectResponse(url="/?screen=scanner&message=Select%20a%20company%20first", status_code=303)

    business_ledger, cash_bank_ledger = pick_default_ledgers(active_company["id"], voucher_type)
    if not business_ledger or not cash_bank_ledger:
        return RedirectResponse(url="/?screen=scanner&message=Need%20at%20least%20one%20business%20ledger%20and%20one%20cash/bank%20ledger", status_code=303)

    voucher_number = f"SCAN-{abs(hash((invoice_number, party_name, date, product_name))) % 1000000}"
    narration = f"Auto-created from invoice scanner for {clean_text(product_name)}"

    if voucher_type == "Purchase":
        cleaned_entries = [
            {"ledger_id": int(business_ledger["id"]), "debit": float(total_amount), "credit": 0.0},
            {"ledger_id": int(cash_bank_ledger["id"]), "debit": 0.0, "credit": float(total_amount)},
        ]
    else:
        cleaned_entries = [
            {"ledger_id": int(cash_bank_ledger["id"]), "debit": float(total_amount), "credit": 0.0},
            {"ledger_id": int(business_ledger["id"]), "debit": 0.0, "credit": float(total_amount)},
        ]

    voucher_data = {
        "voucher_number": voucher_number,
        "date": date or "2026-03-10",
        "type": voucher_type,
        "narration": narration,
        "party_name": clean_text(party_name),
        "payment_mode": clean_text(payment_mode),
        "invoice_number": clean_text(invoice_number),
        "tax_amount": tax_amount
    }

    ai = analyze_voucher(active_company["id"], voucher_data, cleaned_entries)

    with get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO vouchers (
                company_id, voucher_number, date, type, narration,
                party_name, payment_mode, invoice_number,
                product_name, hsn_code, gst_rate, tax_amount, base_amount, total_amount,
                ai_risk_level, ai_risk_score, ai_flags, ai_explanation,
                ai_suggested_fix, ai_score_breakdown, review_status, ai_category
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            active_company["id"],
            voucher_number,
            date or "2026-03-10",
            voucher_type,
            narration,
            clean_text(party_name),
            clean_text(payment_mode),
            clean_text(invoice_number),
            clean_text(product_name),
            clean_text(hsn_code),
            to_float(gst_rate),
            to_float(tax_amount),
            to_float(base_amount),
            to_float(total_amount),
            ai["risk_level"],
            ai["risk_score"],
            ai["flags"],
            ai["explanation"],
            ai["suggested_fix"],
            ai["score_breakdown"],
            "Pending",
            ai["category"]
        ))
        voucher_id = cur.lastrowid

        for item in cleaned_entries:
            conn.execute("""
                INSERT INTO voucher_entries (voucher_id, ledger_id, debit, credit)
                VALUES (?, ?, ?, ?)
            """, (voucher_id, item["ledger_id"], item["debit"], item["credit"]))

    sync_ai_transactions(active_company["id"], voucher_id)
    return RedirectResponse(url="/?screen=voucher&message=Scanner%20voucher%20created%20successfully", status_code=303)


@app.get("/voucher/delete/{voucher_id}")
def delete_voucher_route(voucher_id: int):
    with get_conn() as conn:
        conn.execute("DELETE FROM vouchers WHERE id = ?", (voucher_id,))
    return RedirectResponse(url="/?screen=voucher&message=Voucher%20deleted", status_code=303)
