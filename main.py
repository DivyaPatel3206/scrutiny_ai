from contextlib import contextmanager
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import sqlite3
import re
app = FastAPI(title="Tally Clone + AI Scrutiny + HSN + Invoice Scanner")
templates = Jinja2Templates(directory="templates")

DB_NAME = "tally.db"

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


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                mailing_name TEXT,
                address TEXT,
                state TEXT,
                country TEXT DEFAULT 'India',
                phone TEXT,
                email TEXT,
                financial_year_start TEXT,
                books_from TEXT,
                currency TEXT DEFAULT '₹',
                maintain_inventory TEXT DEFAULT 'Yes',
                enable_gst TEXT DEFAULT 'Yes'
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ledgers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                ledger_name TEXT NOT NULL,
                group_name TEXT NOT NULL,
                opening_balance REAL DEFAULT 0,
                balance_type TEXT DEFAULT 'Debit',
                gst_applicable TEXT DEFAULT 'No',
                gst_number TEXT,
                address TEXT,
                phone TEXT,
                email TEXT,
                UNIQUE(company_id, ledger_name)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS vouchers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                voucher_number TEXT NOT NULL,
                date TEXT NOT NULL,
                type TEXT NOT NULL,
                narration TEXT,
                party_name TEXT,
                payment_mode TEXT,
                invoice_number TEXT,
                product_name TEXT,
                hsn_code TEXT,
                gst_rate REAL DEFAULT 0,
                tax_amount REAL DEFAULT 0,
                base_amount REAL DEFAULT 0,
                total_amount REAL DEFAULT 0,
                ai_risk_level TEXT DEFAULT 'Low',
                ai_risk_score INTEGER DEFAULT 0,
                ai_flags TEXT DEFAULT '',
                ai_explanation TEXT DEFAULT '',
                ai_suggested_fix TEXT DEFAULT '',
                ai_score_breakdown TEXT DEFAULT '',
                review_status TEXT DEFAULT 'Pending',
                ai_category TEXT DEFAULT 'General'
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS voucher_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                voucher_id INTEGER NOT NULL,
                ledger_id INTEGER NOT NULL,
                debit REAL DEFAULT 0,
                credit REAL DEFAULT 0
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                voucher_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                voucher_type TEXT NOT NULL,
                voucher_number TEXT NOT NULL,
                party_name TEXT,
                ledger_name TEXT,
                amount REAL DEFAULT 0,
                tax_amount REAL DEFAULT 0,
                payment_mode TEXT,
                gstin TEXT,
                invoice_number TEXT,
                source_table TEXT DEFAULT 'vouchers',
                risk_flags TEXT DEFAULT ''
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS hsn_master (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_name TEXT NOT NULL,
                hsn_code TEXT NOT NULL,
                gst_rate REAL NOT NULL
            )
        """)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_ledgers_company ON ledgers(company_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_vouchers_company ON vouchers(company_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_voucher ON voucher_entries(voucher_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ai_tx_company ON ai_transactions(company_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ai_tx_voucher ON ai_transactions(voucher_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hsn_product ON hsn_master(product_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hsn_code ON hsn_master(hsn_code)")

        existing_cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(vouchers)").fetchall()
        }

        alter_statements = {
            "ai_score_breakdown": "ALTER TABLE vouchers ADD COLUMN ai_score_breakdown TEXT DEFAULT ''",
            "review_status": "ALTER TABLE vouchers ADD COLUMN review_status TEXT DEFAULT 'Pending'",
            "ai_category": "ALTER TABLE vouchers ADD COLUMN ai_category TEXT DEFAULT 'General'",
            "party_name": "ALTER TABLE vouchers ADD COLUMN party_name TEXT",
            "payment_mode": "ALTER TABLE vouchers ADD COLUMN payment_mode TEXT",
            "invoice_number": "ALTER TABLE vouchers ADD COLUMN invoice_number TEXT",
            "tax_amount": "ALTER TABLE vouchers ADD COLUMN tax_amount REAL DEFAULT 0",
            "ai_explanation": "ALTER TABLE vouchers ADD COLUMN ai_explanation TEXT DEFAULT ''",
            "ai_suggested_fix": "ALTER TABLE vouchers ADD COLUMN ai_suggested_fix TEXT DEFAULT ''",
            "product_name": "ALTER TABLE vouchers ADD COLUMN product_name TEXT",
            "hsn_code": "ALTER TABLE vouchers ADD COLUMN hsn_code TEXT",
            "gst_rate": "ALTER TABLE vouchers ADD COLUMN gst_rate REAL DEFAULT 0",
            "base_amount": "ALTER TABLE vouchers ADD COLUMN base_amount REAL DEFAULT 0",
            "total_amount": "ALTER TABLE vouchers ADD COLUMN total_amount REAL DEFAULT 0",
        }

        for col, stmt in alter_statements.items():
            if col not in existing_cols:
                conn.execute(stmt)

        seed_hsn_master(conn)


def seed_hsn_master(conn):
    count = conn.execute("SELECT COUNT(*) FROM hsn_master").fetchone()[0]
    if count > 0:
        return

    sample_rows = [
        ("car", "8703", 28),
        ("motor car", "8703", 28),
        ("suv", "8703", 28),
        ("laptop", "8471", 18),
        ("computer", "8471", 18),
        ("mobile", "8517", 18),
        ("smartphone", "8517", 18),
        ("gold", "7108", 3),
        ("cement", "2523", 28),
        ("book", "4901", 0),
        ("printer", "8443", 18),
        ("air conditioner", "8415", 28),
        ("tv", "8528", 18),
        ("refrigerator", "8418", 18),
        ("biscuit", "1905", 18),
    ]
    conn.executemany(
        "INSERT INTO hsn_master (product_name, hsn_code, gst_rate) VALUES (?, ?, ?)",
        sample_rows
    )


@app.on_event("startup")
def startup():
    init_db()


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
        return conn.execute("SELECT * FROM companies WHERE id = ?", (company_id,)).fetchone()


def list_companies():
    with get_conn() as conn:
        return conn.execute("SELECT * FROM companies ORDER BY id DESC").fetchall()


def list_ledgers(company_id):
    if not company_id:
        return []
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM ledgers WHERE company_id = ? ORDER BY ledger_name ASC",
            (company_id,)
        ).fetchall()


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

        result = []
        for voucher in vouchers:
            entries = conn.execute("""
                SELECT ve.*, l.ledger_name, l.group_name
                FROM voucher_entries ve
                JOIN ledgers l ON l.id = ve.ledger_id
                WHERE ve.voucher_id = ?
                ORDER BY ve.id ASC
            """, (voucher["id"],)).fetchall()
            result.append({"voucher": voucher, "entries": entries})
        return result


def dashboard_summary(company_id):
    with get_conn() as conn:
        company_count = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]

        if not company_id:
            return {
                "company_count": company_count,
                "ledger_count": 0,
                "voucher_count": 0,
                "debit_total": 0.0,
                "credit_total": 0.0
            }

        ledger_count = conn.execute(
            "SELECT COUNT(*) FROM ledgers WHERE company_id = ?",
            (company_id,)
        ).fetchone()[0]

        voucher_count = conn.execute(
            "SELECT COUNT(*) FROM vouchers WHERE company_id = ?",
            (company_id,)
        ).fetchone()[0]

        debit_total = conn.execute("""
            SELECT COALESCE(SUM(ve.debit), 0)
            FROM voucher_entries ve
            JOIN vouchers v ON v.id = ve.voucher_id
            WHERE v.company_id = ?
        """, (company_id,)).fetchone()[0]

        credit_total = conn.execute("""
            SELECT COALESCE(SUM(ve.credit), 0)
            FROM voucher_entries ve
            JOIN vouchers v ON v.id = ve.voucher_id
            WHERE v.company_id = ?
        """, (company_id,)).fetchone()[0]

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
    if not product_name.strip():
        return None

    name = product_name.strip().lower()

    with get_conn() as conn:
        exact = conn.execute("""
            SELECT product_name, hsn_code, gst_rate
            FROM hsn_master
            WHERE LOWER(product_name) = ?
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
    row = lookup_hsn(product_name)
    if not row:
        return {
            "matched": False,
            "product_name": product_name,
            "hsn_code": "",
            "gst_rate": 0.0,
            "tax_amount": 0.0,
            "total_amount": float(base_amount or 0),
        }

    gst_rate = float(row["gst_rate"])
    tax_amount = float(base_amount or 0) * gst_rate / 100.0
    total_amount = float(base_amount or 0) + tax_amount

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
        lname = row["ledger_name"].lower()
        gname = row["group_name"].lower()

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

    total_debit = sum(e["debit"] for e in entries)
    total_credit = sum(e["credit"] for e in entries)
    total_amount = max(total_debit, total_credit)

    with get_conn() as conn:
        duplicate_voucher_no = conn.execute("""
            SELECT COUNT(*) FROM vouchers
            WHERE company_id = ? AND voucher_number = ?
        """, (company_id, voucher_data["voucher_number"])).fetchone()[0]

        avg_row = conn.execute("""
            SELECT AVG(total_amt) AS avg_amt
            FROM (
                SELECT v.id, SUM(ve.debit) AS total_amt
                FROM vouchers v
                JOIN voucher_entries ve ON ve.voucher_id = v.id
                WHERE v.company_id = ? AND v.type = ?
                GROUP BY v.id
                ORDER BY v.id DESC
                LIMIT 20
            )
        """, (company_id, voucher_data["type"])).fetchone()
        avg_amt = float(avg_row["avg_amt"] or 0)

        duplicate_invoice = 0
        if voucher_data["invoice_number"]:
            duplicate_invoice = conn.execute("""
                SELECT COUNT(*)
                FROM vouchers
                WHERE company_id = ?
                  AND invoice_number = ?
                  AND type = ?
            """, (
                company_id,
                voucher_data["invoice_number"],
                voucher_data["type"]
            )).fetchone()[0]

        duplicate_vendor_bill = 0
        if voucher_data["party_name"] and voucher_data["invoice_number"]:
            duplicate_vendor_bill = conn.execute("""
                SELECT COUNT(*)
                FROM vouchers
                WHERE company_id = ?
                  AND party_name = ?
                  AND invoice_number = ?
                  AND type = ?
            """, (
                company_id,
                voucher_data["party_name"],
                voucher_data["invoice_number"],
                voucher_data["type"]
            )).fetchone()[0]

        similar_party_amount = 0
        if voucher_data["party_name"]:
            similar_party_amount = conn.execute("""
                SELECT COUNT(*)
                FROM vouchers
                WHERE company_id = ?
                  AND party_name = ?
                  AND type = ?
                  AND ABS(
                        COALESCE((
                            SELECT SUM(ve.debit)
                            FROM voucher_entries ve
                            WHERE ve.voucher_id = vouchers.id
                        ), 0) - ?
                  ) < 1
            """, (
                company_id,
                voucher_data["party_name"],
                voucher_data["type"],
                total_amount
            )).fetchone()[0]

    ledger_map = get_ledger_map(company_id)

    has_cash_bank = False
    has_missing_gstin = False

    for item in entries:
        ledger = ledger_map.get(item["ledger_id"])
        if not ledger:
            continue

        gname = (ledger["group_name"] or "").lower()
        lname = (ledger["ledger_name"] or "").lower()

        if "bank" in gname or "bank" in lname or "cash" in lname:
            has_cash_bank = True

        if voucher_data["type"] in ("Sales", "Purchase") and ledger["gst_applicable"] == "Yes" and not (ledger["gst_number"] or "").strip():
            has_missing_gstin = True

    if duplicate_voucher_no > 0:
        flags.append("Duplicate voucher number")
        explanations.append("Same voucher number already exists in this company.")
        suggestions.append("Use a unique voucher number.")
        score_parts.append("Duplicate voucher number: +20")
        risk_score += 20
        ai_category = "Duplicate"

    if voucher_data["invoice_number"] and duplicate_invoice > 0:
        flags.append("Duplicate invoice number")
        explanations.append("Same invoice number already exists for the same voucher type.")
        suggestions.append("Check whether this bill is entered twice.")
        score_parts.append("Duplicate invoice: +30")
        risk_score += 30
        ai_category = "Duplicate"

    if voucher_data["party_name"] and voucher_data["invoice_number"] and duplicate_vendor_bill > 0:
        flags.append("Possible duplicate vendor bill")
        explanations.append("Same vendor and same invoice number already exist.")
        suggestions.append("Verify duplicate booking before filing returns.")
        score_parts.append("Duplicate vendor bill: +25")
        risk_score += 25
        ai_category = "Duplicate"

    if voucher_data["party_name"] and similar_party_amount > 0:
        flags.append("Same party same amount pattern")
        explanations.append("A similar amount already exists for the same party and voucher type.")
        suggestions.append("Check if this is repeated or split billing.")
        score_parts.append("Repeated vendor amount pattern: +10")
        risk_score += 10

    if avg_amt > 0 and total_amount > avg_amt * 3:
        flags.append("Abnormally high voucher amount")
        explanations.append("This voucher amount is much higher than recent average for the same type.")
        suggestions.append("Verify amount, party, and supporting documents.")
        score_parts.append("Unusual amount spike: +25")
        risk_score += 25
        ai_category = "Amount Anomaly"

    if total_amount >= 10000 and int(total_amount) == total_amount and int(total_amount) % 1000 == 0:
        flags.append("Rounded amount pattern")
        explanations.append("The amount is a large rounded figure, which may require review.")
        suggestions.append("Check invoice and actual breakup.")
        score_parts.append("Rounded amount: +8")
        risk_score += 8

    if has_cash_bank and total_amount > 50000:
        flags.append("High cash/bank transaction")
        explanations.append("High-value cash/bank linked voucher detected.")
        suggestions.append("Keep bank proof and supporting narration ready.")
        score_parts.append("High cash/bank transaction: +20")
        risk_score += 20
        ai_category = "Cash Flow"

    if voucher_data["payment_mode"].strip().lower() == "cash" and total_amount > 20000:
        flags.append("High cash payment")
        explanations.append("Cash payment exceeds safe scrutiny threshold.")
        suggestions.append("Maintain proof and justification for cash usage.")
        score_parts.append("High cash payment: +20")
        risk_score += 20
        ai_category = "Cash Flow"

    if voucher_data["type"] in ("Sales", "Purchase") and not voucher_data["narration"].strip():
        flags.append("Missing narration")
        explanations.append("Sales/Purchase voucher has no narration.")
        suggestions.append("Add meaningful narration for scrutiny readiness.")
        score_parts.append("Missing narration: +6")
        risk_score += 6

    if voucher_data["type"] in ("Sales", "Purchase") and not voucher_data["party_name"].strip():
        flags.append("Missing party name")
        explanations.append("Sales/Purchase voucher has no party name.")
        suggestions.append("Add customer/vendor name.")
        score_parts.append("Missing party name: +10")
        risk_score += 10
        ai_category = "Missing Data"

    if voucher_data["type"] in ("Sales", "Purchase") and not voucher_data["invoice_number"].strip():
        flags.append("Missing invoice number")
        explanations.append("Sales/Purchase voucher has no invoice number.")
        suggestions.append("Add invoice reference number.")
        score_parts.append("Missing invoice number: +12")
        risk_score += 12
        ai_category = "Missing Data"

    if voucher_data["type"] in ("Sales", "Purchase"):
        if voucher_data["tax_amount"] < 0:
            flags.append("Negative tax amount")
            explanations.append("Tax amount cannot be negative.")
            suggestions.append("Correct GST value.")
            score_parts.append("Negative tax amount: +20")
            risk_score += 20
            ai_category = "GST"

        elif total_amount > 0 and voucher_data["tax_amount"] == 0:
            flags.append("Missing GST amount")
            explanations.append("Sales/Purchase voucher has zero tax amount.")
            suggestions.append("Verify whether GST applies.")
            score_parts.append("Missing GST amount: +10")
            risk_score += 10
            ai_category = "GST"

        elif total_amount > 0 and voucher_data["tax_amount"] > total_amount * 0.40:
            flags.append("Abnormal GST amount")
            explanations.append("Tax amount is unusually high compared to voucher value.")
            suggestions.append("Check GST rate and taxable amount.")
            score_parts.append("Abnormal GST amount: +20")
            risk_score += 20
            ai_category = "GST"

    if has_missing_gstin:
        flags.append("Missing GST number on GST-applicable ledger")
        explanations.append("A GST-applicable ledger linked to this voucher has no GST number.")
        suggestions.append("Update ledger GSTIN before tax reporting.")
        score_parts.append("Missing GSTIN: +18")
        risk_score += 18
        ai_category = "GST"

    if len(entries) >= 4:
        flags.append("Complex voucher structure")
        explanations.append("Voucher has many lines and should be manually checked.")
        suggestions.append("Review ledger allocations.")
        score_parts.append("Complex voucher structure: +5")
        risk_score += 5

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
        "total_amount": total_amount
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
                SUM(CASE WHEN ai_risk_level = 'High' THEN 1 ELSE 0 END) AS high_count,
                SUM(CASE WHEN ai_risk_level = 'Medium' THEN 1 ELSE 0 END) AS medium_count,
                SUM(CASE WHEN ai_risk_level = 'Low' THEN 1 ELSE 0 END) AS low_count,
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
            SELECT COUNT(*) FROM (
                SELECT invoice_number
                FROM vouchers
                WHERE company_id = ? AND invoice_number IS NOT NULL AND TRIM(invoice_number) <> ''
                GROUP BY invoice_number, type
                HAVING COUNT(*) > 1
            )
        """, (company_id,)).fetchone()[0]

        high_cash_entries = conn.execute("""
            SELECT COUNT(*)
            FROM vouchers
            WHERE company_id = ?
              AND ai_flags LIKE '%High cash%'
        """, (company_id,)).fetchone()[0]

        missing_gstin_count = conn.execute("""
            SELECT COUNT(*)
            FROM vouchers
            WHERE company_id = ?
              AND ai_flags LIKE '%Missing GST number%'
        """, (company_id,)).fetchone()[0]

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
                return match.group(1).strip()
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

    date = find_first([
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
        return float(value.replace(",", "").strip())

    base_amount = parse_amount(base_amount_raw)
    gst_rate = float(gst_rate_raw) if gst_rate_raw else 0.0

    return {
        "invoice_number": invoice_number,
        "party_name": party_name,
        "product_name": product_name,
        "date": date,
        "base_amount": base_amount,
        "hsn_code": hsn_code,
        "gst_rate": gst_rate,
        "payment_mode": payment_mode.strip(),
        "raw_text": text.strip(),
    }


def build_context(request: Request, screen="dashboard", selected_voucher_type="", scanner_result=None):
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
        "scanner_result": scanner_result
    }


@app.get("/", response_class=HTMLResponse)
def home(request: Request, screen: str = "dashboard", type: str = ""):
    return templates.TemplateResponse(
        "index.html",
        build_context(request, screen=screen, selected_voucher_type=type)
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
            name.strip(), mailing_name.strip(), address.strip(), state.strip(),
            country.strip(), phone.strip(), email.strip(),
            financial_year_start, books_from, currency.strip() or "₹",
            maintain_inventory, enable_gst
        ))
        company_id = cur.lastrowid

    response = RedirectResponse(url="/?screen=company", status_code=303)
    if not request.cookies.get("active_company_id"):
        response.set_cookie("active_company_id", str(company_id))
    return response


@app.get("/company/select/{company_id}")
def select_company(company_id: int):
    response = RedirectResponse(url="/?screen=dashboard", status_code=303)
    response.set_cookie("active_company_id", str(company_id))
    return response


@app.get("/company/delete/{company_id}")
def delete_company_route(request: Request, company_id: int):
    with get_conn() as conn:
        voucher_ids = conn.execute(
            "SELECT id FROM vouchers WHERE company_id = ?",
            (company_id,)
        ).fetchall()

        for row in voucher_ids:
            conn.execute("DELETE FROM voucher_entries WHERE voucher_id = ?", (row["id"],))
            conn.execute("DELETE FROM ai_transactions WHERE voucher_id = ?", (row["id"],))

        conn.execute("DELETE FROM vouchers WHERE company_id = ?", (company_id,))
        conn.execute("DELETE FROM ledgers WHERE company_id = ?", (company_id,))
        conn.execute("DELETE FROM companies WHERE id = ?", (company_id,))

    response = RedirectResponse(url="/?screen=company", status_code=303)
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
        return RedirectResponse(url="/?screen=ledger", status_code=303)

    with get_conn() as conn:
        existing = conn.execute("""
            SELECT COUNT(*)
            FROM ledgers
            WHERE company_id = ? AND LOWER(ledger_name) = LOWER(?)
        """, (active_company["id"], ledger_name.strip())).fetchone()[0]

        if existing == 0:
            conn.execute("""
                INSERT INTO ledgers (
                    company_id, ledger_name, group_name, opening_balance,
                    balance_type, gst_applicable, gst_number, address, phone, email
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                active_company["id"],
                ledger_name.strip(),
                group_name,
                opening_balance,
                balance_type,
                gst_applicable,
                gst_number.strip(),
                address.strip(),
                phone.strip(),
                email.strip()
            ))

    return RedirectResponse(url="/?screen=ledger", status_code=303)


@app.get("/ledger/delete/{ledger_id}")
def delete_ledger_route(ledger_id: int):
    with get_conn() as conn:
        conn.execute("DELETE FROM ledgers WHERE id = ?", (ledger_id,))
    return RedirectResponse(url="/?screen=ledger", status_code=303)


@app.post("/voucher")
async def create_voucher_route(request: Request):
    active_company = get_active_company(request)
    if not active_company:
        return RedirectResponse(url="/?screen=voucher", status_code=303)

    form = await request.form()

    voucher_number = str(form.get("voucherNumber", "")).strip()
    date = str(form.get("date", "")).strip()
    voucher_type = str(form.get("type", "")).strip()
    narration = str(form.get("narration", "")).strip()
    party_name = str(form.get("party_name", "")).strip()
    payment_mode = str(form.get("payment_mode", "")).strip()
    invoice_number = str(form.get("invoice_number", "")).strip()
    product_name = str(form.get("product_name", "")).strip()
    hsn_code = str(form.get("hsn_code", "")).strip()

    base_amount = float(form.get("base_amount", "0") or 0)
    gst_rate = float(form.get("gst_rate", "0") or 0)
    tax_amount = float(form.get("tax_amount", "0") or 0)
    total_amount = float(form.get("total_amount", "0") or 0)

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
        d = float(debit or 0)
        c = float(credit or 0)
        if d <= 0 and c <= 0:
            continue
        cleaned_entries.append({
            "ledger_id": int(ledger_id),
            "debit": d,
            "credit": c
        })

    if len(cleaned_entries) < 2:
        return RedirectResponse(url="/?screen=voucher", status_code=303)

    debit_total = sum(x["debit"] for x in cleaned_entries)
    credit_total = sum(x["credit"] for x in cleaned_entries)

    if round(debit_total, 2) != round(credit_total, 2):
        return RedirectResponse(url="/?screen=voucher", status_code=303)

    voucher_data = {
        "voucher_number": voucher_number,
        "date": date,
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
            date,
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
    return RedirectResponse(url="/?screen=voucher", status_code=303)


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
        return RedirectResponse(url="/?screen=scanner", status_code=303)

    debit_ledger, credit_ledger = pick_default_ledgers(active_company["id"], voucher_type)
    if not debit_ledger or not credit_ledger:
        return RedirectResponse(url="/?screen=scanner", status_code=303)

    voucher_number = f"SCAN-{abs(hash((invoice_number, party_name, date))) % 1000000}"
    narration = f"Auto-created from invoice scanner for {product_name}".strip()

    cleaned_entries = [
        {
            "ledger_id": int(debit_ledger["id"]),
            "debit": float(total_amount),
            "credit": 0.0
        },
        {
            "ledger_id": int(credit_ledger["id"]),
            "debit": 0.0,
            "credit": float(total_amount)
        }
    ]

    voucher_data = {
        "voucher_number": voucher_number,
        "date": date or "2026-03-09",
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
            date or "2026-03-09",
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
    return RedirectResponse(url="/?screen=voucher", status_code=303)


@app.get("/voucher/delete/{voucher_id}")
def delete_voucher_route(voucher_id: int):
    with get_conn() as conn:
        conn.execute("DELETE FROM voucher_entries WHERE voucher_id = ?", (voucher_id,))
        conn.execute("DELETE FROM ai_transactions WHERE voucher_id = ?", (voucher_id,))
        conn.execute("DELETE FROM vouchers WHERE id = ?", (voucher_id,))
    return RedirectResponse(url="/?screen=voucher", status_code=303)
