import sqlite3
from contextlib import contextmanager

DB_NAME = "tally.db"


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def execute(query: str, params: tuple = ()):
    with get_conn() as conn:
        cur = conn.execute(query, params)
        return cur


def fetchone(query: str, params: tuple = ()):
    with get_conn() as conn:
        return conn.execute(query, params).fetchone()


def fetchall(query: str, params: tuple = ()):
    with get_conn() as conn:
        return conn.execute(query, params).fetchall()


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

        seed_hsn_master(conn)


def seed_hsn_master(conn):
    count = conn.execute("SELECT COUNT(*) FROM hsn_master").fetchone()[0]
    if count > 0:
        return

    rows = [
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
        rows
    )
