import sqlite3

DB_PATH = "/tmp/database.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            mailing_name TEXT,
            address TEXT,
            state TEXT,
            country TEXT,
            phone TEXT,
            email TEXT,
            financial_year_start TEXT,
            books_from TEXT,
            currency TEXT,
            maintain_inventory TEXT,
            enable_gst TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ledgers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            type TEXT
        )
    """)

    conn.commit()
    conn.close()

def fetchall(query, params=()):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def execute(query, params=()):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(query, params)
    conn.commit()
    conn.close()
