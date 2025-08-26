# db/db.py
import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from utils.gsc_utils import fetch_gsc_data
from datetime import date, timedelta
import uuid,json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from typing import Optional,List

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

Base = declarative_base()
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_connection():
    return psycopg2.connect(DATABASE_URL)


def insert_gsc_summary_daily(rows):
    query = """
        INSERT INTO gsc_summary_daily 
            (date, clicks, impressions, ctr, position, tenant_id, session_id)
        VALUES %s
        ON CONFLICT (session_id) DO NOTHING
    """
    _insert_bulk(query, rows)


def insert_gsc_queries_daily(rows):
    query = """
        INSERT INTO gsc_queries_daily 
            (date, query, clicks, impressions, ctr, position, tenant_id, session_id)
        VALUES %s
        ON CONFLICT (session_id) DO NOTHING
    """
    _insert_bulk(query, rows)


def insert_gsc_pages_daily(rows):
    query = """
        INSERT INTO gsc_pages_daily 
            (date, page, clicks, impressions, ctr, position, tenant_id, session_id)
        VALUES %s
        ON CONFLICT (session_id) DO NOTHING
    """
    _insert_bulk(query, rows)


def insert_gsc_countries_daily(rows):
    query = """
        INSERT INTO gsc_countries_daily 
            (date, country, clicks, impressions, ctr, position, tenant_id, session_id)
        VALUES %s
        ON CONFLICT (session_id) DO NOTHING
    """
    _insert_bulk(query, rows)


def insert_gsc_devices_daily(rows):
    query = """
        INSERT INTO gsc_devices_daily 
            (date, device, clicks, impressions, ctr, position, tenant_id, session_id)
        VALUES %s
        ON CONFLICT (session_id) DO NOTHING
    """
    _insert_bulk(query, rows)


def insert_ga4_top_pages_daily(rows):
    query = """
        INSERT INTO ga4_top_pages_daily 
            (tenant_id, session_id, page_path, views, active_users, views_per_user,
             avg_engagement_time, event_count, bounce_rate, engagement_rate, date)
        VALUES %s
        ON CONFLICT (session_id) DO NOTHING
    """
    columns = [
        "tenant_id", "session_id", "page_path", "views", "active_users", "views_per_user",
        "avg_engagement_time", "event_count", "bounce_rate", "engagement_rate", "date"
    ]
    filtered_rows = [{col: row.get(col) for col in columns} for row in rows]
    _insert_bulk(query, filtered_rows, columns)


def insert_ga4_traffic_acquisition_daily(rows):
    query = """
        INSERT INTO ga4_traffic_acquisition_daily 
            (tenant_id, session_id, source_medium, sessions, engaged_sessions,
             engagement_rate, avg_engagement_time, events_per_session, total_events, date)
        VALUES %s
        ON CONFLICT (session_id) DO NOTHING
    """
    columns = [
        "tenant_id", "session_id", "source_medium", "sessions", "engaged_sessions",
        "engagement_rate", "avg_engagement_time", "events_per_session", "total_events", "date"
    ]
    filtered_rows = [{col: row.get(col) for col in columns} for row in rows]
    _insert_bulk(query, filtered_rows, columns)


def insert_ga4_country_metrics_daily(rows):
    query = """
        INSERT INTO ga4_country_metrics_daily 
            (tenant_id, session_id, country, active_users, new_users, engaged_sessions,
             engaged_sessions_per_user, engagement_rate, avg_engagement_time,
             event_count, date)
        VALUES %s
        ON CONFLICT (session_id) DO NOTHING
    """
    columns = [
        "tenant_id", "session_id", "country", "active_users", "new_users",
        "engaged_sessions", "engaged_sessions_per_user", "engagement_rate",
        "avg_engagement_time", "event_count", "date"
    ]
    filtered_rows = [{col: row.get(col) for col in columns} for row in rows]
    _insert_bulk(query, filtered_rows, columns)


def insert_ga4_browser_metrics_daily(rows):
    query = """
        INSERT INTO ga4_browser_metrics_daily 
            (tenant_id, session_id, browser, active_users, new_users, engaged_sessions,
             engaged_sessions_per_user, engagement_rate, avg_engagement_time,
             event_count, date)
        VALUES %s
        ON CONFLICT (session_id) DO NOTHING
    """
    columns = [
        "tenant_id", "session_id", "browser", "active_users", "new_users",
        "engaged_sessions", "engaged_sessions_per_user", "engagement_rate",
        "avg_engagement_time", "event_count", "date"
    ]
    filtered_rows = [{col: row.get(col) for col in columns} for row in rows]
    _insert_bulk(query, filtered_rows, columns)

def insert_cloudflare_summary(tenant_id: str, session_id: str, date: str, page_views: int, visits: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO cloudflare_summary_daily (tenant_id, session_id, date, page_views, visits)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (tenant_id, session_id, date, page_views, visits))
    conn.commit()
    cur.close()
    conn.close()


def get_or_create_tenant(tenant_id: str):
    query = """
        INSERT INTO tenants (tenant_id)
        VALUES (%s)
        ON CONFLICT (tenant_id) DO NOTHING
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (tenant_id,))
        conn.commit()


# Alias to match import in main.py
ensure_tenant_exists = get_or_create_tenant


def _insert_bulk(query, rows, columns=None):
    if not rows:
        print("⚠️ No rows to insert for:", query.split()[2])
        return
    if columns:
        values = [tuple(row.get(col) for col in columns) for row in rows]
    else:
        values = [tuple(row.values()) for row in rows]
    print(f"✅ Inserting {len(values)} rows into table: {query.split()[2]}")
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, values)
        conn.commit()


def insert_rows(table_name, rows):
    if not rows:
        print(f"⚠️ No rows to insert for: {table_name}")
        return

    keys = rows[0].keys()
    columns = ', '.join(keys)
    placeholders = ', '.join(['%s'] * len(keys))

    insert_query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    values = [tuple(row[key] for key in keys) for row in rows]

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, values)
        conn.commit()



def insert_alert_event(tenant_id: str, alert_type: str, message: str | None):
    conn = get_connection()  
    cursor = conn.cursor()

    query = """
        INSERT INTO alert_events (tenant_id, alert_type, alert_data)
        VALUES (%s, %s, %s)
    """
    cursor.execute(query, (tenant_id, alert_type, json.dumps({"message": message})))
    
    conn.commit()         
    cursor.close()
    conn.close()


def setup_tables():
    commands = [
        """
        CREATE TABLE IF NOT EXISTS tenants (
            id SERIAL PRIMARY KEY,
            tenant_id TEXT UNIQUE NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            tenant_id UUID NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS alert_events (
                id SERIAL PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                alert_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS tenant_credentials (
            id SERIAL PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            service TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS gsc_summary_daily (
            id SERIAL PRIMARY KEY, 
            clicks INT,
            impressions INT,
            ctr FLOAT,
            position FLOAT,
            tenant_id TEXT,
            session_id TEXT UNIQUE,
            date DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS gsc_queries_daily (
            id SERIAL PRIMARY KEY,
            query TEXT,
            clicks INT,
            impressions INT,
            ctr FLOAT,
            position FLOAT,
            tenant_id TEXT,
            session_id TEXT UNIQUE,
            date DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS gsc_pages_daily (
            id SERIAL PRIMARY KEY,
            page TEXT,
            clicks INT,
            impressions INT,
            ctr FLOAT,
            position FLOAT,
            tenant_id TEXT,
            session_id TEXT UNIQUE,
            date DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS gsc_countries_daily (
            id SERIAL PRIMARY KEY,
            country TEXT,
            clicks INT,
            impressions INT,
            ctr FLOAT,
            position FLOAT,
            tenant_id TEXT,
            session_id TEXT UNIQUE,
            date DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS gsc_devices_daily (
            id SERIAL PRIMARY KEY,
            device TEXT,
            clicks INT,
            impressions INT,
            ctr FLOAT,
            position FLOAT,
            tenant_id TEXT,
            session_id TEXT UNIQUE,
            date DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ga4_top_pages_daily (
            id SERIAL PRIMARY KEY,
            tenant_id TEXT,
            session_id TEXT UNIQUE,
            page_path TEXT,
            views INT,
            active_users INT,
            views_per_user FLOAT,
            avg_engagement_time FLOAT,
            event_count INT,
            bounce_rate FLOAT,
            engagement_rate FLOAT,
            date DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ga4_traffic_acquisition_daily (
            id SERIAL PRIMARY KEY,
            tenant_id TEXT,
            session_id TEXT UNIQUE,
            source_medium TEXT,
            sessions INT,
            engaged_sessions INT,
            engagement_rate FLOAT,
            avg_engagement_time FLOAT,
            events_per_session FLOAT,
            total_events INT,
            date DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ga4_country_metrics_daily (
            id SERIAL PRIMARY KEY,
            tenant_id TEXT,
            session_id TEXT UNIQUE,
            country TEXT,
            active_users INT,
            new_users INT,
            engaged_sessions INT,
            engaged_sessions_per_user FLOAT,
            engagement_rate FLOAT,
            avg_engagement_time FLOAT,
            event_count INT,
            date DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ga4_browser_metrics_daily (
            id SERIAL PRIMARY KEY,
            tenant_id TEXT,
            session_id TEXT UNIQUE,
            browser TEXT,
            active_users INT,
            new_users INT,
            engaged_sessions INT,
            engaged_sessions_per_user FLOAT,
            engagement_rate FLOAT,
            avg_engagement_time FLOAT,
            event_count INT,
            date DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS cloudflare_summary_daily (
            id SERIAL PRIMARY KEY,
            tenant_id TEXT,
            session_id TEXT,
            date DATE,
            page_views INTEGER,
            visits INTEGER
        );
        """
    ]
    conn = get_connection()
    cur = conn.cursor()
    for command in commands:
        cur.execute(command)
    conn.commit()
    cur.close()
    conn.close()


def fetch_table(table: str) -> Optional[list]:
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table}")
                return cursor.fetchall()
    except Exception as e:
        print(f"❌ Error fetching data from {table}: {e}")
        return None


def run_gsc_fetch_for_tenant(tenant_id: str):
    fetch_date = date.today() - timedelta(days=1)
    session_id = str(uuid.uuid4())
    print(f"🔄 Running GSC fetch for tenant: {tenant_id} | date: {fetch_date}")

    get_or_create_tenant(tenant_id)
    gsc_data = fetch_gsc_data(tenant_id, fetch_date, fetch_date, session_id)

    insert_gsc_summary_daily(gsc_data["summary"])
    insert_gsc_queries_daily(gsc_data["queries"])
    insert_gsc_pages_daily(gsc_data["pages"])
    insert_gsc_countries_daily(gsc_data["countries"])
    insert_gsc_devices_daily(gsc_data["devices"])
    print("✅ GSC data fetched and stored successfully.")





def get_table_data(table: str) -> List[dict]:
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table}")
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        print(f"❌ Error reading {table}: {e}")
        return []

def get_tenant_credentials(tenant_id: str, service: str) -> dict:
    query = """
        SELECT key, value FROM tenant_credentials
        WHERE tenant_id = %s AND service = %s
    """
    creds = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (tenant_id, service))
            rows = cur.fetchall()
            for key, value in rows:
                creds[key] = value
    return creds

def fetch_all(table_name, tenant_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            query = f"SELECT * FROM {table_name} WHERE tenant_id = %s ORDER BY date DESC LIMIT 100"
            cur.execute(query, (tenant_id,))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result = [dict(zip(columns, row)) for row in rows]
            return result
