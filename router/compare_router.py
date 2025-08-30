# router/compare_router.py
from fastapi import APIRouter, Depends,Query, HTTPException
from pydantic import BaseModel
from db.db import get_connection
from utils.jwt_utils import get_current_user
from models.token_data import TokenData
from datetime import date
import psycopg2.extras

router = APIRouter(prefix="/compare", tags=["Comparison"])


# ---------- Request Model (shared for all) ---------- #
class CompareRequest(BaseModel):
    start1: date
    end1: date
    start2: date
    end2: date


# ---------------- GSC ---------------- #
def fetch_gsc_data(conn, tenant_id, table, dim_col, start_date, end_date):
    """
    Fetch GSC data for a table and date range.
    Returns list of dicts.
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    dims = f"{dim_col}," if dim_col else ""
    group_by = f"GROUP BY {dims} date"

    cursor.execute(f"""
        SELECT 
            {dims} date,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            AVG(ctr) AS ctr,
            AVG(position) AS position
        FROM {table}
        WHERE tenant_id=%s AND date BETWEEN %s AND %s
        {group_by}
        ORDER BY date
    """, (tenant_id, start_date, end_date))

    rows = cursor.fetchall()
    cursor.close()
    return rows


def calculate_percentage_change(val1, val2):
    if val1 is None or val2 is None:
        return None
    if val1 == 0:
        return None
    return ((val2 - val1) / val1) * 100


def build_gsc_comparison(range1_data, range2_data, dim_col=None):
    """
    Calculates percentage changes and daily comparison per row.
    Returns a dict with percentage_changes and daily_comparison array.
    """
    # Aggregate total metrics for range1 and range2
    totals1 = {"clicks": 0, "impressions": 0, "ctr": 0, "position": 0}
    totals2 = {"clicks": 0, "impressions": 0, "ctr": 0, "position": 0}

    daily_comparison = []

    # Helper: index rows by dimension + date for alignment
    def index_rows(rows):
        idx = {}
        for r in rows:
            key = (r.get(dim_col) if dim_col else "summary", r["date"])
            idx[key] = r
        return idx

    r1_index = index_rows(range1_data)
    r2_index = index_rows(range2_data)

    all_keys = set(r1_index.keys()) | set(r2_index.keys())

    for key in sorted(all_keys):
        row1 = r1_index.get(key, {})
        row2 = r2_index.get(key, {})

        daily_row = {
            "date": row1.get("date") or row2.get("date"),
        }
        if dim_col:
            daily_row[dim_col] = row1.get(dim_col) or row2.get(dim_col)

        for metric in ["clicks", "impressions", "ctr", "position"]:
            val1 = row1.get(metric, 0) or 0
            val2 = row2.get(metric, 0) or 0
            daily_row[metric] = {
                "range1": val1,
                "range2": val2,
                "diff": val2 - val1,
                "pct_change": calculate_percentage_change(val1, val2),
            }
            totals1[metric] += val1
            totals2[metric] += val2

        daily_comparison.append(daily_row)

    # Overall percentage changes
    percentage_changes = {
        metric: calculate_percentage_change(totals1[metric], totals2[metric])
        for metric in totals1
    }

    return {
        "percentage_changes": percentage_changes,
        "daily_comparison": daily_comparison
    }


@router.post("/gsc")
def compare_gsc(req: CompareRequest, current_user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    try:
        tenant_id = current_user.tenant_id

        tables = {
            "summary": ("gsc_summary_daily", None),
            "queries": ("gsc_queries_daily", "query"),
            "pages": ("gsc_pages_daily", "page"),
            "countries": ("gsc_countries_daily", "country"),
            "devices": ("gsc_devices_daily", "device"),
        }

        result = {}

        for name, (table, dim_col) in tables.items():
            range1 = fetch_gsc_data(conn, tenant_id, table, dim_col, req.start1, req.end1)
            range2 = fetch_gsc_data(conn, tenant_id, table, dim_col, req.start2, req.end2)

            comparison = build_gsc_comparison(range1, range2, dim_col)

            result[name] = {
                "range1": {"start": req.start1, "end": req.end1, "data": range1},
                "range2": {"start": req.start2, "end": req.end2, "data": range2},
                "percentage_changes": comparison["percentage_changes"],
                "daily_comparison": comparison["daily_comparison"],
            }

        return {"platform": "gsc", "comparison": result}
    finally:
        conn.close()

# ---------------- GA4 ---------------- #
def fetch_ga4_data(conn, tenant_id, start_date, end_date, table_name):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if table_name == "ga4_top_pages_daily":
            cur.execute(
                """
                SELECT
    date,
    page_path,
    SUM(views) AS total_views,
    SUM(active_users) AS total_active_users,
    AVG(views_per_user) AS avg_views_per_user,
    AVG(avg_engagement_time) AS avg_engagement_time,
    SUM(event_count) AS total_event_count,
    AVG(bounce_rate) AS avg_bounce_rate,
    AVG(engagement_rate) AS avg_engagement_rate
FROM ga4_top_pages_daily
WHERE tenant_id = %s
  AND date BETWEEN %s AND %s
GROUP BY date, page_path
ORDER BY date, total_views DESC

                """,
                (tenant_id, start_date, end_date),
            )
            return cur.fetchall()

        elif table_name == "ga4_traffic_acquisition_daily":
            cur.execute(
                """
                SELECT
                    date,
                    SUM(sessions) AS total_sessions,
                    SUM(engaged_sessions) AS total_engaged_sessions,
                    AVG(engagement_rate) AS avg_engagement_rate,
                    AVG(avg_engagement_time) AS avg_engagement_time,
                    AVG(events_per_session) AS avg_events_per_session,
                    SUM(total_events) AS total_events
                FROM ga4_traffic_acquisition_daily
                WHERE tenant_id = %s
                  AND date BETWEEN %s AND %s
                GROUP BY date
                ORDER BY date ASC
                """,
                (tenant_id, start_date, end_date),
            )
            return cur.fetchall()

        elif table_name == "ga4_country_metrics_daily":
            cur.execute(
                """
                SELECT
                    date,
                    country,
                    SUM(active_users) AS total_active_users,
                    SUM(new_users) AS total_new_users,
                    SUM(engaged_sessions) AS total_engaged_sessions,
                    AVG(engaged_sessions_per_user) AS avg_engaged_sessions_per_user,
                    AVG(engagement_rate) AS avg_engagement_rate,
                    AVG(avg_engagement_time) AS avg_engagement_time,
                    SUM(event_count) AS total_event_count
                FROM ga4_country_metrics_daily
                WHERE tenant_id = %s
                  AND date BETWEEN %s AND %s
                GROUP BY date, country
                ORDER BY date ASC
                """,
                (tenant_id, start_date, end_date),
            )
            return cur.fetchall()

        elif table_name == "ga4_browser_metrics_daily":
            cur.execute(
                """
                SELECT
                    date,
                    browser,
                    SUM(active_users) AS total_active_users,
                    SUM(new_users) AS total_new_users,
                    SUM(engaged_sessions) AS total_engaged_sessions,
                    AVG(engaged_sessions_per_user) AS avg_engaged_sessions_per_user,
                    AVG(engagement_rate) AS avg_engagement_rate,
                    AVG(avg_engagement_time) AS avg_engagement_time,
                    SUM(event_count) AS total_event_count
                FROM ga4_browser_metrics_daily
                WHERE tenant_id = %s
                  AND date BETWEEN %s AND %s
                GROUP BY date, browser
                ORDER BY date ASC
                """,
                (tenant_id, start_date, end_date),
        )
        return cur.fetchall()

    return {}


def calculate_percentage_changes(range1_data, range2_data):
    changes = {}
    if isinstance(range1_data, dict) and isinstance(range2_data, dict):
        for key, val1 in range1_data.items():
            val2 = range2_data.get(key)
            if (
                isinstance(val1, (int, float))
                and isinstance(val2, (int, float))
                and val1 != 0
            ):
                changes[key] = ((val2 - val1) / val1) * 100
    return changes


@router.post("/ga4")
def compare_ga4(
    req: CompareRequest,
    current_user: TokenData = Depends(get_current_user),
):
    conn = get_connection()
    try:
        tables = [
            "ga4_top_pages_daily",
            "ga4_traffic_acquisition_daily",
            "ga4_country_metrics_daily",
            "ga4_browser_metrics_daily",
        ]
        result = {}
        for table in tables:
            range1 = fetch_ga4_data(conn, current_user.tenant_id, req.start1, req.end1, table)
            range2 = fetch_ga4_data(conn, current_user.tenant_id, req.start2, req.end2, table)

            if isinstance(range1, list) and isinstance(range2, list):
                r1 = range1[0] if range1 else {}
                r2 = range2[0] if range2 else {}
                changes = calculate_percentage_changes(r1, r2)
            else:
                changes = calculate_percentage_changes(range1 or {}, range2 or {})

            result[table] = {
                "range1": {"start": req.start1, "end": req.end1, "data": range1},
                "range2": {"start": req.start2, "end": req.end2, "data": range2},
                "percentage_changes": changes,
            }
        return result
    finally:
        conn.close()
# ---------------- Cloudflare ---------------- #
import psycopg2.extras

def fetch_cloudflare_summary(conn, tenant_id, start, end):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        query = """
        SELECT 
            SUM(page_views) AS total_page_views,
            SUM(visits) AS total_visits
        FROM cloudflare_summary_daily
        WHERE tenant_id = %s AND date BETWEEN %s AND %s
        """
        cur.execute(query, (tenant_id, start, end))
        return cur.fetchone()


@router.post("/cloudflare")
def compare_cloudflare(
    req: CompareRequest,
    current_user: TokenData = Depends(get_current_user),
):
    conn = get_connection()
    try:
        range1 = fetch_cloudflare_summary(conn, current_user.tenant_id, req.start1, req.end1)
        range2 = fetch_cloudflare_summary(conn, current_user.tenant_id, req.start2, req.end2)

        return {
            "range1": {"start": req.start1, "end": req.end1, **range1},
            "range2": {"start": req.start2, "end": req.end2, **range2},
        }
    finally:
        conn.close()
