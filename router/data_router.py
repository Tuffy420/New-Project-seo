# router/data_router.py
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from utils.jwt_utils import get_current_user
from db.db import get_connection
from models.token_data import TokenData
from io import StringIO
import csv 
import zipfile
import io

router = APIRouter(prefix="/data", tags=["Data Viewer"])


def parse_range_clause(range_val):
    """Helper to convert frontend range to SQL clause."""
    if not range_val:
        return ""
    if range_val == "today":
        return " AND date = CURRENT_DATE"
    try:
        days = int(range_val)
        return f" AND date >= CURRENT_DATE - INTERVAL '{days} day'"
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid range parameter")


@router.get("/gsc")
def get_gsc_data(range: str = None, start: str = None, end: str = None, user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    cur = conn.cursor()

    def fetch(table):
        query = f"SELECT * FROM {table} WHERE tenant_id = %s"
        params = [user.tenant_id]

        if start and end:
            query += " AND date BETWEEN %s AND %s"
            params += [start, end]
        elif range:
            query += parse_range_clause(range)

        query += " ORDER BY date DESC LIMIT 100"
        cur.execute(query, tuple(params))
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

    result = {
        "summary": fetch("gsc_summary_daily"),
        "queries": fetch("gsc_queries_daily"),
        "pages": fetch("gsc_pages_daily"),
        "countries": fetch("gsc_countries_daily"),
        "devices": fetch("gsc_devices_daily"),
    }

    cur.close()
    conn.close()
    return result


@router.get("/ga4")
def get_ga4_data(range: str = None, start: str = None, end: str = None, user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    cur = conn.cursor()

    def fetch(table):
        query = f"SELECT * FROM {table} WHERE tenant_id = %s"
        params = [user.tenant_id]

        if start and end:
            query += " AND date BETWEEN %s AND %s"
            params += [start, end]
        elif range:
            query += parse_range_clause(range)

        query += " ORDER BY date DESC LIMIT 100"
        cur.execute(query, tuple(params))
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

    result = {
        "top_pages": fetch("ga4_top_pages_daily"),
        "traffic": fetch("ga4_traffic_acquisition_daily"),
        "countries": fetch("ga4_country_metrics_daily"),
        "browsers": fetch("ga4_browser_metrics_daily"),
    }

    cur.close()
    conn.close()
    return result


@router.get("/cloudflare")
def get_cf_data(range: str = None, start: str = None, end: str = None, user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    cur = conn.cursor()

    query = "SELECT * FROM cloudflare_summary_daily WHERE tenant_id = %s"
    params = [user.tenant_id]

    if start and end:
        query += " AND date BETWEEN %s AND %s"
        params += [start, end]
    elif range:
        if range == "today":
            query += " AND date = CURRENT_DATE"
        else:
            try:
                days = int(range)
                query += f" AND date >= CURRENT_DATE - INTERVAL '{days} day'"
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid range parameter")

    query += " ORDER BY date DESC LIMIT 100"
    cur.execute(query, tuple(params))

    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

# --- Helper function to generate CSV ---
def generate_csv(rows, headers):
    csv_file = StringIO()
    writer = csv.writer(csv_file)
    writer.writerow(headers)
    writer.writerows(rows)
    csv_file.seek(0)
    return csv_file

# --- GSC CSV Export ---
# --- GSC CSV Export ---
@router.get("/gsc/summary/export")
def export_gsc_data(user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM gsc_summary_daily
                WHERE tenant_id = %s
                ORDER BY date DESC
            """, [user.tenant_id])
            rows = cur.fetchall()
            headers = [desc[0] for desc in cur.description]
            csv_file = generate_csv(rows, headers)
    finally:
        conn.close()

    return StreamingResponse(
        csv_file,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=gsc_data.csv"}
    )
# --- GSC Queries ---
@router.get("/gsc/queries/export")
def export_gsc_queries(user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM gsc_queries_daily
                WHERE tenant_id = %s
                ORDER BY date DESC
            """, [user.tenant_id])
            rows = cur.fetchall()
            headers = [desc[0] for desc in cur.description]
            csv_file = generate_csv(rows, headers)
    finally:
        conn.close()

    return StreamingResponse(
        csv_file,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=gsc_queries.csv"}
    )
# --- GSC Pages ---
@router.get("/gsc/pages/export")
def export_gsc_pages(user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM gsc_pages_daily
                WHERE tenant_id = %s
                ORDER BY date DESC
            """, [user.tenant_id])
            rows = cur.fetchall()
            headers = [desc[0] for desc in cur.description]
            csv_file = generate_csv(rows, headers)
    finally:
        conn.close()

    return StreamingResponse(
        csv_file,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=gsc_pages.csv"}
    )

# --- GSC Devices ---
@router.get("/gsc/devices/export")
def export_gsc_devices(user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM gsc_devices_daily
                WHERE tenant_id = %s
                ORDER BY date DESC
            """, [user.tenant_id])
            rows = cur.fetchall()
            headers = [desc[0] for desc in cur.description]
            csv_file = generate_csv(rows, headers)
    finally:
        conn.close()

    return StreamingResponse(
        csv_file,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=gsc_devices.csv"}
    )

# --- GSC Countries ---
@router.get("/gsc/countries/export")
def export_gsc_countries(user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM gsc_countries_daily
                WHERE tenant_id = %s
                ORDER BY date DESC
            """, [user.tenant_id])
            rows = cur.fetchall()
            headers = [desc[0] for desc in cur.description]
            csv_file = generate_csv(rows, headers)
    finally:
        conn.close()

    return StreamingResponse(
        csv_file,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=gsc_countries.csv"}
    )


# --- GA4 CSV Export ---
@router.get("/ga4/top_pages/export")
def export_ga4_top_pages(user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM ga4_top_pages_daily
            WHERE tenant_id = %s
            ORDER BY views DESC
        """, [user.tenant_id])
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]
        csv_file = generate_csv(rows, headers)
    finally:
        cur.close()
        conn.close()

    return StreamingResponse(
        csv_file,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=ga4_top_pages.csv"}
    )

# --- GA4 Traffic Acquisition CSV Export ---
@router.get("/ga4/traffic/export")
def export_ga4_traffic(user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM ga4_traffic_acquisition_daily
            WHERE tenant_id = %s
            ORDER BY date DESC
        """, [user.tenant_id])
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]
        csv_file = generate_csv(rows, headers)
    finally:
        cur.close()
        conn.close()

    return StreamingResponse(
        csv_file,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=ga4_traffic.csv"}
    )

# --- GA4 Countries CSV Export ---
@router.get("/ga4/countries/export")
def export_ga4_countries(user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM ga4_country_metrics_daily
            WHERE tenant_id = %s
            ORDER BY date DESC
        """, [user.tenant_id])
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]
        csv_file = generate_csv(rows, headers)
    finally:
        cur.close()
        conn.close()

    return StreamingResponse(
        csv_file,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=ga4_countries.csv"}
    )

# --- GA4 Browsers CSV Export ---
@router.get("/ga4/browsers/export")
def export_ga4_browsers(user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM ga4_browser_metrics_daily
            WHERE tenant_id = %s
            ORDER BY date DESC
        """, [user.tenant_id])
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]
        csv_file = generate_csv(rows, headers)
    finally:
        cur.close()
        conn.close()

    return StreamingResponse(
        csv_file,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=ga4_browsers.csv"}
    )

# --- Cloudflare CSV Export ---
@router.get("/cloudflare/export")
def export_cloudflare_data(user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM cloudflare_summary_daily
                WHERE tenant_id = %s
                ORDER BY date DESC
            """, [user.tenant_id])
            rows = cur.fetchall()
            headers = [desc[0] for desc in cur.description]
            csv_file = generate_csv(rows, headers)
    finally:
        conn.close()

    return StreamingResponse(
        csv_file,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=cloudflare_data.csv"}
    )

@router.get("/export/all")
def export_all(user: TokenData = Depends(get_current_user)):
    conn = get_connection()
    cur = conn.cursor()

    # List of tables to export
    tables = {
        "gsc_summary_daily": "SELECT * FROM gsc_summary_daily WHERE tenant_id = %s",
        "gsc_queries_daily": "SELECT * FROM gsc_queries_daily WHERE tenant_id = %s",
        "gsc_pages_daily": "SELECT * FROM gsc_pages_daily WHERE tenant_id = %s",
        "gsc_countries_daily": "SELECT * FROM gsc_countries_daily WHERE tenant_id = %s",
        "gsc_devices_daily": "SELECT * FROM gsc_devices_daily WHERE tenant_id = %s",
        "ga4_traffic_acquisition_daily": "SELECT * FROM ga4_traffic_acquisition_daily WHERE tenant_id = %s",
        "ga4_top_pages_daily": "SELECT * FROM ga4_top_pages_daily WHERE tenant_id = %s",
        "ga4_country_metrics_daily": "SELECT * FROM ga4_country_metrics_daily WHERE tenant_id = %s",
        "ga4_browser_metrics_daily": "SELECT * FROM ga4_browser_metrics_daily WHERE tenant_id = %s",
        "cloudflare_summary_daily": "SELECT * FROM cloudflare_summary_daily WHERE tenant_id = %s"
    }

    # In-memory zip
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for table_name, query in tables.items():
            cur.execute(query, (user.tenant_id,))
            rows = cur.fetchall()
            headers = [desc[0] for desc in cur.description]

            # Create CSV in memory
            csv_buffer = io.StringIO()
            writer = csv.writer(csv_buffer)
            writer.writerow(headers)
            writer.writerows(rows)

            # Add CSV file to zip
            zip_file.writestr(f"{table_name}.csv", csv_buffer.getvalue())

    conn.close()

    zip_buffer.seek(0)
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=all_data_export.zip"}
    )