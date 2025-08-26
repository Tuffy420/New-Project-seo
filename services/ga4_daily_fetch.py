# services/ga4_daily_fetch.py
from datetime import date
from db.db import insert_rows, ensure_tenant_exists, get_connection
import uuid, json

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
from sqlalchemy.orm import Session
from services.credential_service import get_credentials_for_service


def run_report(client, property_id, dimensions, metrics, fetch_date):
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(start_date=str(fetch_date), end_date=str(fetch_date))]
    )
    return client.run_report(request)


def fetch_ga4_data(tenant_id, fetch_date, session_id):
    with get_connection() as conn:
        db = Session(bind=conn)
        credentials_dict = get_credentials_for_service(tenant_id, "ga4")
        db.close()

    if not credentials_dict:
        raise ValueError(f"❌ GA4 credentials not found for tenant {tenant_id}")

    # normalized keys from credential_service are uppercase/no-extra-chars
    property_id = credentials_dict.get("PROPERTY_ID") or credentials_dict.get("PROPERTYID")
    service_account_val = credentials_dict.get("SERVICE_ACCOUNT_JSON") or credentials_dict.get("SERVICEACCOUNTJSON")

    if not property_id:
        raise ValueError("❌ Missing GA4 property_id for tenant")

    if not service_account_val:
        raise ValueError("❌ Missing service account JSON for tenant")

    # Ensure service_creds is a dict (if stored as JSON string, it might already be parsed by credential_service)
    service_creds = service_account_val
    if isinstance(service_account_val, str):
        try:
            service_creds = json.loads(service_account_val)
        except Exception as e:
            # helpful error
            raise ValueError("❌ SERVICE_ACCOUNT_JSON is not valid JSON") from e

    if not isinstance(service_creds, dict):
        raise ValueError("❌ SERVICE_ACCOUNT_JSON did not parse to an object/dict")

    credentials = service_account.Credentials.from_service_account_info(service_creds)
    client = BetaAnalyticsDataClient(credentials=credentials)

    print(f"🔍 Using GA4 Property ID: {property_id}")
    def safe_report(dimensions, metrics):
        try:
            return run_report(client, property_id, dimensions, metrics, fetch_date).rows
        except Exception as e:
            print(f"❌ Error fetching {dimensions}: {e}")
            return []

    # --- Top Pages ---
    top_pages = []
    for row in safe_report(
        ["pagePath"],
        ["screenPageViews", "activeUsers", "bounceRate", "engagementRate", "averageSessionDuration", "eventCount"]
    ):
        views = int(row.metric_values[0].value)
        active_users = int(row.metric_values[1].value)
        top_pages.append({
            "tenant_id": tenant_id,
            "session_id": session_id,
            "page_path": row.dimension_values[0].value,
            "views": views,
            "active_users": active_users,
            "bounce_rate": float(row.metric_values[2].value),
            "engagement_rate": row.metric_values[3].value,
            "avg_engagement_time": row.metric_values[4].value,
            "event_count": int(row.metric_values[5].value),
            "views_per_user": round(views / max(active_users, 1), 2),
            "date": fetch_date
        })

    # --- Traffic Acquisition ---
    traffic_sources = []
    for row in safe_report(
        ["sessionSourceMedium"],
        ["sessions", "engagedSessions", "engagementRate", "averageSessionDuration", "eventsPerSession", "eventCount"]
    ):
        traffic_sources.append({
            "tenant_id": tenant_id,
            "session_id": session_id,
            "source_medium": row.dimension_values[0].value,
            "sessions": int(row.metric_values[0].value),
            "engaged_sessions": int(row.metric_values[1].value),
            "engagement_rate": row.metric_values[2].value,
            "avg_engagement_time": row.metric_values[3].value,
            "events_per_session": float(row.metric_values[4].value),
            "total_events": int(row.metric_values[5].value),
            "date": fetch_date
        })

    # --- Country Metrics ---
    country_metrics = []
    for row in safe_report(
        ["country"],
        ["activeUsers", "newUsers", "engagedSessions", "engagementRate", "averageSessionDuration", "eventCount"]
    ):
        active_users = int(row.metric_values[0].value)
        engaged_sessions = int(row.metric_values[2].value)
        country_metrics.append({
            "tenant_id": tenant_id,
            "session_id": session_id,
            "country": row.dimension_values[0].value,
            "active_users": active_users,
            "new_users": int(row.metric_values[1].value),
            "engaged_sessions": engaged_sessions,
            "engagement_rate": row.metric_values[3].value,
            "avg_engagement_time": row.metric_values[4].value,
            "event_count": int(row.metric_values[5].value),
            "engaged_sessions_per_user": round(engaged_sessions / max(active_users, 1), 2),
            "date": fetch_date
        })

    # --- Browser Metrics ---
    browser_metrics = []
    for row in safe_report(
        ["browser"],
        ["activeUsers", "newUsers", "engagedSessions", "engagementRate", "averageSessionDuration", "eventCount"]
    ):
        active_users = int(row.metric_values[0].value)
        engaged_sessions = int(row.metric_values[2].value)
        browser_metrics.append({
            "tenant_id": tenant_id,
            "session_id": session_id,
            "browser": row.dimension_values[0].value,
            "active_users": active_users,
            "new_users": int(row.metric_values[1].value),
            "engaged_sessions": engaged_sessions,
            "engagement_rate": row.metric_values[3].value,
            "avg_engagement_time": row.metric_values[4].value,
            "event_count": int(row.metric_values[5].value),
            "engaged_sessions_per_user": round(engaged_sessions / max(active_users, 1), 2),
            "date": fetch_date
        })

    return {
        "ga4_top_pages_daily": top_pages,
        "ga4_traffic_acquisition_daily": traffic_sources,
        "ga4_country_metrics_daily": country_metrics,
        "ga4_browser_metrics_daily": browser_metrics
    }


def run_ga4_fetch_for_tenant(tenant_id: str, fetch_date: date, service_account: dict, property_id: str):
    session_id = str(uuid.uuid4())
    print(f"📈 Running GA4 fetch for tenant: {tenant_id} | date: {fetch_date}")
    ensure_tenant_exists(tenant_id)

    ga4_data = fetch_ga4_data(tenant_id, fetch_date, session_id)

    for table_suffix, rows in ga4_data.items():
        table_name = table_suffix
        readable_name = table_suffix.replace("ga4_", "").replace("_", " ").title()
        print(f"📊 GA4 {readable_name} Rows on {fetch_date}: {len(rows)}")
        insert_rows(table_name, rows)

    print("✅ GA4 data fetched and stored successfully.\n")