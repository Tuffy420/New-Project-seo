# services/gsc_daily_fetch.py
from datetime import date, timedelta
from googleapiclient.discovery import build
from google.oauth2 import service_account
import json
from sqlalchemy.orm import Session


from db.db import (
    insert_gsc_summary_daily,
    insert_gsc_queries_daily,
    insert_gsc_pages_daily,
    insert_gsc_countries_daily,
    insert_gsc_devices_daily,
    get_connection,
 # ✅ NEW import
)
from services.credential_service import get_credentials_for_service
from utils.credential_utils import build_gsc_credentials

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]

def initialize_gsc_api(credentials_data: dict):
    service_creds = build_gsc_credentials(credentials_data)
    credentials = service_account.Credentials.from_service_account_info(
        service_creds, scopes=SCOPES
    )
    return build("searchconsole", "v1", credentials=credentials)

from google.oauth2.service_account import Credentials

def build_gsc_credentials(creds_data: dict):
    return Credentials.from_service_account_info(
        creds_data,
        scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
    )


def fetch_gsc_data(tenant_id: str, creds, site_url: str, target_date: date = None):
    service = build("searchconsole", "v1", credentials=creds)

    if not site_url:
        print(f"⚠️ site_url not provided for tenant {tenant_id}, auto-detecting...")
        sites_list = service.sites().list().execute()
        possible_sites = [
            entry["siteUrl"]
        for entry in sites_list.get("siteEntry", [])
            if entry.get("permissionLevel") in ["siteOwner", "siteFullUser"]
        ]
        if possible_sites:
            site_url = possible_sites[0]
            print(f"✅ Auto-detected site_url: {site_url}")
        else:
            raise ValueError("No accessible GSC properties found for service account.")

    if target_date is None:
        target_date = date.today() - timedelta(days=3)
    start_date = end_date = target_date.isoformat()
    session_base = f"{tenant_id}_{target_date}"

    print(f"\n🔄 Running GSC fetch for tenant: {tenant_id} | date: {target_date}")

    def query_gsc(dimensions):
        request = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions,
            "rowLimit": 25000,
        }
        response = service.searchanalytics().query(siteUrl=site_url, body=request).execute()
        return response.get("rows", [])

    insert_gsc_summary_daily([
        {
            "date": target_date,
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"],
            "tenant_id": tenant_id,
            "session_id": f"{session_base}_summary_{idx}"
        }
        for idx, row in enumerate(query_gsc([]))
    ])

    insert_gsc_queries_daily([
        {
            "date": target_date,
            "query": row["keys"][0],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"],
            "tenant_id": tenant_id,
            "session_id": f"{session_base}_query_{idx}"
        }
        for idx, row in enumerate(query_gsc(["query"]))
    ])

    insert_gsc_pages_daily([
        {
            "date": target_date,
            "page": row["keys"][0],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"],
            "tenant_id": tenant_id,
            "session_id": f"{session_base}_page_{idx}"
        }
        for idx, row in enumerate(query_gsc(["page"]))
    ])

    insert_gsc_countries_daily([
        {
            "date": target_date,
            "country": row["keys"][0],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"],
            "tenant_id": tenant_id,
            "session_id": f"{session_base}_country_{idx}"
        }
        for idx, row in enumerate(query_gsc(["country"]))
    ])

    insert_gsc_devices_daily([
        {
            "date": target_date,
            "device": row["keys"][0],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"],
            "tenant_id": tenant_id,
            "session_id": f"{session_base}_device_{idx}"
        }
        for idx, row in enumerate(query_gsc(["device"]))
    ])

    print("✅ GSC data fetched and stored successfully.")
    
def run_gsc_fetch_for_tenant(tenant_id: str, target_date=None):
    with get_connection() as conn:
        db = Session(bind=conn)
        raw_creds = get_credentials_for_service(tenant_id, "gsc")  # Pass db session if needed
        db.close()

    service_account_json = raw_creds.get("SERVICE_ACCOUNT_JSON")

    if service_account_json is None:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON in credentials")

    if isinstance(service_account_json, str):
        creds_data = json.loads(service_account_json)
    elif isinstance(service_account_json, dict):
        creds_data = service_account_json
    else:
        raise ValueError("SERVICE_ACCOUNT_JSON credential is neither str nor dict")

    creds = build_gsc_credentials(creds_data)

    if target_date is None:
        target_date = date.today() - timedelta(days=3)

    fetch_gsc_data(tenant_id, creds, creds_data.get("site_url"), target_date)

