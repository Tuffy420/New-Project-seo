# utils/gsc_utils.py

import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import hashlib

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
GSC_KEY_FILE = os.getenv("GSC_KEY_FILE", "credentials/gsc_service_account.json")

def get_gsc_service():
    credentials = service_account.Credentials.from_service_account_file(GSC_KEY_FILE, scopes=SCOPES)
    return build("searchconsole", "v1", credentials=credentials)

def generate_session_id(row: dict) -> str:
    hash_input = "|".join(str(value) for value in row.values())
    return hashlib.md5(hash_input.encode()).hexdigest()

def fetch_gsc_data(site_url, start_date, end_date, tenant_id):
    service = get_gsc_service()

    dimensions_list = [
        ("gsc_summary_daily", []),
        ("gsc_queries_daily", ["query"]),
        ("gsc_pages_daily", ["page"]),
        ("gsc_countries_daily", ["country"]),
        ("gsc_devices_daily", ["device"]),
    ]

    result = {}

    for table_name, dimensions in dimensions_list:
        request = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions,
            "rowLimit": 25000
        }

        response = service.searchanalytics().query(siteUrl=site_url, body=request).execute()
        rows = response.get("rows", [])

        table_rows = []
        for row in rows:
            data = {
                "date": start_date,
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "ctr": row.get("ctr", 0.0),
                "position": row.get("position", 0.0),
                "tenant_id": tenant_id,
            }

            # Attach dynamic dimension value
            if dimensions:
                data[dimensions[0]] = row["keys"][0]

            # Add a unique session_id
            data["session_id"] = generate_session_id(data)

            table_rows.append(data)

        result[table_name] = table_rows

    return result
