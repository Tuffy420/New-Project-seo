# services/cloudflare_service.py
import requests
import pandas as pd
from typing import Dict
from sqlalchemy.orm import Session
from db.db import get_connection
from services.credential_service import get_credentials_for_service

class CloudflareAnalyticsExtractor:
    def __init__(self, api_token: str, zone_id: str):
        self.api_token = api_token
        self.zone_id = zone_id
        self.base_url = "https://api.cloudflare.com/client/v4/graphql"
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }

    @classmethod
    def from_tenant(cls, tenant_id: str):
    # Directly fetch credentials from PostgreSQL using helper
      credentials = get_credentials_for_service(tenant_id, service_name="cloudflare")

      if not credentials:
        raise ValueError(f"❌ Cloudflare credentials not found for tenant: {tenant_id}")

      api_token = credentials.get("api_token")
      zone_id = credentials.get("zone_id")

      if not api_token or not zone_id:
        raise ValueError(f"❌ Missing required Cloudflare credentials for tenant: {tenant_id}")

      return cls(api_token=api_token, zone_id=zone_id)

    def _execute_query(self, query: str) -> Dict:
        response = requests.post(self.base_url, headers=self.headers, json={"query": query})
        if response.status_code != 200:
            raise Exception(f"❌ API request failed: {response.status_code} - {response.text}")
        return response.json()

    def get_pageviews_and_visits(self, start_date: str, end_date: str) -> Dict:
        query = f"""
        {{
          viewer {{
            zones(filter: {{zoneTag: "{self.zone_id}"}}) {{
              httpRequests1dGroups(
                filter: {{
                  date_geq: "{start_date}",
                  date_leq: "{end_date}"
                }}
                limit: 1000
              ) {{
                date: dimensions {{
                  date
                }}
                pageViews: sum {{
                  pageViews
                }}
                visitors: uniq {{
                  uniques
                }}
              }}
            }}
          }}
        }}
        """
        return self._execute_query(query)

    def format_data_to_dataframe(self, raw_data: Dict) -> pd.DataFrame:
        try:
            zones = raw_data.get('data', {}).get('viewer', {}).get('zones', [])
            if not zones or 'httpRequests1dGroups' not in zones[0]:
                return pd.DataFrame()

            rows = zones[0]['httpRequests1dGroups']
            formatted = [{
                'date': item['date']['date'],
                'page_views': item['pageViews']['pageViews'],
                'visits': item['visitors']['uniques']
            } for item in rows]

            df = pd.DataFrame(formatted)
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            return df
        except Exception as e:
            print(f"❌ Failed to format Cloudflare data: {e}")
            return pd.DataFrame()
