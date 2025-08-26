# router/fetch_router.py
from fastapi import APIRouter, Depends, Request
from datetime import datetime, timedelta
from services.gsc_daily_fetch import run_gsc_fetch_for_tenant
from services.ga4_daily_fetch import run_ga4_fetch_for_tenant
from services.cloudflare_service import CloudflareAnalyticsExtractor
from db.db import insert_cloudflare_summary, get_tenant_credentials
from utils.jwt_utils import get_current_user, TokenData
from services.credential_service import get_credentials_for_service
router = APIRouter(prefix="/fetch", tags=["Manual Fetch (Secured)"])


def date_range_list(start_date: str, end_date: str) -> list:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    delta = end - start
    return [(start + timedelta(days=i)) for i in range(delta.days + 1)]


@router.post("/gsc")
async def fetch_gsc(request: Request, user: TokenData = Depends(get_current_user)):
    try:
        tenant_id = user.tenant_id
        data = await request.json()  # ✅ fixed
        start_date = data.get("start_date")
        end_date = data.get("end_date")

        # GSC fetch is always 3 days behind
        adjusted_dates = [
            d - timedelta(days=3) for d in date_range_list(start_date, end_date)
        ]

        for target_date in adjusted_dates:
            print(f"🔐 Authenticated GSC fetch for tenant {tenant_id} on {target_date}")
            run_gsc_fetch_for_tenant(tenant_id, target_date)

        return {"message": "GSC data fetched", "tenant_id": tenant_id}

    except Exception as e:
        print("Error in GSC fetch:", e)
        return {"error": str(e)}


@router.post("/ga4")
async def fetch_ga4(request: Request, user: TokenData = Depends(get_current_user)):
    try:
        tenant_id = user.tenant_id
        data = await request.json()
        start_date = data.get("start_date")
        end_date = data.get("end_date")

        creds = get_credentials_for_service(tenant_id, "ga4")
        service_account = (
            creds.get("SERVICEACCOUNTJSON") or
            creds.get("SERVICEACCOUNT") or
            creds.get("service_account")
        )
        property_id = creds.get("PROPERTYID") or creds.get("property_id")

        if not service_account:
            return {"error": "Missing GA4 service account JSON for this tenant"}

        for target_date in date_range_list(start_date, end_date):
            print(f"🔐 Authenticated GA4 fetch for tenant {tenant_id} on {target_date}")
            run_ga4_fetch_for_tenant(
                tenant_id,
                target_date,
                service_account=service_account,
                property_id=property_id  # Only if required
            )

        return {"message": "GA4 data fetched", "tenant_id": tenant_id}

    except Exception as e:
        print("Error in GA4 fetch:", e)
        return {"error": str(e)}

@router.post("/cloudflare")
async def fetch_cloudflare(request: Request, user: TokenData = Depends(get_current_user)):
    try:
        tenant_id = user.tenant_id
        data = await request.json()  # ✅ fixed
        start_date = data.get("start_date")
        end_date = data.get("end_date")

        print(f"🔐 Authenticated Cloudflare fetch for tenant {tenant_id} on {start_date} to {end_date}")

        creds = get_tenant_credentials(tenant_id, "cloudflare")
        api_token = creds.get("api_token")
        zone_id = creds.get("zone_id")

        if not api_token or not zone_id:
            return {"error": "Missing Cloudflare API token or zone_id for this tenant"}

        extractor = CloudflareAnalyticsExtractor(api_token, zone_id)
        raw_data = extractor.get_pageviews_and_visits(start_date, end_date)
        df = extractor.format_data_to_dataframe(raw_data)

        for _, row in df.iterrows():
            insert_cloudflare_summary(
                tenant_id=tenant_id,
                session_id=f"{tenant_id}_{row['date']}",
                date=row["date"],
                page_views=row["page_views"],
                visits=row["visits"]
            )

        return {
            "message": "Cloudflare data fetched",
            "tenant_id": tenant_id,
            "range": f"{start_date} to {end_date}"
        }

    except Exception as e:
        print("Error in Cloudflare fetch:", e)
        return {"error": str(e)}
