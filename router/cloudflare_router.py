# router/cloudflare_router.py
from fastapi import APIRouter, Query, Depends
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta
from services.cloudflare_service import CloudflareAnalyticsExtractor
from utils.jwt_utils import get_current_user

router = APIRouter(prefix="/cloudflare", tags=["Cloudflare"])

@router.get("/summary")
def fetch_cloudflare_summary(
    days: int = Query(7, description="Fetch data for N days"),
    user=Depends(get_current_user)
):
    try:
        extractor = CloudflareAnalyticsExtractor.from_tenant(user.tenant_id)

        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days - 1)).strftime('%Y-%m-%d')

        raw_data = extractor.get_pageviews_and_visits(start_date, end_date)
        df = extractor.format_data_to_dataframe(raw_data)

        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
