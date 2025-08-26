## router/ga4_router.py
from fastapi import APIRouter, Depends
from db.db import get_table_data
from utils.jwt_utils import get_current_user  

router = APIRouter(prefix="/ga4", tags=["GA4 Daily Data"])

@router.get("/top-pages")
def get_top_pages(user=Depends(get_current_user)):
    return get_table_data("ga4_top_pages_daily", tenant_id=user.tenant_id)

@router.get("/traffic-sources")
def get_traffic_sources(user=Depends(get_current_user)):
    return get_table_data("ga4_traffic_acquisition_daily", tenant_id=user.tenant_id)

@router.get("/country-metrics")
def get_country_metrics(user=Depends(get_current_user)):
    return get_table_data("ga4_country_metrics_daily", tenant_id=user.tenant_id)

@router.get("/browser-metrics")
def get_browser_metrics(user=Depends(get_current_user)):
    return get_table_data("ga4_browser_metrics_daily", tenant_id=user.tenant_id)
