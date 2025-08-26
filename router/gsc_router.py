## router/gsc_router.py
from fastapi import APIRouter, Depends
from db import db
from utils.jwt_utils import get_current_user  

router = APIRouter(prefix="/gsc", tags=["GSC Daily Data"])

@router.get("/summary")
def get_summary(user=Depends(get_current_user)):
    return db.fetch_table("gsc_summary_daily", tenant_id=user.tenant_id)

@router.get("/queries")
def get_queries(user=Depends(get_current_user)):
    return db.fetch_table("gsc_queries_daily", tenant_id=user.tenant_id)

@router.get("/pages")
def get_pages(user=Depends(get_current_user)):
    return db.fetch_table("gsc_pages_daily", tenant_id=user.tenant_id)

@router.get("/countries")
def get_countries(user=Depends(get_current_user)):
    return db.fetch_table("gsc_countries_daily", tenant_id=user.tenant_id)

@router.get("/devices")
def get_devices(user=Depends(get_current_user)):
    return db.fetch_table("gsc_devices_daily", tenant_id=user.tenant_id)
