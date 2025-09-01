# router/tenant_router.py
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from services.credential_service import save_credential
from utils.jwt_utils import get_current_user
from enum import Enum

from db.db import get_db,get_connection 

router = APIRouter(prefix="/tenant", tags=["Tenant Credentials"])

class ServiceEnum(str, Enum):
    ga4 = "ga4"
    gsc = "gsc"
    cloudflare = "cloudflare"

class CredentialUpload(BaseModel):
    service: ServiceEnum
    key: str
    value: str

@router.post("/credentials")
def upload_credential(
    payload: CredentialUpload,
    db: Session = Depends(get_db),
    user=Depends(get_current_user)
):
    try:
        return save_credential(
            db=db,
            tenant_id=user.tenant_id,
            service=payload.service,
            key=payload.key,
            value=payload.value
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tenants")
def list_tenants():
    query = "SELECT DISTINCT tenant_id FROM tenant_credentials"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            tenants = [row[0] for row in cur.fetchall()]
    return {"tenants": tenants}