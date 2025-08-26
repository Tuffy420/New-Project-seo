# router/alert_router.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from db.db import insert_alert_event

router = APIRouter()

# Pydantic model for the incoming alert data
class AlertPayload(BaseModel):
    tenant_id: str
    alert_type: str
    message: str
    alert_triggered: bool

# Webhook endpoint for receiving alerts
@router.post("/webhook/alert")
async def receive_alert(payload: AlertPayload):
    try:
        insert_alert_event(
            tenant_id=payload.tenant_id,
            alert_type=payload.alert_type,
            message=payload.message,
        )
        return {"message": f"✅ Alert received for tenant {payload.tenant_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Error: {str(e)}")
