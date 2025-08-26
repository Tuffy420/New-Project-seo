
# services/credential_service.py
from sqlalchemy.orm import Session
from models.tenant_credentials import TenantCredentials
from db.db import get_connection
import json
import re
import logging

logger = logging.getLogger(__name__)

def save_credential(db: Session, tenant_id: str, service: str, key: str, value: str):
    existing = db.query(TenantCredentials).filter_by(
        tenant_id=tenant_id, service=service, key=key
    ).first()

    if existing:
        existing.value = value  # update
    else:
        existing = TenantCredentials(
            tenant_id=tenant_id,
            service=service,
            key=key,
            value=value
        )
        db.add(existing)

    db.commit()
    db.refresh(existing)
    return existing

def _normalize_key(key) -> str:
    """Coerce to str, strip, uppercase and strip out anything not A-Z0-9_."""
    if key is None:
        return ""
    k = str(key)
    # trim whitespace
    k = k.strip().upper()
    # remove characters that are not A-Z, 0-9 or underscore
    k = re.sub(r'[^A-Z0-9_]', '', k)
    return k

def _try_parse_json_maybe_twice(value):
    """Try to turn value into dict/list if it's JSON. Handles common double-encoded cases."""
    # If it's already a dict/list, return it
    if isinstance(value, (dict, list)):
        return value

    if not isinstance(value, str):
        return value

    # Try normal JSON parse
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        pass

    # Sometimes the DB stores a JSON string wrapped in quotes or escaped twice.
    # Try stripping surrounding quotes then parse:
    trimmed = value.strip()
    if (trimmed.startswith('"') and trimmed.endswith('"')) or (trimmed.startswith("'") and trimmed.endswith("'")):
        try:
            return json.loads(trimmed[1:-1])
        except Exception:
            pass

    # As a last attempt, un-escape common escapes and try again
    try:
        unescaped = trimmed.encode('utf-8').decode('unicode_escape')
        return json.loads(unescaped)
    except Exception:
        # fallback to original string
        return value

def get_credentials_for_service(tenant_id: str, service_name: str) -> dict:
    """
    Returns a dict mapping NORMALIZED_KEY -> parsed_value.
    Normalized keys are uppercase, trimmed, and non-alphanumerics removed.
    Values are JSON-decoded where possible.
    """
    credentials = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT key, value FROM tenant_credentials WHERE tenant_id = %s AND service = %s",
                (tenant_id, service_name),
            )
            rows = cur.fetchall()

    # Debug/log the raw rows (optional)
    logger.debug("credential rows for tenant=%s service=%s -> %s", tenant_id, service_name, rows)

    for key, value in rows:
        normalized = _normalize_key(key)
        parsed = _try_parse_json_maybe_twice(value)
        credentials[normalized] = parsed

    return credentials

