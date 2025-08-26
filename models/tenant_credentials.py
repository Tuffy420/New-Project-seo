# models/tenant_credentials.py
from sqlalchemy import Column, Integer, String
from db.db import Base  

class TenantCredentials(Base):
    __tablename__ = "tenant_credentials"

    id = Column(Integer, primary_key=True, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    service = Column(String, nullable=False)  # e.g., "ga4", "gsc", "cloudflare"
    key = Column(String, nullable=False)      # e.g., "client_email", "property_id"
    value = Column(String, nullable=False)    # The actual credential
