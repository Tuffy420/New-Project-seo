from pydantic import BaseModel

class TokenData(BaseModel):
    username: str
    tenant_id: str
