# router/auth_router.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext
from db.db import get_connection, get_or_create_tenant
from utils.jwt_utils import create_access_token
import uuid

router = APIRouter(prefix="/auth", tags=["Authentication"])

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# 📦 Request Schemas
class RegisterRequest(BaseModel):
    email: EmailStr
    password: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


# ✅ Register Endpoint
@router.post("/register")
def register_user(request: RegisterRequest):
    hashed_password = pwd_context.hash(request.password)

    try:
        conn = get_connection()
        cur = conn.cursor()

        # Insert user if not exists
        cur.execute("""
            INSERT INTO users (email, password, tenant_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (email) DO NOTHING
        """, (request.email, hashed_password, str(uuid.uuid4())))
        conn.commit()

        # Always fetch tenant_id from DB
        cur.execute("SELECT tenant_id FROM users WHERE email = %s", (request.email,))
        tenant_id = cur.fetchone()[0]

        cur.close()
        conn.close()

        # Ensure tenant exists in tenants table
        get_or_create_tenant(tenant_id)

        return {"message": "User registered successfully", "tenant_id": tenant_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Login Endpoint
@router.post("/login")
def login_user(request: LoginRequest):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT password, tenant_id FROM users WHERE email = %s", (request.email,))
    result = cur.fetchone()
    cur.close()
    conn.close()

    if not result:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    hashed_password, tenant_id = result

    if not pwd_context.verify(request.password, hashed_password):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token(data={"sub": request.email, "tenant_id": tenant_id})

    # Include tenant_id in the response
    return {
        "access_token": token,
        "token_type": "bearer",
        "tenant_id": tenant_id
    }
