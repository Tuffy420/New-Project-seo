# main.py
from fastapi import FastAPI
from dotenv import load_dotenv
from db.db import setup_tables
from router.gsc_router import router as gsc_router
from router.ga4_router import router as ga4_router
from router.cloudflare_router import router as cloudflare_router
from router.fetch_router import router as fetch_router
from router.auth_router import router as auth_router
from router.alert_router import router as alert_router
from router.tenant_router import router as tenant_router
from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI, UploadFile, Form
from fastapi.responses import JSONResponse  
import shutil
from fastapi.middleware.cors import CORSMiddleware
from router import data_router 
load_dotenv()

app = FastAPI(title="SEO Analytics API")

# ⏳ Initialize DB Tables
setup_tables()

# 🔗 Register API Routes
app.include_router(gsc_router)
app.include_router(ga4_router)
app.include_router(cloudflare_router)
app.include_router(fetch_router)
app.include_router(auth_router, prefix="/api")
app.include_router(alert_router)
app.include_router(tenant_router, prefix="/api")
app.include_router(data_router.router)
app.mount("/static", StaticFiles(directory="frontend"), name="static")
@app.get("/")
def root():
    return {"message": "Welcome to the SEO Analytics API"}



app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or restrict to ["http://127.0.0.1:5500"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
