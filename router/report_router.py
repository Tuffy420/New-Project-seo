# router/report_router.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from db.db import fetch_all, get_connection
import smtplib, ssl, os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from io import StringIO
import csv
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()

class ReportRequest(BaseModel):
    emails: list[str]
    gsc: list[str] = []
    ga4: list[str] = []
    cf: list[str] = []

# ---------------- Helper functions ----------------

def dicts_to_csv_attachment(data, metric_name="Report", filename="report.csv"):
    if not data:
        data = []

    if isinstance(data[0], tuple):
        headers = [f"col{i}" for i in range(len(data[0]))]
        rows = data
    else:
        headers = list(data[0].keys()) if data else []
        rows = [tuple(d[h] for h in headers) for d in data]

    headers = ["Metric"] + headers
    rows = [(metric_name, *row) for row in rows]

    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(rows)

    mime_part = MIMEBase('application', 'octet-stream')
    mime_part.set_payload(csv_buffer.getvalue())
    encoders.encode_base64(mime_part)
    mime_part.add_header('Content-Disposition', f'attachment; filename="{filename}"')

    return mime_part

def get_all_tenants():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT tenant_id FROM tenants")
            return [row[0] for row in cur.fetchall()]

# ---------------- Main endpoint ----------------

@router.post("/api/send-report")
async def send_report(req: ReportRequest):
    tenants = get_all_tenants()
    if not tenants:
        raise HTTPException(status_code=400, detail="No tenants found in DB")

    smtp_user = os.getenv("EMAIL_ACCOUNT")
    smtp_pass = os.getenv("EMAIL_PASSWORD")
    smtp_server = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", 587))

    if not smtp_user or not smtp_pass:
        raise HTTPException(status_code=500, detail="SMTP credentials not set in .env")

    context = ssl.create_default_context()

    gsc_mapping = {
        "GSC - Summary": "gsc_summary_daily",
        "GSC - Queries": "gsc_queries_daily",
        "GSC - Page": "gsc_pages_daily",
        "GSC - Country": "gsc_countries_daily",
        "GSC - Device": "gsc_devices_daily",
    }

    ga4_mapping = {
        "GA4 - Page": "ga4_top_pages_daily",
        "GA4 - Traffic": "ga4_traffic_acquisition_daily",
        "GA4 - Country": "ga4_country_metrics_daily",
        "GA4 - Browser": "ga4_browser_metrics_daily",
    }

    cf_mapping = {
        "CF - Cloudflare CSV": "cloudflare_summary_daily"
    }

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls(context=context)
            server.login(smtp_user, smtp_pass)

            for email in req.emails:
                msg = MIMEMultipart()
                msg["Subject"] = "📊 Analytics CSV Report"
                msg["From"] = smtp_user
                msg["To"] = email

                msg.attach(MIMEText("Please find attached CSV reports for all metrics.", "plain"))

                for tenant_id in tenants:
                    # GSC CSV attachments
                    for key in req.gsc:
                        table_name = gsc_mapping.get(key)
                        if table_name:
                            data = fetch_all(table_name, tenant_id)
                            safe_name = key.replace(" ", "").replace("-", "")
                            csv_attachment = dicts_to_csv_attachment(
                                data,
                                metric_name=key,
                                filename=f"{safe_name}.csv"
                            )
                            msg.attach(csv_attachment)

                    # GA4 CSV attachments
                    for key in req.ga4:
                        table_name = ga4_mapping.get(key)
                        if table_name:
                            data = fetch_all(table_name, tenant_id)
                            safe_name = key.replace(" ", "").replace("-", "")
                            csv_attachment = dicts_to_csv_attachment(
                                data,
                                metric_name=key,
                                filename=f"{safe_name}.csv"
                            )
                            msg.attach(csv_attachment)

                    # Cloudflare CSV attachments
                    for key in req.cf:
                        table_name = cf_mapping.get(key)
                        if table_name:
                            data = fetch_all(table_name, tenant_id)
                            safe_name = key.replace(" ", "").replace("-", "")
                            csv_attachment = dicts_to_csv_attachment(
                                data,
                                metric_name=key,
                                filename=f"{safe_name}.csv"
                            )
                            msg.attach(csv_attachment)

                print(f"Sending report to {email}...")
                server.sendmail(smtp_user, [email], msg.as_string())

    except Exception as e:
        print("SMTP error:", e)
        raise HTTPException(status_code=500, detail=f"Email sending failed: {e}")

    return {"message": "Report sent successfully with CSV attachments for GSC, GA4, and Cloudflare."}