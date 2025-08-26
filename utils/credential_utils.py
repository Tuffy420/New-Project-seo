#utils/credential_utils.py
def build_gsc_credentials(credentials: dict) -> dict:
    if not credentials.get("client_email") or not credentials.get("private_key"):
        raise ValueError("Missing GSC credentials: client_email or private_key")
    
    return {
        "type": "service_account",
        "client_email": credentials["client_email"],
        "private_key": credentials["private_key"].replace('\\n', '\n'),
        "token_uri": "https://oauth2.googleapis.com/token"
    }


def build_cloudflare_headers(credentials: dict) -> dict:
    token = credentials.get("api_token")
    if not token:
        raise ValueError("Missing Cloudflare API token")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def build_ga4_credentials(credentials: dict) -> dict:
    return {
        "type": "service_account",
        "client_email": credentials.get("client_email"),
        "private_key": credentials.get("private_key", "").replace('\\n', '\n'),
        "token_uri": "https://oauth2.googleapis.com/token"
    }
