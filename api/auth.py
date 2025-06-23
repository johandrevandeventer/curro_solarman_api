"""
Authentication module for Solarman API access.
Author: Johandré van Deventer
Date: 2025-06-13
"""

import os
import time
import requests

TOKEN_CACHE = None
TOKEN_EXPIRY = 0  # Initialize to expired


def get_access_token(session: requests.Session) -> str:
    """Fetch access token from authentication API with caching"""
    global TOKEN_CACHE, TOKEN_EXPIRY

    # Return cached token if still valid (assuming 1 hour expiry)
    if TOKEN_CACHE and time.time() < TOKEN_EXPIRY:
        return TOKEN_CACHE

    app_id = os.getenv("API_APP_ID")
    app_secret = os.getenv("API_APP_SECRET")
    email = os.getenv("API_EMAIL")
    password = os.getenv("API_PASSWORD")
    org_id = os.getenv("API_ORG_ID")

    base_url = "https://globalapi.solarmanpv.com/account/v1.0/token"

    headers = {"Content-Type": "application/json"}
    params = {"language": "en", "appId": app_id}
    payload = {
        "appSecret": app_secret,
        "email": email,
        "password": password,
        "orgId": org_id,
    }

    response = session.post(
        url=base_url,
        headers=headers,
        json=payload,
        params=params,
        timeout=5,  # Reduced from 15
    )
    response.raise_for_status()

    data = response.json()
    TOKEN_CACHE = data["access_token"]
    TOKEN_EXPIRY = (
        time.time() + 3600
    )  # Cache for 1 hour (adjust based on actual token expiry)

    return TOKEN_CACHE
