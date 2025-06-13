"""
API client for interacting with the backend services.
Author: Johandré van Deventer
Date: 2025-06-13
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def create_session():
    """Create a requests session with retry logic"""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST", "GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
