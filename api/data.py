"""
Module for fetching device data from the Solarman API.
Author: Johandré van Deventer
Date: 2025-06-13
"""

import requests


def fetch_device_data(
    session: requests.Session,
    access_token: str,
    device_id: str,
    start_time: int,
    end_time: int,
):
    """Fetch data for a specific device using the access token"""
    base_url = "https://globalapi.solarmanpv.com/device/v1.0/historical"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    payload = {
        "timeType": 5,
        "startTime": start_time,
        "endTime": end_time,
        "deviceSn": device_id,
    }

    try:
        response = session.post(
            url=base_url,
            headers=headers,
            json=payload,
            timeout=60,
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout as e:
        raise requests.exceptions.Timeout("Request timed out after 60 seconds") from e
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(
            f"API request failed: {str(e)}"
        ) from e
