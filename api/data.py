"""
Module for fetching device data from the Solarman API.
Author: Johandré van Deventer
Date: 2025-06-13
"""

import requests
import time
from typing import Optional, Dict, Any


def fetch_device_data(
    session: requests.Session,
    access_token: str,
    device_id: str,
    start_time: int,
    end_time: int,
    max_retries: int = 5,
    initial_backoff: float = 1.0,
) -> Optional[Dict[str, Any]]:
    """
    Fetch data for a specific device using the access token with robust error handling.

    Args:
        session: Authenticated requests session
        access_token: API access token
        device_id: Device serial number
        start_time: Start timestamp (seconds)
        end_time: End timestamp (seconds)
        max_retries: Maximum number of retry attempts
        initial_backoff: Initial backoff time in seconds (doubles each retry)

    Returns:
        JSON response data or None if all retries failed

    Raises:
        requests.exceptions.RequestException: If request fails after all retries
    """
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

    retry_count = 0
    backoff = initial_backoff

    while retry_count < max_retries:
        try:
            response = session.post(
                url=base_url,
                headers=headers,
                json=payload,
                timeout=60,
            )

            # Check for 5xx errors
            if response.status_code >= 500:
                # logger.warning(
                #     f"Server error (attempt {retry_count + 1}/{max_retries}): "
                #     f"HTTP {response.status_code}"
                # )
                raise requests.exceptions.HTTPError(
                    f"Server error: HTTP {response.status_code}"
                )

            response.raise_for_status()
            # return response.json()

            response_payload = {
                "status": response.status_code,
                "data": response.json() if response.status_code == 200 else None,
                "error": response.text if response.status_code != 200 else None,
                "serial": device_id,
                "start_time": start_time,
                "end_time": end_time,
            }

            return response_payload

        except requests.exceptions.Timeout as e:
            # logger.warning(
            #     f"Timeout occurred (attempt {retry_count + 1}/{max_retries})"
            # )
            if retry_count == max_retries - 1:
                raise requests.exceptions.Timeout(
                    f"Request timed out after {max_retries} attempts"
                ) from e

        except requests.exceptions.HTTPError as e:
            if retry_count == max_retries - 1:
                raise requests.exceptions.RequestException(
                    f"API request failed after {max_retries} attempts: {str(e)}"
                ) from e

        except requests.exceptions.RequestException as e:
            # logger.error(f"Request failed: {str(e)}")
            if retry_count == max_retries - 1:
                raise requests.exceptions.RequestException(
                    f"API request failed after {max_retries} attempts: {str(e)}"
                ) from e

        # Exponential backoff before retrying
        sleep_time = min(backoff * (2**retry_count), 60)  # Cap at 60 seconds
        # logger.info(f"Waiting {sleep_time:.1f} seconds before retry...")
        time.sleep(sleep_time)
        retry_count += 1

    return None
