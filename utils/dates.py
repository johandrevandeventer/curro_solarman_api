"""
Utility functions for handling date and time operations, including Unix timestamp ranges.
Author: Johandré van Deventer
Date: 2025-06-13
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Tuple

from config import setup


def get_daily_unix_ranges(
    start_date: str, end_date: str, tz_name: str = setup.TIMEZONE
) -> List[Tuple[int, int]]:
    """Generate Unix timestamps for each day in range"""
    date_ranges = []
    tz = ZoneInfo(tz_name)

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    current_day = start
    while current_day <= end:
        start_of_day = datetime(
            current_day.year, current_day.month, current_day.day, 0, 0, 0, tzinfo=tz
        )
        end_of_day = datetime(
            current_day.year, current_day.month, current_day.day, 23, 59, 59, tzinfo=tz
        )
        date_ranges.append((int(start_of_day.timestamp()), int(end_of_day.timestamp())))
        current_day += timedelta(days=1)

    return date_ranges
