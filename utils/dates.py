"""
Utility functions for handling date and time operations, including Unix timestamp ranges.
Author: Johandré van Deventer
Date: 2025-06-13
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple


def get_daily_unix_ranges(
    start_date: str, end_date: str, tz_name: str
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


def group_unix_ranges_by_month(
    daily_ranges: List[Tuple[int, int]], tz_name: str = "Africa/Johannesburg"
) -> Dict[str, List[Tuple[int, int]]]:
    """Group daily Unix ranges by 'YYYY-MM' month."""
    grouped: Dict[str, List[Tuple[int, int]]] = {}
    tz = ZoneInfo(tz_name)

    for start_unix, end_unix in daily_ranges:
        dt = datetime.fromtimestamp(start_unix, tz)
        month_key = dt.strftime("%Y-%m")
        grouped.setdefault(month_key, []).append((start_unix, end_unix))

    return grouped


def get_month_year_string(unix_timestamp):
    """Extract year and month from Unix timestamp."""
    dt = datetime.fromtimestamp(unix_timestamp)
    return dt.strftime("%B %Y")
