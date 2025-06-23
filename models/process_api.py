"""
Model for processing Solarman API responses with optimized timezone handling and data conversion.
Author: Johandré van Deventer
Date: 2025-06-13
"""

from datetime import datetime, timezone
import pandas as pd


def process_api_response(response_data):
    """Optimized processor for Solarman API responses with proper timezone handling.

    Args:
        response_data: API response data to process
        exclude_columns: List of column names to exclude from the final DataFrame
    """
    exclude_columns = ["Battery Mode"]

    if not isinstance(response_data, list):
        response_data = [response_data]

    # Pre-allocate collections
    times = []
    unix_times = []
    records = []
    columns = set()

    for data in response_data:
        # Process timestamp
        timestamp = int(data.get("collectTime", 0))
        local_time = datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone()
        times.append(local_time)
        unix_times.append(timestamp)

        # Process data items
        record = {}
        for item in data.get("dataList", []):
            value = item.get("value")
            name = item.get("name")

            # Skip excluded columns
            if name in exclude_columns:
                continue

            columns.add(name)

            if value is None or value == "":
                record[name] = None
            elif isinstance(value, str):
                try:
                    record[name] = float(value) if "." in value else int(value)
                except ValueError:
                    record[name] = value
            else:
                record[name] = value

        records.append(record)

    # Create DataFrame
    df = pd.DataFrame(records)

    # Add time columns with proper timezone handling
    if times:
        # Convert to timezone-naive in local time
        df["Time"] = pd.to_datetime(
            [t.replace(tzinfo=None) for t in times], format="%Y-%m-%d %H:%M:%S"
        )
        df["Unix_Time"] = unix_times
        df["Hour"] = df["Time"].dt.hour.astype("int8")

    # Optimize numeric columns
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], downcast="float")

    return df
