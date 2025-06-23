"""
Utility functions for file operations.
Author: Johandré van Deventer
Date: 2025-06-13
"""

import pandas as pd
from pathlib import Path


class FileError(Exception):
    """Base exception for file-related errors"""

    pass


def read_device_ids(file_path: str) -> dict[str, str]:
    """Reads device IDs from an Excel file and returns a mapping of device serial numbers to their connected plant names."""

    file = Path(file_path)

    if not file.exists():
        raise FileError(f"File not found at {file_path}")
    if not file.is_file():
        raise FileError(f"Path is not a file: {file_path}")

    try:
        devices_df = pd.read_excel(file_path)
        device_map = dict(
            zip(
                devices_df["SN"],
                devices_df["Connected Plant"]
                # .str.lower()
                .str.replace("Curro ", ""),
                # .str.replace(" ", "_"),
            )
        )

        sorted_device_map = dict(sorted(device_map.items(), key=lambda item: item[1]))
        device_map = sorted_device_map

        return device_map
    except Exception as e:
        raise FileError(f"Error reading device IDs from {file_path}: {str(e)}") from e
