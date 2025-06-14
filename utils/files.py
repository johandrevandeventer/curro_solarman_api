"""
Utility functions for file operations.
Author: Johandré van Deventer
Date: 2025-06-13
"""

import pandas as pd


def read_device_ids(file_path: str) -> dict:
    """Reads device IDs from an Excel file and returns a mapping of device serial numbers to their connected plant names."""

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
    except FileNotFoundError:
        print(f"✖  Error: File not found - {file_path}")
        return {}
    except Exception as e:
        print(f"✖  Error: {e}")
        return {}
