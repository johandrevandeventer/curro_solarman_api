# models/process_data.py

"""
Data processing models for device data.
Author: Johandré van Deventer
Date: 2025-06-13
"""

import pandas as pd


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Clean DataFrame column names by removing non-breaking spaces and stripping whitespace."""
    df.columns = df.columns.str.replace("\xa0", " ", regex=False)
    df.columns = df.columns.str.replace("  ", " ", regex=False)
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.encode("ascii", "ignore").str.decode("ascii")

    return df
