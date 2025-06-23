# utils/utils.py

"""
Utility functions for various tasks.
Author: Johandré van Deventer
Date: 2025-06-13
"""


def print_header(header: str = "Header", width: int = 80) -> None:
    """Print a formatted header with a specified width."""
    print("\n" + "=" * width)
    print(f"{header.center(width)}")
    print("=" * width + "\n")
