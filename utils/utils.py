"""Utility functions for various tasks."""


def print_header(header: str = "Header", width: int = 80) -> None:
    """Print a formatted header with a specified width."""
    print("\n" + "=" * width)
    print(f"{header.center(width)}")
    print("=" * width + "\n")
