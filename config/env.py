"""
Environment variables handling for the project.
Author: Johandré van Deventer
Date: 2025-06-13
"""

import os
from dotenv import load_dotenv


def check_required_env_vars():
    """Check if all required environment variables are set."""
    required_vars = [
        "API_APP_ID",
        "API_APP_SECRET",
        "API_EMAIL",
        "API_PASSWORD",
        "API_ORG_ID",
    ]

    # Load environment variables
    load_dotenv()

    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )
