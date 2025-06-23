# processor/processor.py

"""
Processor for device data.
Author: Johandré van Deventer
Date: 2025-06-13
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo
import os
import pickle
from tqdm import tqdm
import pandas as pd

from api.client import create_session
from api.data import fetch_device_data
from api.auth import get_access_token
from models.process_api import process_api_response
from models.process_data import clean_columns
from utils.utils import print_header, print_sub_header
from utils.dates import (
    get_daily_unix_ranges,
    group_unix_ranges_by_month,
    get_month_year_string,
)
from shutdown.shutdown_controller import register_cleanup_handler, is_shutdown_requested


class DeviceDataProcessorError(Exception):
    """Base exception for device data processor errors."""

    pass


class DeviceDataProcessor:
    def __init__(self, app_config: dict[str, Any], device_map: dict[str, str]):
        """
        Initialize the processor with a mapping of device serial numbers to plant names.

        Args:
            device_map (dict[str, str]): Mapping of device serial numbers to plant names.
        """
        self.app_config = app_config
        self.device_map = device_map
        # self.session = None
        # self.access_token = None

        self.date_ranges: list[tuple[int, int]] = []
        self.start_unix = None
        self.end_unix = None

        self._create_session()
        self._fetch_access_token()

        self._prepare_date_ranges()

        # Checkpoint file to save progress
        checkpoint_dir = self.app_config["checkpoint"]["directory"]
        checkpoint_filename = f"{self.app_config["checkpoint"]["file_prefix"]}.pkl"
        self.checkpoint_file_path = f"{checkpoint_dir}/{checkpoint_filename}"

        self.current_device_index = 0
        self.current_device_serial = None
        self.current_date = self.start_unix

        register_cleanup_handler(self.save_checkpoint)

        self.load_checkpoint()

    def _create_session(self):
        """Create a new session for API requests."""
        print_sub_header("Creating API Session")
        self.session = create_session()
        if not self.session:
            # print("✖  Error creating API session")
            # return
            raise DeviceDataProcessorError("Failed to create API session")

        print("✔  API session created successfully")

        time.sleep(2)

    def _fetch_access_token(self):
        print_sub_header("Fetching Access Token")
        get_access_token_start_time = time.time()
        try:
            self.access_token = get_access_token(self.session)
        except Exception as e:
            raise DeviceDataProcessorError(f"Failed to fetch access token: {e}") from e

        get_access_token_end_time = time.time()
        get_access_token_duration = (
            get_access_token_end_time - get_access_token_start_time
        )
        print(
            f"⏱  Time taken to fetch access token: {get_access_token_duration:.2f} seconds"
        )
        print("✔  Access token fetched successfully")
        print(f"🔑 Access Token: {self.access_token}")

        time.sleep(2)

    def _prepare_date_ranges(self):
        print_sub_header("Preparing Date Ranges for Data Collection")

        TZ = ZoneInfo(self.app_config["date_range"]["timezone"])
        self.date_ranges = get_daily_unix_ranges(
            self.app_config["date_range"]["start"],
            self.app_config["date_range"]["end"],
            self.app_config["date_range"]["timezone"],
        )
        self.start_unix = self.date_ranges[0][0]
        self.end_unix = self.date_ranges[-1][1]
        start_date_str = datetime.fromtimestamp(self.start_unix, TZ).strftime(
            "%Y-%m-%d"
        )
        end_date_str = datetime.fromtimestamp(self.end_unix, TZ).strftime("%Y-%m-%d")

        print(f"• Date range: {self.start_unix} to {self.end_unix}")
        print(f"• Date range: {start_date_str} to {end_date_str}")
        print(f"• Total days to process: {len(self.date_ranges)}")

        time.sleep(2)

    def save_checkpoint(self):
        """Save current progress to checkpoint file"""

        print_sub_header("Saving Checkpoint")

        checkpoint = {
            "current_device_index": self.current_device_index,
            "current_device_serial": self.current_device_serial,
            "current_date": self.current_date,
        }

        file = Path(self.checkpoint_file_path)

        if not file.parent.exists():
            file.parent.mkdir(parents=True, exist_ok=True)

        with open(file, "wb") as f:
            pickle.dump(checkpoint, f)

        print(
            f"Checkpoint saved at device {self.current_device_index}, serial {self.current_device_serial}, date {self.current_date}"
        )

    def load_checkpoint(self):
        """Load progress from checkpoint file if exists"""

        print_sub_header("Loading Checkpoint")

        file = Path(self.checkpoint_file_path)

        if file.exists():
            with open(file, "rb") as f:
                checkpoint = pickle.load(f)

            self.current_device_index = checkpoint["current_device_index"]
            self.current_device_serial = checkpoint["current_device_serial"]
            self.current_date = checkpoint["current_date"]
            print(
                f"Resuming from checkpoint: device {self.current_device_index}, serial {self.current_device_serial}, date {self.current_date}"
            )

    def run(self):
        """
        Run the data collection process for all devices and dates.
        """
        print_header("Starting Data Collection")

        time.sleep(2)
