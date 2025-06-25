# processor/processor.py

"""
Processor for device data.
Author: Johandré van Deventer
Date: 2025-06-13
"""

from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from pathlib import Path
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from tqdm import tqdm
from api.client import create_session
from api.auth import get_access_token
from api.data import fetch_device_data
from processor.checkpoint_manager import CheckpointManager
from shutdown.shutdown_controller import is_shutdown_requested
from utils.utils import print_header, print_sub_header
from utils.dates import (
    get_daily_unix_ranges,
)

DELAY_1 = 0  # seconds
DELAY_2 = 0  # seconds
DELAY_3 = 0  # seconds


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

        self._create_session()
        self._fetch_access_token()

        self.start_unix = 0
        self.end_unix = 0
        self.date_ranges = []

        self._prepare_date_ranges()

        # Checkpoint file to save progress
        checkpoint_dir = self.app_config["checkpoint"]["directory"]
        checkpoint_filename = f"{self.app_config["checkpoint"]["file_prefix"]}.pkl"
        self.checkpoint_file_path = f"{checkpoint_dir}/{checkpoint_filename}"

        self.checkpoint = CheckpointManager(self.checkpoint_file_path)

    def _create_session(self):
        """Create a new session for API requests."""
        print_sub_header("Creating API Session")
        self.session = create_session()
        if not self.session:
            # print("✖  Error creating API session")
            # return
            raise DeviceDataProcessorError("Failed to create API session")

        print("✔  API session created successfully")

        time.sleep(DELAY_2)

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

        time.sleep(DELAY_2)

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

        time.sleep(DELAY_2)

    def _print_skipped_devices(self, device_ids: list[str]):
        """
        Print devices that have already been processed.
        """
        if not device_ids:
            print("No devices to process.")
            return

        print_sub_header("Skipped Devices")

        skipped_devices = [
            device_serial
            for device_serial in device_ids
            if self.checkpoint.is_device_complete(device_serial)
        ]

        for device_serial in skipped_devices:
            site_name = self.device_map.get(device_serial, "Unknown Site")
            print(f"Skipping device {site_name} ({device_serial}) (already processed)")

        if skipped_devices:
            print()

    def _get_remaining_devices(self, device_ids: list[str]) -> list[str]:
        """
        Get the list of devices that have not been fully processed.
        """
        remaining_devices = [
            device_serial
            for device_serial in device_ids
            if not self.checkpoint.is_device_complete(device_serial)
        ]
        return remaining_devices

    def _get_remaining_dates(
        self, device_serial: str, date_ranges: list[tuple[int, int]]
    ) -> list[tuple[int, int]]:
        """
        Get the list of date ranges that have not been fully processed for a device.
        """
        remaining_dates = []

        for start_unix, end_unix in date_ranges:
            start_date_obj = datetime.fromtimestamp(start_unix)
            start_date_str = start_date_obj.strftime("%Y-%m-%d")

            if not self.checkpoint.is_date_complete(device_serial, start_date_str):
                remaining_dates.append((start_unix, end_unix))

        return remaining_dates

    def process_date(self, args, result_queue: Queue):
        device_serial, start_unix, end_unix, device_index = args
        start_date_obj = datetime.fromtimestamp(start_unix)
        start_date_str = start_date_obj.strftime("%Y-%m-%d")

        fetch_device_data(
            self.session,
            self.access_token,
            device_serial,
            start_unix,
            end_unix,
        )

        result_queue.put((device_serial, start_date_str, device_index))

        return True

    def run(self):
        """
        Run the data collection process for all devices and dates.
        """
        print_header("Starting Data Collection")

        time.sleep(DELAY_2)

        checkpoint_data = self.checkpoint.load_checkpoint()

        print(checkpoint_data)

        processed_devices = 0
        progress_devices = {}
        total_progress_devices = {}

        try:
            device_ids = list(self.device_map.keys())
            remaining_devices = self._get_remaining_devices(device_ids)
            print(f"Remaining devices to process: {len(remaining_devices)}")

            self._print_skipped_devices(device_ids)

            devices_bar = tqdm(
                total=len(remaining_devices),
                desc="Devices",
                unit="device",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]",
                position=0,
                leave=True,
            )

            for device_index, device_serial in enumerate(remaining_devices):

                site_name = self.device_map[device_serial]

                # Track whether we finished all dates
                completed_all_dates = True

                # Get remaining date ranges for the current device
                remaining_dates = self._get_remaining_dates(
                    device_serial, self.date_ranges
                )

                device_bar = tqdm(
                    total=len(remaining_dates),
                    desc=f"Collecting {site_name[:15]}... ({device_serial})",
                    unit="day",
                    leave=False,
                    position=1,
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]",
                )

                days_processed = 0

                task_args = [
                    (
                        device_serial,
                        start_unix,
                        end_unix,
                        device_index,
                    )
                    for start_unix, end_unix in remaining_dates
                ]

                try:
                    result_queue = Queue()
                    with ThreadPoolExecutor(max_workers=30) as executor:
                        futures = []
                        for args in task_args:
                            if is_shutdown_requested():
                                completed_all_dates = False
                                break

                            futures.append(
                                executor.submit(self.process_date, args, result_queue)
                            )

                        for future in futures:
                            if is_shutdown_requested():
                                completed_all_dates = False
                                break

                            future.result()  # This will raise exceptions if any occurred
                            days_processed += 1
                            device_bar.update(1)

                            while not result_queue.empty():
                                device_serial, start_date_str, device_index = (
                                    result_queue.get()
                                )
                                self.checkpoint.mark_date_complete(
                                    device_serial, start_date_str
                                )
                                self.checkpoint.save_checkpoint(
                                    device_index, device_serial, start_date_str
                                )

                except Exception as e:
                    completed_all_dates = False

                # Handle progress tracking (similar to original code)
                pre_processed_days = len(self.date_ranges) - len(remaining_dates)

                if not completed_all_dates:
                    if len(remaining_dates) > days_processed > 0:
                        progress_devices[device_serial] = days_processed
                        total_progress_devices[device_serial] = (
                            days_processed + pre_processed_days
                        )

                    if (
                        days_processed > 0
                        and days_processed == len(remaining_dates)
                        and device_serial in progress_devices
                    ):
                        del progress_devices[device_serial]
                        del total_progress_devices[device_serial]

                device_bar.close()

                # Only mark device as complete if we processed all dates
                if completed_all_dates:
                    self.checkpoint.mark_device_complete(device_serial)
                    processed_devices += 1

                    devices_bar.update(1)

            devices_bar.close()

        except Exception as e:
            print(f"Error during data collection: {e}")
            raise DeviceDataProcessorError(f"Data collection failed: {e}") from e
        finally:
            if is_shutdown_requested():
                print("\nShutdown requested. Running cleanup handlers...", flush=True)

                if len(progress_devices) > 0:
                    print(
                        f"\nProcessed {processed_devices} devices, but {len(progress_devices)} devices were not fully processed."
                    )

                    for device_serial, days_processed in progress_devices.items():
                        print(
                            f" - {self.device_map.get(device_serial, 'Unknown Site')} ({device_serial}): {days_processed} days processed and {total_progress_devices[device_serial]} total days processed."
                        )

                    print("\nProgress saved. You can resume later.")
