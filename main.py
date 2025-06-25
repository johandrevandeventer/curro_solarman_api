# main.py

"""
Main module to start the data collection process.
Author: Johandré van Deventer
Date: 2025-06-13
"""

from datetime import datetime
import time

from config.config import get_config, ConfigError
from processor.processor import DeviceDataProcessor, DeviceDataProcessorError
from utils.utils import print_header
from utils.files import read_device_ids, FileError

DELAY_1 = 0  # seconds
DELAY_2 = 0  # seconds
DELAY_3 = 0  # seconds


def main():
    """Main function to start the data collection process.
    This function initializes the application, sets up signal handlers for graceful shutdown,
    and starts the data collection process."""

    # =============================================================================
    # STEP 1: INITIALIZATION
    # =============================================================================
    start_time = time.time()
    print_header(
        f"Starting Data Collection at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    time.sleep(DELAY_2)

    # =============================================================================
    # STEP 2: Load Configuration
    # =============================================================================
    print_header("Loading Configuration")

    load_configuration_start_time = time.time()

    try:
        app_config = get_config("config/config.yaml")
        # print(app_config)
    except ConfigError as e:
        print(f"✖  Error loading configuration: {e}")
        return

    load_configuration_end_time = time.time()
    load_configuration_duration = (
        load_configuration_end_time - load_configuration_start_time
    )

    print(
        f"⏱  Time taken to load configuration: {load_configuration_duration:.2f} seconds\n"
    )
    time.sleep(DELAY_2)

    # =============================================================================
    # STEP 3: READ DEVICE IDS
    # =============================================================================
    print_header("Reading Device IDs from Excel File")

    read_device_id_file_start_time = time.time()

    try:
        device_id_dir = app_config["input"]["device_ids_directory"]
        device_id_filename = f"{app_config["input"]["device_ids_file_prefix"]}.xlsx"
    except KeyError as e:
        print(f"✖  Invalid configuration key: {e}")
        return

    device_id_file_path = f"{device_id_dir}/{device_id_filename}"

    try:
        device_map = read_device_ids(device_id_file_path)
    except FileError as e:
        print(f"✖  Error reading device IDs: {e}")
        return

    read_device_id_file_end_time = time.time()
    read_device_id_file_duration = (
        read_device_id_file_end_time - read_device_id_file_start_time
    )

    print(
        f"⏱  Time taken to read device IDs: {read_device_id_file_duration:.2f} seconds"
    )
    print(f"✔  Found {len(device_map)} device IDs to process\n")

    time.sleep(DELAY_2)

    print_header("Devices Found")

    for serial, device_name in device_map.items():
        print(f"• {device_name} ({serial})")

    time.sleep(DELAY_2)

    # =============================================================================
    # STEP 4: Initialize Processor
    # =============================================================================
    print_header("Initializing Processor")

    try:
        processor = DeviceDataProcessor(app_config, device_map)
    except DeviceDataProcessorError as e:
        print(f"✖  Error initializing processor: {e}")
        return

    processor.run()


if __name__ == "__main__":
    main()
