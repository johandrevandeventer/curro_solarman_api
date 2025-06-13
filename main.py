"""
Main script for the project.
Author: Johandré van Deventer
Date: 2025-06-13
"""

from datetime import datetime
import time
from signal_handler import signal_handler
from utils import utils, files
from config import setup, env
from api import auth, client


def main():
    """Main function to execute the script."""

    global shutdown_requested

    # =============================================================================
    # VARIABLES
    # =============================================================================

    # =============================================================================
    # STEP 1: INITIALIZATION
    # =============================================================================
    signal_handler.setup_signal_handler()

    start_time = time.time()
    utils.print_header(
        f"Starting Data Collection at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    time.sleep(2)

    # =============================================================================
    # STEP 2: CHECK ENVIRONMENT VARIABLES
    # =============================================================================
    utils.print_header("Checking Environment Variables")

    try:
        env.check_required_env_vars()
    except EnvironmentError as e:
        print(f"✖  {e}")
        return

    print("✔  All required environment variables are set")
    time.sleep(2)

    # =============================================================================
    # STEP 2: READ DEVICE IDS
    # =============================================================================
    utils.print_header("Reading Device IDs from Excel File")
    read_device_id_file_start_time = time.time()
    device_map = files.read_device_ids(setup.EXCEL_INPUT_FILE)
    if not device_map:
        return

    read_device_id_file_end_time = time.time()
    read_device_id_file_duration = (
        read_device_id_file_end_time - read_device_id_file_start_time
    )

    print(
        f"⏱  Time taken to read device IDs: {read_device_id_file_duration:.2f} seconds"
    )
    print(f"✔  Found {len(device_map)} device IDs to process\n")

    time.sleep(2)

    utils.print_header("Devices Found")

    for serial, device_name in device_map.items():
        print(f"• {device_name} ({serial})")

    time.sleep(2)

    # =============================================================================
    # STEP 3: CREATE SESSION
    # =============================================================================
    utils.print_header("Creating API Session")
    session = client.create_session()
    if not session:
        print("✖  Error creating API session")
        return

    print("✔  API session created successfully")

    time.sleep(2)

    # =============================================================================
    # STEP 4: FETCH ACCESS TOKEN
    # =============================================================================
    utils.print_header("Fetching Access Token")
    try:
        get_access_token_start_time = time.time()
        access_token = auth.get_access_token(session)
        get_access_token_end_time = time.time()
        get_access_token_duration = (
            get_access_token_end_time - get_access_token_start_time
        )
        print(
            f"⏱  Time taken to fetch access token: {get_access_token_duration:.2f} seconds"
        )
        print("✔  Access token fetched successfully")
        print(f"🔑 Access Token: {access_token}")
    except Exception as e:
        print(f"✖  Error fetching access token: {e}")
        return

    time.sleep(2)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting gracefully...")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
    finally:
        if signal_handler.shutdown_requested:
            print("\nShutdown requested, cleaning up resources...")
        else:
            print("\nExiting normally.")
