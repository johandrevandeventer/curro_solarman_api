"""
Main script for the project.
Author: Johandré van Deventer
Date: 2025-06-13
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import time
from zoneinfo import ZoneInfo
from tqdm import tqdm

from signal_handler import signal_handler
from utils import utils, dates, files
from config import setup, env
from api import auth, client, data
from models import process_api


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

    # =============================================================================
    # STEP 5: PREPARE DATE RANGES
    # =============================================================================
    utils.print_header("Preparing Date Ranges for Data Collection")

    TZ = ZoneInfo(setup.TIMEZONE)
    date_ranges = dates.get_daily_unix_ranges(
        setup.START_DATE, setup.END_DATE, tz_name=setup.TIMEZONE
    )
    start_unix = date_ranges[0][0]
    end_unix = date_ranges[-1][1]
    start_date_str = datetime.fromtimestamp(start_unix, TZ).strftime("%Y-%m-%d")
    end_date_str = datetime.fromtimestamp(end_unix, TZ).strftime("%Y-%m-%d")

    print(f"• Date range: {start_unix} to {end_unix}")
    print(f"• Date range: {start_date_str} to {end_date_str}")
    print(f"• Total days to process: {len(date_ranges)}")

    time.sleep(2)

    # =============================================================================
    # STEP 6: START DATA COLLECTION
    # =============================================================================
    utils.print_header("Starting Data Collection")

    time.sleep(1)

    month_ranges = dates.group_unix_ranges_by_month(date_ranges)

    total_records = 0

    for month, month_range in month_ranges.items():
        month_year_str = dates.get_month_year_string(month_range[0][0])

        print(f"Processing month: {month_year_str} ({len(month_range)} days)")

        devices_bar = tqdm(
            total=len(device_map),
            desc="Devices",
            unit="device",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]",
            position=0,
            leave=False,
        )

        for serial, device_name in device_map.items():
            record_count = 0  # Track number of records processed
            total_processing_time = 0  # Track total time spent processing
            avg_time_per_day_total = 0  # Track average time per day

            if signal_handler.shutdown_requested:
                print("\nShutdown requested, exiting data collection loop...")
                return

            with ThreadPoolExecutor(max_workers=30) as executor:
                futures = {
                    executor.submit(
                        data.fetch_device_data,
                        session,
                        access_token,
                        serial,
                        start_unix,
                        end_unix,
                    ): (start_unix, end_unix)
                    for start_unix, end_unix in month_range
                }

                device_bar = tqdm(
                    total=len(futures),
                    desc=f"Collecting {device_name[:15]}... ({serial})",
                    unit="day",
                    leave=False,
                    position=1,
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]",
                )

                for future in as_completed(futures):
                    if signal_handler.shutdown_requested:
                        print("\nShutdown requested, exiting data collection loop...")
                        return

                    result = future.result()

                    if (
                        result
                        and isinstance(result, dict)
                        and "paramDataList" in result
                    ):
                        df = process_api.process_api_response(result["paramDataList"])

                        if not df.empty:
                            record_count += len(df)
                            total_records += record_count
                            processing_time = time.time() - start_time
                            total_processing_time += processing_time
                            avg_time_per_day_total = total_processing_time / (
                                record_count or 1
                            )

                            raw_output_dir = Path(setup.RAW_CSV_OUTPUT)
                            raw_output_dir.mkdir(parents=True, exist_ok=True)

                            raw_month_dir = raw_output_dir / month
                            raw_month_dir.mkdir(parents=True, exist_ok=True)

                            df.to_csv(
                                raw_month_dir / f"{device_name}_{serial}.csv",
                                index=False,
                            )

                    device_bar.update(1)
                    device_bar.set_postfix(
                        {
                            "Records": record_count,
                            "Avg Time/Day": f"{avg_time_per_day_total:.2f}s",
                        }
                    )

            devices_bar.update(1)
            tqdm.write(
                f"✔  Completed collecting {device_name} ({serial}) for month: {month_year_str}. Records: {record_count}, Avg Time/Day: {avg_time_per_day_total:.2f}s"
            )

        devices_bar.set_postfix(
            {"Month": month_year_str, "Total Devices": len(device_map)}
        )
        devices_bar.close()

    total_time = time.time() - start_time
    utils.print_header("Data Collection Completed")
    print(f"✔  Total records collected: {total_records}")
    print(f"✔  Total time taken: {total_time:.2f} seconds")

    #  =============================================================================
    # STEP 7: CLEAN UP
    # =============================================================================

    utils.print_header("Cleaning up raw data files")


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
