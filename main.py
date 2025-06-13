"""
Main script for the project.
Author: Johandré van Deventer
Date: 2025-06-13
"""

from datetime import datetime
from time import time
from signal_handler import signal_handler
from utils import utils


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
