"""
Signal handler for graceful shutdown of long-running processes.
Author: Johandré van Deventer
Date: 2025-06-13
"""

import signal
import time
import os

shutdown_requested = False


def handle_signal(sig, frame):
    """Handle interrupt signals gracefully."""
    global shutdown_requested
    print("\nReceived shutdown signal, finishing current operations...")
    shutdown_requested = True
    time.sleep(1)
    os._exit(1)


def setup_signal_handler():
    """Set up signal handlers for graceful shutdown."""
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
