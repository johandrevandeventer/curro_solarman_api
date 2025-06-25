# shutdown/shutdown_controller.py

"""Module to handle graceful shutdown of a Python application.
This module provides functionality to register cleanup handlers that will be
called when a shutdown signal is received (like SIGINT or SIGTERM).
Author: Johandré van Deventer
Date: 2025-06-13
"""

import signal
import threading
from typing import Callable

_shutdown_requested = threading.Event()
_cleanup_handlers = []


def is_shutdown_requested() -> bool:
    """Check if shutdown was triggered."""
    return _shutdown_requested.is_set()


def register_cleanup_handler(handler: Callable[[], None]):
    """Add a cleanup function to be called on shutdown."""
    _cleanup_handlers.append(handler)


def _trigger_shutdown(signum=None, frame=None):
    """Internal: Signal handler that initiates shutdown."""
    if not _shutdown_requested.is_set():
        _shutdown_requested.set()
        # print("\nShutdown requested. Running cleanup handlers...", flush=True)
        for handler in _cleanup_handlers:
            try:
                handler()
            except Exception as e:
                print(f"Cleanup error: {e}")


# Register for SIGINT (Ctrl+C) and SIGTERM (kill)
signal.signal(signal.SIGINT, _trigger_shutdown)
signal.signal(signal.SIGTERM, _trigger_shutdown)
