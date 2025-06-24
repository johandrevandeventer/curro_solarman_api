# processor/checkpoint_manager.py

"""
Checkpoint Manager for Device Data Processing
This module manages the checkpointing system for tracking the progress of data collection
from devices, including the current device index, serial number, and dates processed.
Author: Johandré van Deventer
Date: 2025-06-13
"""

import pickle
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

from utils.utils import print_sub_header


class CheckpointManager:
    def __init__(self, checkpoint_file_path: str):
        self.checkpoint_file_path = checkpoint_file_path
        self.checkpoint_data = {
            "current_device_index": 0,
            "current_device_serial": None,
            "current_date": None,
            "completed_devices": set(),  # Track fully processed devices
            "completed_dates": dict(),  # {device_serial: set(dates)}
            "start_time": datetime.now().isoformat(),
            "last_updated": None,
            "version": "1.0",  # For future compatibility
        }

    def save_checkpoint(
        self, device_index: int, device_serial: str, current_date: str
    ) -> None:
        """Save current progress to checkpoint file with additional metadata."""
        # print_sub_header("Saving Checkpoint")

        # Update checkpoint data
        self.checkpoint_data.update(
            {
                "current_device_index": device_index,
                "current_device_serial": device_serial,
                "current_date": current_date,
                "last_updated": datetime.now().isoformat(),
            }
        )

        # Ensure directory exists
        file = Path(self.checkpoint_file_path)
        file.parent.mkdir(parents=True, exist_ok=True)

        # Atomic write operation
        temp_file = file.with_suffix(".tmp")
        try:
            with open(temp_file, "wb") as f:
                pickle.dump(self.checkpoint_data, f)
            temp_file.replace(file)  # Atomic rename
            # print(
            #     f"Checkpoint saved successfully: device {device_index}, serial {device_serial}, date {current_date}"
            # )
        except Exception as e:
            print(f"Failed to save checkpoint: {str(e)}")
            if temp_file.exists():
                temp_file.unlink()

    def mark_device_complete(self, device_serial: str) -> None:
        """Mark a device as fully processed."""
        self.checkpoint_data["completed_devices"].add(device_serial)
        self.save_checkpoint(
            self.checkpoint_data["current_device_index"],
            self.checkpoint_data["current_device_serial"],
            self.checkpoint_data["current_date"],
        )

    def mark_date_complete(self, device_serial: str, date: str) -> None:
        """Mark a specific date for a device as processed."""
        if device_serial not in self.checkpoint_data["completed_dates"]:
            self.checkpoint_data["completed_dates"][device_serial] = set()
        self.checkpoint_data["completed_dates"][device_serial].add(date)
        self.save_checkpoint(
            self.checkpoint_data["current_device_index"],
            self.checkpoint_data["current_device_serial"],
            self.checkpoint_data["current_date"],
        )

    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Load progress from checkpoint file if exists."""
        file = Path(self.checkpoint_file_path)

        if not file.exists():
            return None

        try:
            with open(file, "rb") as f:
                loaded_data = pickle.load(f)

            print_sub_header("Loading Checkpoint")
            print(
                f"Resuming from checkpoint created at {loaded_data.get('start_time')}"
            )
            print(f"Last updated at {loaded_data.get('last_updated')}")
            print(
                f"Resuming from device {loaded_data['current_device_index']}, "
                f"serial {loaded_data['current_device_serial']}, "
                f"date {loaded_data['current_date']}"
            )
            print(
                f"Completed devices: {len(loaded_data.get('completed_devices', set()))}"
            )

            # Merge with current checkpoint data (for new fields)
            self.checkpoint_data.update(loaded_data)
            return loaded_data
        except Exception as e:
            print(f"Error loading checkpoint: {str(e)}")
            return None

    def is_device_complete(self, device_serial: str) -> bool:
        """Check if a device has been fully processed."""
        return device_serial in self.checkpoint_data["completed_devices"]

    def is_date_complete(self, device_serial: str, date: str) -> bool:
        """Check if a specific date for a device has been processed."""
        return (
            device_serial in self.checkpoint_data["completed_dates"]
            and date in self.checkpoint_data["completed_dates"][device_serial]
        )
