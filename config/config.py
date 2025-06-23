"""
Configuration settings for the project with precise error handling.
Author: Johandré van Deventer
Date: 2025-06-13
"""

import os
import threading
from pathlib import Path
from typing import Dict, Any, Optional
import yaml
from dotenv import load_dotenv

_config_lock = threading.Lock()
_app_config: Optional[Dict[str, Any]] = None


class ConfigError(Exception):
    """Base exception for configuration-related errors"""

    pass


class ConfigValidationError(ConfigError):
    """Exception for configuration validation failures"""

    pass


class ConfigFileError(ConfigError):
    """Exception for configuration file issues"""

    pass


# Load environment variables first (before any config loading)
def _init_environment(env_path: str = ".env") -> bool:
    """
    Load environment variables from .env file

    Args:
        env_path: Path to .env file

    Returns:
        bool: True if loaded successfully, False otherwise
    """
    env_file = Path(env_path)
    if env_file.exists():
        load_dotenv(env_file)
        return True
    return False


# Initialize environment at module import
_init_environment()


def _load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """
    Load and parse the YAML configuration file

    Args:
        config_path: Path to the configuration file

    Returns:
        Parsed configuration dictionary

    Raises:
        ConfigFileError: If file is missing or unreadable
        ConfigError: If YAML parsing fails
    """
    config_file = Path(config_path)

    if not config_file.exists():
        raise ConfigFileError(f"Config file not found at {config_path}")
    if not config_file.is_file():
        raise ConfigFileError(f"Config path is not a file: {config_path}")

    try:
        with config_file.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in config file: {str(e)}") from e
    except OSError as e:
        raise ConfigFileError(f"Error reading config file: {str(e)}") from e


def _validate_config(config: Dict[str, Any]):
    """
    Validate the configuration structure

    Args:
        config: Configuration dictionary to validate

    Raises:
        ConfigValidationError: If validation fails
    """
    if not isinstance(config, dict):
        raise ConfigValidationError("Config must be a dictionary")

    required_sections = ["devices", "date_range", "http_settings", "output"]
    missing = [section for section in required_sections if section not in config]
    if missing:
        raise ConfigValidationError(
            f"Missing required config sections: {', '.join(missing)}"
        )

    if not isinstance(config["devices"], list):
        raise ConfigValidationError("'devices' must be a list")

    if not config["devices"]:
        raise ConfigValidationError("No devices specified in config")


def _apply_env_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply environment variable overrides to config

    Args:
        config: Configuration dictionary to modify

    Returns:
        Modified configuration dictionary
    """
    if "http_settings" not in config:
        config["http_settings"] = {}

    if os.getenv("API_BASE_URL"):
        config["http_settings"]["base_url"] = os.getenv("API_BASE_URL")

    if os.getenv("API_TOKEN"):
        if "headers" not in config["http_settings"]:
            config["http_settings"]["headers"] = {}
        config["http_settings"]["headers"][
            "Authorization"
        ] = f"Bearer {os.getenv('API_TOKEN')}"

    if os.getenv("API_APP_ID"):
        config["http_settings"]["app_id"] = os.getenv("API_APP_ID")

    if os.getenv("API_APP_SECRET"):
        config["http_settings"]["app_secret"] = os.getenv("API_APP_SECRET")

    if os.getenv("API_EMAIL"):
        config["http_settings"]["email"] = os.getenv("API_EMAIL")

    if os.getenv("API_PASSWORD"):
        config["http_settings"]["password"] = os.getenv("API_PASSWORD")

    if os.getenv("API_ORG_ID"):
        config["http_settings"]["org_id"] = os.getenv("API_ORG_ID")

    return config


def get_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """
    Get the application configuration

    Args:
        config_path: Path to the configuration file

    Returns:
        Loaded and validated configuration dictionary

    Raises:
        ConfigError: If any configuration operation fails
    """
    global _app_config

    with _config_lock:
        if _app_config is None:
            try:
                config = _load_config(config_path)
                _validate_config(config)
                _app_config = _apply_env_overrides(config)
            except (ConfigFileError, ConfigValidationError, ConfigError) as e:
                raise ConfigError(
                    f"Configuration initialization failed: {str(e)}"
                ) from e

        return _app_config.copy()  # Return copy to prevent accidental modification


def reload_config(config_path: str = "config.yaml"):
    """
    Reload the configuration from file

    Args:
        config_path: Path to the configuration file

    Raises:
        ConfigError: If reloading fails
    """
    global _app_config

    with _config_lock:
        try:
            config = _load_config(config_path)
            _validate_config(config)
            _app_config = _apply_env_overrides(config)
        except (ConfigFileError, ConfigValidationError, ConfigError) as e:
            raise ConfigError(f"Configuration reload failed: {str(e)}") from e
