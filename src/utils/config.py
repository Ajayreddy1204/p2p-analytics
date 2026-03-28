"""
Configuration Module
Manages application configuration from YAML files
"""

import os
import yaml
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class Config:
    """Application configuration manager"""

    def __init__(self, config_path: str = None):
        """
        Initialize configuration

        Args:
            config_path: Path to config file (default: config/development.yaml)
        """
        self.config = {}
        self.env = os.environ.get('APP_ENV', 'development')

        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                'config',
                f'{self.env}.yaml'
            )

        self.load_config(config_path)

    def load_config(self, config_path: str) -> Dict:
        """
        Load configuration from YAML file

        Args:
            config_path: Path to YAML config file

        Returns:
            Configuration dictionary
        """
        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)

            logger.info(f"Loaded configuration from {config_path}")
            return self.config

        except FileNotFoundError:
            logger.error(f"Config file not found: {config_path}")
            self.config = {}
            return {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing config file: {str(e)}")
            self.config = {}
            return {}

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key (supports dot notation)

        Args:
            key: Configuration key (e.g., 'database.host')
            default: Default value if key not found

        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default

        return value if value is not None else default

    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value

        Args:
            key: Configuration key (supports dot notation)
            value: Value to set
        """
        keys = key.split('.')
        config = self.config

        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]

        config[keys[-1]] = value

    def get_database_config(self) -> Dict:
        """
        Get database configuration

        Returns:
            Database configuration dictionary
        """
        return self.get('database', {})

    def get_aws_config(self) -> Dict:
        """
        Get AWS configuration

        Returns:
            AWS configuration dictionary
        """
        return self.get('aws', {})

    def get_redshift_config(self) -> Dict:
        """
        Get Redshift configuration

        Returns:
            Redshift configuration dictionary
        """
        return self.get('database.redshift', {})

    def get_aurora_config(self) -> Dict:
        """
        Get Aurora configuration

        Returns:
            Aurora configuration dictionary
        """
        return self.get('database.aurora', {})

    def reload(self, config_path: str = None) -> Dict:
        """
        Reload configuration from file

        Args:
            config_path: Path to config file

        Returns:
            Configuration dictionary
        """
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                'config',
                f'{self.env}.yaml'
            )

        return self.load_config(config_path)