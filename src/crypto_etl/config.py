from pathlib import Path
from typing import Dict, Any
import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

class DatabaseConfig(BaseSettings):
    """
    Database configuration with enhanced flexibility
    """
    host: str = 'localhost'
    port: int = 5432
    name: str
    user: str
    password: str

    def get_connection_string(self) -> str:
        """
        Generate PostgreSQL connection string
        """
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    model_config = SettingsConfigDict(
        env_prefix='DB_',
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False
    )

class ApiConfig(BaseSettings):
    """
    API configuration settings
    """
    base_url: str = 'https://api.coingecko.com/api/v3'
    timeout: int = 10
    retry_attempts: int = 3
    retry_delay: int = 5

    model_config = SettingsConfigDict(
        env_prefix='API_',
        env_file='.env',
        case_sensitive=False
    )

class LoggingConfig(BaseSettings):
    """
    Logging configuration settings
    """
    level: str = 'INFO'
    format: str = 'json'

    model_config = SettingsConfigDict(
        env_prefix='LOG_',
        env_file='.env',
        case_sensitive=False
    )

class AppConfig(BaseSettings):
    """
    Comprehensive application configuration
    """
    database: DatabaseConfig = DatabaseConfig()
    api: ApiConfig = ApiConfig()
    logging: LoggingConfig = LoggingConfig()

    @classmethod
    def load(cls, config_path: Path = Path('config.yaml')) -> 'AppConfig':
        """
        Load configuration from YAML file with environment variable overrides

        Args:
            config_path (Path): Path to configuration file

        Returns:
            AppConfig: Validated configuration object
        """
        try: 
            # If config file exists, load it
            if config_path.exists():
                with open(config_path, 'r') as f:
                    config_dict = yaml.safe_load(f) or {}
            else:
                config_dict = {}
            
            # Override with environment variables and Pydantic Settings
            return cls(**config_dict)
        except (yaml.YAMLError) as e:
            print(f"Configuration error: {e}")
            raise

    model_config = SettingsConfigDict(
        env_file='.env',
        case_sensitive=False
    )

# Singleton configuration instance
config = AppConfig.load()
```

