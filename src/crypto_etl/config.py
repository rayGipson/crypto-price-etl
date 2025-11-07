from pathlib import Path 
from typing import Dict, Any
import yaml
from pydantic import BaseModel, Field, ValidationError, ValidationError
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

class DatabaseConfig(BaseModel):
    host: str = Field(default='localhost', env='DB_HOST')
    port: int = Field(default=5432, env='DB_PORT')
    name: str = Field(env='DB_NAME')
    user: str = Field(env='DB_USER')
    password: str = Field(env='DB_PASSWORD')

    def get_connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

class ApiConfig(BaseModel):
    base_url: str = 'https://api.coingecko.com/api/v3'
    timeout: int = 10
    retry_attempts: int = 3
    retry_delay: int = 5

class LoggingConfig(BaseModel):
    level: str = 'INFO'
    format: str = 'json'

class AppConfig(BaseModel):
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
            # if config file exists, load it
            if config_path.exists():
                with open(config_path, 'r') as f:
                    config_dict = yaml.safe_load(f) of {}
            else:
                config_dict = {}

            
            # Override with environment variables
            return cls(**config_dict)
        except (yaml.YAMLError, ValidationError) as e:
            print(f"Configuration error: {e}")
            raise

# Singleton configuration instance
config = AppConfig.load()