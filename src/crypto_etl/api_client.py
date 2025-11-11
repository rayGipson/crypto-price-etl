from typing import Dict, List, Any, Optional
import time
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError

from .config import config
from .logger import logger

class CoinGeckoAPIClient:
    def __init__(
        self, 
        base_url: str = config.api.base_url, 
        timeout: int = config.api.timeout,
        retry_attempts: int = config.api.retry_attempts,
        retry_delay: int = config.api.retry_delay
    ):
        """
        Initialize CoinGecko API client with configurable retry and timeout settings
        
        Args:
            base_url (str): Base URL for CoinGecko API
            timeout (int): Request timeout in seconds
            retry_attempts (int): Number of retry attempts
            retry_delay (int): Delay between retry attempts
        """
        self.base_url = base_url
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.session = requests.Session()

    def _make_request(
        self, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make a robust request to CoinGecko API with retry logic
        
        Args:
            endpoint (str): API endpoint
            params (dict, optional): Query parameters
        
        Returns:
            Dict containing API response
        
        Raises:
            RequestException: If all retry attempts fail
        """
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(1, self.retry_attempts + 1):
            try:
                logger.info(f"API Request", 
                    url=url, 
                    attempt=attempt, 
                    params=params
                )
                
                response = self.session.get(
                    url, 
                    params=params, 
                    timeout=self.timeout
                )
                
                response.raise_for_status()  # Raise exception for bad status codes
                
                return response.json()
            
            except (Timeout, ConnectionError, RequestException) as e:
                logger.warning(
                    "API Request Failed", 
                    error=str(e), 
                    attempt=attempt
                )
                
                if attempt == self.retry_attempts:
                    logger.error(
                        "All API request attempts failed", 
                        error=str(e)
                    )
                    raise
                
                time.sleep(self.retry_delay)

    def get_top_cryptocurrencies(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Retrieve top cryptocurrencies by market cap
        
        Args:
            limit (int): Number of cryptocurrencies to retrieve
        
        Returns:
            List of cryptocurrency dictionaries
        """
        endpoint = "coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": limit,
            "page": 1,
            "sparkline": False
        }
        
        return self._make_request(endpoint, params)

    def get_historical_price(
        self, 
        coin_id: str, 
        date: str
    ) -> Dict[str, float]:
        """
        Retrieve historical price for a specific cryptocurrency on a given date
        
        Args:
            coin_id (str): CoinGecko coin identifier
            date (str): Date in format 'dd-mm-yyyy'
        
        Returns:
            Dictionary with price information
        """
        endpoint = f"coins/{coin_id}/history"
        params = {
            "date": date,
            "localization": False
        }
        
        return self._make_request(endpoint, params)

# Client instance
api_client = CoinGeckoAPIClient()