"""
CoinGecko Cryptocurrency Price ETL Pipeline

This module provides a basic Extract, Transform, Load (ETL) pipeline 
for retrieving top cryptocurrency prices from the CoinGecko API 
and storing them in a PostgreSQL database.

Features:
- API data extraction
- Basic data transformation
- Database loading
"""

import requests
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from typing import List, Dict, Any

from .config import settings

# Database connection using settings
engine = sa.create_engine("postgresql:///crypto_prices")
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def create_crypto_prices_table():
    """
    Create the crypto_prices table if it doesn't exist.
    
    Returns:
        SQLAlchemy Table object for crypto_prices
    """
    metadata = sa.MetaData()
    crypto_prices = sa.Table(
        'crypto_prices', 
        metadata,
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('name', sa.String, nullable=False),
        sa.Column('symbol', sa.String, nullable=False),
        sa.Column('current_price', sa.Float, nullable=False),
        sa.Column('market_cap', sa.Float),
        sa.Column('timestamp', sa.DateTime, default=datetime.utcnow)
    )
    metadata.create_all(engine)
    return crypto_prices

def extract_top_cryptocurrencies(limit: int = None) -> List[Dict[str, Any]]:
    """
    Extract top cryptocurrencies from CoinGecko API.
    
    Args:
        limit (int, optional): Number of cryptocurrencies to fetch. 
                                Defaults to settings value.
    
    Returns:
        List of cryptocurrency data dictionaries
    """
    url = f"{settings.api.base_url}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": limit or settings.top_currencies_limit,
        "page": 1,
        "sparkline": False
    }
    
    response = requests.get(url, params=params, timeout=settings.api.timeout)
    response.raise_for_status()  # Raise exception for bad responses
    return response.json()

def transform_crypto_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform raw cryptocurrency data for database insertion.
    
    Args:
        raw_data (List[Dict]): Raw cryptocurrency data from API
    
    Returns:
        List of transformed cryptocurrency data
    """
    return [
        {
            'name': item['name'],
            'symbol': item['symbol'],
            'current_price': item['current_price'],
            'market_cap': item['market_cap'],
            'timestamp': datetime.utcnow()
        }
        for item in raw_data
    ]

def load_to_database(transformed_data: List[Dict[str, Any]]):
    """
    Load transformed cryptocurrency data into PostgreSQL database.
    
    Args:
        transformed_data (List[Dict]): Transformed cryptocurrency data
    """
    crypto_prices = create_crypto_prices_table()
    
    with SessionLocal() as session:
        # Bulk insert records
        session.execute(
            crypto_prices.insert(), 
            transformed_data
        )
        session.commit()

def run_etl_pipeline(limit: int = None):
    """
    Execute complete ETL pipeline for cryptocurrency prices.
    
    Args:
        limit (int, optional): Number of cryptocurrencies to process
    """
    # Extract
    raw_data = extract_top_cryptocurrencies(limit)
    
    # Transform
    transformed_data = transform_crypto_data(raw_data)
    
    # Load
    load_to_database(transformed_data)

# Entry point for direct script execution
if __name__ == "__main__":
    run_etl_pipeline()