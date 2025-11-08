import requests
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Basic database connection
DATABASE_URL = "postgresql://usr:pwd@localhost/crypto_prices"
engine = sa.create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Basic database model
def create_crypto_prices_table():
    metadata = sa.MetaData()
    crypto_prices = sa.Table(
        'crypto_prices', 
        metadata,
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('name', sa.String),
        sa.Column('symbol', sa.String),
        sa.Column('current_price', sa.Float),
        sa.Column('market_cap', sa.Float),
        sa.Column('timestamp', sa.DateTime, default=datetime.utcnow)
    )
    metadata.create_all(engine)
    return crypto_prices

# Simple API extraction
def extract_top_cryptocurrencies(limit=10):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": limit,
        "page": 1,
        "sparkline": False
    }
    
    response = requests.get(url, params=params)
    return response.json()

# Basic transformation
def transform_crypto_data(raw_data):
    transformed_data = []
    for item in raw_data:
        transformed_item = {
            'name': item['name'],
            'symbol': item['symbol'],
            'current_price': item['current_price'],
            'market_cap': item['market_cap'],
            'timestamp': datetime.utcnow()
        }
        transformed_data.append(transformed_item)
    return transformed_data

# Simple load function
def load_to_database(transformed_data):
    crypto_prices = create_crypto_prices_table()
    
    with SessionLocal() as session:
        # Insert multiple records
        session.execute(
            crypto_prices.insert(), 
            transformed_data
        )
        session.commit()

# Main ETL pipeline
def run_etl_pipeline(limit=10):
    print(f"Starting ETL pipeline for top {limit} cryptocurrencies")
    
    # Extract
    raw_data = extract_top_cryptocurrencies(limit)
    print(f"Extracted {len(raw_data)} cryptocurrencies")
    
    # Transform
    transformed_data = transform_crypto_data(raw_data)
    print(f"Transformed {len(transformed_data)} cryptocurrencies")
    
    # Load
    load_to_database(transformed_data)
    print("Data loaded successfully")

# Entry point
if __name__ == "__main__":
    run_etl_pipeline()
