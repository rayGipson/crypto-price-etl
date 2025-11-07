from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import pandas as pd

from .config import config
from .logger import logger

# Create SQLAlchemy base and engine
Base = declarative_base()
engine = create_engine(config.database.get_connection_string())
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class CryptoPriceModel(Base):
    """SQLAlchemy model for storing cryptocurrency prices"""
    __tablename__ = 'crypto_prices'

    id = Column(Integer, primary_key=True, index=True)
    coin_id = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    name = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    market_cap = Column(Float)
    market_cap_rank = Column(Integer)
    fully_diluted_valuation = Column(Float)
    total_volume = Column(Float)
    high_24h = Column(Float)
    low_24h = Column(Float)
    price_change_24h = Column(Float)
    price_change_percentage_24h = Column(Float)
    market_cap_change_24h = Column(Float)
    market_cap_change_percentage_24h = Column(Float)
    circulating_supply = Column(Float)
    total_supply = Column(Float)
    max_supply = Column(Float)
    ath = Column(Float)
    ath_change_percentage = Column(Float)
    ath_date = Column(DateTime)
    atl = Column(Float)
    atl_change_percentage = Column(Float)
    atl_date = Column(DateTime)
    timestamp = Column(DateTime, default=datetime.utcnow)

class DatabaseManager:
    """Manages database operations for cryptocurrency data"""

    @staticmethod
    def create_tables():
        """Create all tables in the database"""
        try:
            Base.metadata.create_all(bind=engine)
            logger.info("Database tables created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Error creating database tables: {e}")
            raise

    @staticmethod
    def insert_crypto_prices(crypto_data: list):
        """
        Insert cryptocurrency price data into the database
        
        Args:
            crypto_data (list): List of cryptocurrency price dictionaries
        """
        session = SessionLocal()
        try:
            # Convert API data to SQLAlchemy model instances
            price_models = [
                CryptoPriceModel(
                    coin_id=item.get('id'),
                    symbol=item.get('symbol'),
                    name=item.get('name'),
                    price=item.get('current_price'),
                    market_cap=item.get('market_cap'),
                    market_cap_rank=item.get('market_cap_rank'),
                    fully_diluted_valuation=item.get('fully_diluted_valuation'),
                    total_volume=item.get('total_volume'),
                    high_24h=item.get('high_24h'),
                    low_24h=item.get('low_24h'),
                    price_change_24h=item.get('price_change_24h'),
                    price_change_percentage_24h=item.get('price_change_percentage_24h'),
                    market_cap_change_24h=item.get('market_cap_change_24h'),
                    market_cap_change_percentage_24h=item.get('market_cap_change_percentage_24h'),
                    circulating_supply=item.get('circulating_supply'),
                    total_supply=item.get('total_supply'),
                    max_supply=item.get('max_supply'),
                    ath=item.get('ath'),
                    ath_change_percentage=item.get('ath_change_percentage'),
                    ath_date=datetime.fromisoformat(item.get('ath_date')) if item.get('ath_date') else None,
                    atl=item.get('atl'),
                    atl_change_percentage=item.get('atl_change_percentage'),
                    atl_date=datetime.fromisoformat(item.get('atl_date')) if item.get('atl_date') else None
                ) for item in crypto_data
            ]

            # Bulk insert
            session.add_all(price_models)
            session.commit()
            
            logger.info(f"Inserted {len(price_models)} cryptocurrency prices")
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error inserting cryptocurrency prices: {e}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_crypto_prices_as_dataframe():
        """
        Retrieve cryptocurrency prices as a pandas DataFrame
        
        Returns:
            pandas.DataFrame: DataFrame containing cryptocurrency prices
        """
        session = SessionLocal()
        try:
            # Query all prices and convert to DataFrame
            query = session.query(CryptoPriceModel).all()
            df = pd.read_sql(session.query(CryptoPriceModel).statement, session.bind)
            return df
        except SQLAlchemyError as e:
            logger.error(f"Error retrieving cryptocurrency prices: {e}")
            raise
        finally:
            session.close()