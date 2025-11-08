from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

def test_database_connection():
    try:
        # Use the simplified local development connection
        engine = create_engine("postgresql:///crypto_prices")
        
        with engine.connect() as connection:
            # Simple query to test connection
            result = connection.execute(text("SELECT 1"))
            print("Database connection successful!")
            print("Test query result:", list(result)[0][0])
    
    except SQLAlchemyError as e:
        print(f"Database connection failed: {e}")

if __name__ == "__main__":
    test_database_connection()
