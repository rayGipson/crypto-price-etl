# crypto-price-etl
## Environment Configuration

1. Copy the `.env.template` to `.env`
   ```bash
   cp .env.template .env
   ```

2. Edit the `.env` file with your specific configurations
   ```bash
   # Open in your preferred text editor
   nano .env
   # or
   vim .env
   ```

### Configuration Guidelines

- **Database**: Provide your PostgreSQL credentials
- **API**: Adjust API-related settings if needed
- **Logging**: Modify logging level and format
- **ETL**: Set cryptocurrency limit and other pipeline parameters

#### Security Notes
- Never commit your `.env` file to version control
- Keep sensitive information like passwords confidential
- Use strong, unique passwords
- Consider using secret management tools in production

### Example `.env` File
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=crypto_prices
DB_USER=myuser
DB_PASSWORD=mysecurepassword

API_TIMEOUT=15
LOG_LEVEL=DEBUG

ETL_TOP_CURRENCIES_LIMIT=100
```