# Yahoo Finance Data Pipeline

## Description
A Python-based ETL pipeline that extracts stock data from Yahoo Finance and loads it into a PostgreSQL database.
This project features Apache Airflow integration for automated data collection, along with dedicated tools for database management and data processing.
The architecture emphasizes modularity, data quality, and automated workflows through DAGs.

## Features

### Database Management (`DatabaseConnect` Class)
- Environment-aware configuration (dev/prod)
- Automated logging setup
- Secure credential management
- Connection pooling and lifecycle management
- Error handling
- Schema-specific configurations
- Connection testing capabilities

### Data Processing (`df_manipulate.py`)
- Dual support for Pandas and Polars DataFrames
- Column name normalization
- Comprehensive error handling and logging
- Performance-optimized regular expressions
- Type checking and validation

## Technical Architecture

### Database Connection (`database_connect.py`)

The `DatabaseConnect` class manages all database interactions with environment-specific configurations:

```python
class DatabaseConnect:
    def __init__(self):
        """
        Initialize database connection manager with environment-specific settings.
        Loads environment variables and sets up logging automatically.
        """

    def connect(self) -> bool:
        """
        Establish database connection with environment-specific credentials.
        Returns True on successful connection.
        """

    def disconnect(self) -> None:
        """
        Safely close database connections and cursors.
        """

    def test_connection(self) -> bool:
        """
        Verify database connectivity through test query.
        Returns True if connection is successful.
        """

    def setup_credentials(self) -> None:
        """
        Configure environment-specific database credentials.
        """
```

#### Environment Configuration
The class supports two environments:
- Development (`dev`): INFO level logging, development credentials
- Production (`prod`): DEBUG level logging, production credentials

#### Required Environment Variables
```bash
# Database Configuration
DB_MODE=dev|prod
DB_NAME=your_database_name
DB_HOST=your_host

# Development Credentials
DB_USERNAME_DEV=dev_username
DB_PASSWORD_DEV=dev_password
DB_SCHEMA_DEV=dev_schema

# Production Credentials
DB_USERNAME_PROD=prod_username
DB_PASSWORD_PROD=prod_password
DB_SCHEMA_PROD=prod_schema
```

### DataFrame Manipulation (`df_manipulate.py`)
Contains core data processing functionality:

```python
def normalize_pd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize and validate column names in a pandas DataFrame.
    
    Args:
        df (pd.DataFrame): The DataFrame to normalize.
    Returns:
        pd.DataFrame: The normalized DataFrame.
    Raises:
        TypeError: If the input is not a pandas DataFrame.
        ValueError: If the DataFrame is empty.
    """
```

```python
def normalize_pl(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalize and validate column names in a polars DataFrame.
    
    Args:
        df (pl.DataFrame): The DataFrame to normalize.
    Returns:
        pl.DataFrame: The normalized DataFrame.
    Raises:
        TypeError: If the input is not a polars DataFrame.
        ValueError: If the DataFrame is empty.
    """
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/rockefeller.git
cd rockefeller
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
# Create .env file
touch .env

# Add required variables (see Environment Variables section)
```

## Usage

### Basic Database Operations

```python
from database_connect import DatabaseConnect

# Initialize connection manager
db = DatabaseConnect()

# Test connection
db.test_connection():

# Establish connection for operations
db.connect()

try:
    # Perform database operations
    # ...
    pass
finally:
    # Always close connection
    db.disconnect()
```

### Data Processing

```python
import pandas as pd
import polars as pl
from df_manipulate import normalize_pd, normalize_pl

# Process Pandas DataFrame
df_pandas = pd.DataFrame({'Column Name!': [1, 2], 'Another@Column': [3, 4]})
normalized_pd_df = normalize_pd(df_pandas)

# Process Polars DataFrame
df_polars = pl.DataFrame({'Column Name!': [1, 2], 'Another@Column': [3, 4]})
normalized_pl_df = normalize_pl(df_polars)
```

### Complete ETL Pipeline

```python
from database_connect import DatabaseConnect
from df_manipulate import normalize_pd
import yfinance as yf

# Initialize database connection
db = DatabaseConnect()

try:
    # Fetch data
    ticker = yf.Ticker("AAPL")
    df = ticker.history(period="1mo")
    
    # Normalize data
    df_normalized = normalize_pd(df)
    
    # Connect to database
    db.connect()
    
    # Perform database operations
    # ... your code here ...
    
finally:
    # Clean up
    db.disconnect()
```

## Project Structure

```
project-root/
├── airflow/
│   └── dags/
│       └── daily_dag.py      # Airflow DAG for daily data collection <- WIP
├── scripts/
│   ├── collector.py          # Data collection utilities <- WIP
│   └── tools/
│       ├── __init__.py
│       ├── db_connect.py     # Database connection management
│       └── df_manipulation.py # DataFrame processing functions
├── .gitignore               # Git ignore configurations
└── requirements.txt         # Project dependencies
```

## Dependencies

### Core Dependencies
- pandas
- polars
- psycopg2-binary
- python-dotenv
- yfinance

## Contributing
This is a private project so no contributions will be accepted :) 

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Disclaimer

This project is for educational and research purposes only. Ensure compliance with Yahoo Finance's terms of service when using their data.

## Contact

For questions and feedback:
- Create an issue in the repository
