# Rockefeller

A high-performance ETL pipeline for financial data extraction, combining the power of Yahoo Finance with PostgreSQL for robust data storage and analysis. Built with Python 3.12+, this project features efficient parallel processing, comprehensive error handling, and flexible DataFrame manipulation using both Pandas and Polars.

## Key Features

- **Parallel Data Processing**: Efficiently handles multiple financial trackers simultaneously
- **Dual DataFrame Support**: Seamless integration with both Pandas and Polars
- **Environment-Aware**: Separate dev/prod configurations for safe development
- **Robust Error Handling**: Comprehensive logging and graceful failure recovery
- **Automated Verification**: Built-in tools to verify tracker validity
- **Column Normalization**: Automated cleanup of DataFrame columns across libraries

## Technical Requirements

- Python 3.12.8 (recommended) or Python 3.13.1 (tested but may have library compatibility issues)
- PostgreSQL database
- Required Python packages (see [Dependencies](#dependencies))

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/rockefeller.git
cd rockefeller
```

2. Set up a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create and configure your `.env` file:
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

## Quick Start

### Database Operations
```python
from tools.db_connect import DatabaseConnect

# Initialize connection
db = DatabaseConnect()

# Verify connection
if db.test_connection():
    print("Successfully connected to database!")

# Use the connection
try:
    db.connect()
    # Your database operations here
finally:
    db.disconnect()
```

### DataFrame Processing
```python
from tools.df_manipulation import ReadyDF
import pandas as pd
import polars as pl

# Using with Pandas
df_pd = pd.DataFrame({'Column Name!': [1, 2]})
normalized_pd = ReadyDF.normalize(df_pd)

# Using with Polars
df_pl = pl.DataFrame({'Column Name!': [1, 2]})
normalized_pl = ReadyDF.normalize(df_pl)

# Direct method on DataFrame (after importing ReadyDF)
df = pd.DataFrame({'Column Name!': [1, 2]})
normalized = df.normalize()
```

## Project Structure
```
rockefeller/
├── airflow/
│   └── dags/
│       └── daily_dag.py       # Airflow DAG (WIP)
├── scripts/
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── db_connect.py      # Database management
│   │   └── df_manipulation.py # DataFrame utilities
│   ├── collector.py           # Data collection
│   └── verify.py              # Tracker verification
├── .env                       # Environment variables
├── .gitignore
├── README.md
└── requirements.txt
```

## Core Components

### DatabaseConnect
Manages database connections with environment-specific configurations:
- Automatic environment detection
- Connection pooling
- Secure credential management
- Comprehensive logging
- Schema-specific configurations

### ReadyDF
Provides unified DataFrame processing across Pandas and Polars:
- Automatic type detection
- Column name normalization
- Method injection for direct usage
- Comprehensive error handling
- Performance-optimized processing

### Collector
Handles data collection with:
- Parallel processing support
- Automatic retry mechanisms
- Granular error handling
- Progress tracking
- Memory-efficient processing

## Dependencies

Core dependencies:
- pandas
- polars
- psycopg2-binary
- python-dotenv
- yfinance

## Disclaimer

This project is for educational and research purposes only. Please ensure compliance with Yahoo Finance's terms of service when using their data.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE.md) file for details.

## Contact

For questions and feedback:
- Create an issue in the repository
