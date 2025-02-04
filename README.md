# Rockefeller

A high-performance ETL pipeline for financial data extraction, combining the power of Yahoo Finance with PostgreSQL for robust data storage and analysis. Built with Python 3.12+, this project features efficient parallel processing, comprehensive error handling, and flexible DataFrame manipulation using both Pandas and Polars.

## Key Features

- **Parallel Data Processing**: Efficiently handles multiple financial trackers simultaneously
- **Dual DataFrame Support**: Seamless integration with both Pandas and Polars
- **Environment-Aware**: Separate dev/prod configurations for safe development
- **Robust Error Handling**: Comprehensive logging and graceful failure recovery
- **Automated Verification**: Built-in tools to verify tracker validity
- **Column Normalization**: Automated cleanup of DataFrame columns across libraries
- **Standardized Table Management**: Centralized table name management through standards module

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
source venv/bin activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create and configure your `.env` file:
```bash
# Database Configuration
DB_MODE= prod|dev|user
DB_NAME=
DB_HOST=

# Production Credentials
DB_USERNAME_PROD=
DB_PASSWORD_PROD=
DB_SCHEMA_PROD=

# Development Credentials <- Currently unsupported
DB_USERNAME_DEV=
DB_PASSWORD_DEV=
DB_SCHEMA_DEV=

# Public User Credentials <- Request access through Issues
DB_USERNAME_PUBLIC=
DB_PASSWORD_PUBLIC=
DB_SCHEMA_PUBLIC= 
```

## Quick Start

### Database Operations
```python
from tools.db_connect import DatabaseConnect
from tools.standards import DEFAULT_TABLES

# Initialize connection
db = DatabaseConnect()

# Access standardized table names
dim_trackers_table = DEFAULT_TABLES["DIM_TRACKERS"]
fct_trackers_table = DEFAULT_TABLES["FCT_TRACKERS"]

# Verify connection
if db.test_connection():
    print("Successfully connected to database!")

# Use the connection with standardized table names
try:
    db.connect()
    db.cursor.execute(f"SELECT COUNT(*) FROM {dim_trackers_table}")
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
│   │   ├── df_manipulation.py # DataFrame utilities
│   │   └── standards.py       # Table name standardization
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
- Integration with standardized table names

### ReadyDF
Provides unified DataFrame processing across Pandas and Polars:
- Automatic type detection
- Column name normalization
- Method injection for direct usage
- Comprehensive error handling
- Performance-optimized processing

### Standards
Centralizes database table name management:
- Consistent table naming across the application
- Function-based table name generation
- Default table mapping through DEFAULT_TABLES dictionary
- Type-safe table name access
- Simplified table name maintenance

### Collector
Handles data collection with:
- Parallel processing support
- Automatic retry mechanisms
- Granular error handling
- Progress tracking
- Memory-efficient processing

## Dependencies

Core dependencies:
- pandas>=2.0.0           # DataFrame manipulation
- polars>=0.20.0         # High-performance DataFrame operations
- numpy>=1.24.0          # Numerical computations and array operations
- psycopg2-binary>=2.9.0 # PostgreSQL database adapter
- python-dotenv>=1.0.0   # Environment variable management
- yfinance>=0.2.0        # Yahoo Finance data retrieval

## Disclaimer

This project is for educational and research purposes only. Please ensure compliance with Yahoo Finance's terms of service when using their data.

## Contributions

This is a personal project and thus I will not be accepting any contributions. Thank you for understanding. :)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE.md) file for details.

## Contact

For questions and feedback:
- Create an issue in the repository