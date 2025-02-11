# Rockefeller

A high-performance ETL pipeline for financial data extraction, combining the power of Yahoo Finance with PostgreSQL for robust data storage and analysis. Built with Python 3.12+, this project features efficient parallel processing, comprehensive error handling, and flexible DataFrame manipulation using both Pandas and Polars.

## Key Features

- **Parallel Data Processing**: Efficiently handles multiple financial trackers simultaneously
- **Dual DataFrame Support**: Seamless integration with both Pandas and Polars
- **Environment-Aware**: Separate dev/prod configurations for safe development
- **Robust Error Handling**: Comprehensive logging and graceful failure recovery
- **Automated Verification**: Built-in tools to verify tracker validity
- **Column Normalization**: Automated cleanup of DataFrame columns across libraries
- **Flexible DataFrame Operations**: Support for both method chaining and functional approaches

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

### ReadyDF
Provides unified DataFrame processing across Pandas and Polars:
- Multiple usage patterns (instance methods and module functions)
- Automatic type detection
- Column name normalization
- Method injection for direct usage
- Comprehensive error handling
- Performance-optimized processing

### Standards
Centralizes database table name management with:
- Consistent table naming
- Type-safe access
- Default table mapping

## Dependencies

Core dependencies:
- pandas
- polars
- numpy
- psycopg2-binary
- python-dotenv
- yfinance

## Disclaimer

This project is for educational and research purposes only. Please ensure compliance with Yahoo Finance's terms of service when using their data.

## Contributions

This is a personal project and thus I will not be accepting any contributions. Thank you for understanding. :)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE.md) file for details.

## Contact

For questions and feedback:
- Create an issue in the repository