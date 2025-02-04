# Tools Directory

This directory contains essential utility modules that provide the foundation for the Rockefeller project's functionality. Each module is designed with specific responsibilities and integrates seamlessly with the main scripts.

## Database Connection (db_connect.py)

The DatabaseConnect class provides a robust and secure way to interact with our PostgreSQL database across different environments.

### Key Features

The module implements several sophisticated features for database management:

1. Environment-Aware Configuration
   ```python
   # Environment detection and setup
   env = os.getenv("DB_MODE", "dev")  # Defaults to dev if not specified
   ```

2. Secure Credential Management
   ```python
   # Example .env configuration
   DB_USERNAME_PROD=your_username
   DB_PASSWORD_PROD=your_password
   ```

3. Connection Lifecycle Management
   ```python
   db = DatabaseConnect()
   try:
       db.connect()
       # Perform database operations
   finally:
       db.disconnect()  # Ensures proper cleanup
   ```

## DataFrame Manipulation (df_manipulation.py)

The ReadyDF class provides a comprehensive system for DataFrame manipulation, supporting both pandas and polars DataFrames. The module extends both DataFrame libraries with standardization and finalization capabilities.

### Key Features

1. Multiple Usage Patterns
   ```python
   # As instance methods
   df = df.normalize()
   
   # As pandas module functions
   df = pd.normalize(df)
   
   # As polars module functions
   df = pl.normalize(df)
   ```

2. Column Standardization
   ```python
   # Converts 'Column Name!' to 'column_name'
   df = df.normalize()
   ```
   ```

### Best Practices

When working with DataFrame manipulation:
1. Always normalize column names before finalization
2. Remember that only 'tracker' and 'date' fields are required
3. Handle None values appropriately in optional fields

## Standards Module (standards.py)

The standards module centralizes table name management and provides consistent naming conventions across the project.

### Key Features

1. Table Name Management
   ```python
   from tools.standards import DEFAULT_TABLES
   
   dim_trackers = DEFAULT_TABLES["DIM_TRACKERS"]
   fct_trackers = DEFAULT_TABLES["FCT_TRACKERS"]
   ```

2. Type Safety
   ```python
   def dim_trackers() -> str:
       return "dim_trackers"
   ```

## Integration Examples

Here's how these tools work together:

```python
from tools.db_connect import DatabaseConnect
from tools.standards import DEFAULT_TABLES
import pandas as pd

# Read and prepare data
df = pd.DataFrame(your_data)
df = df.normalize()

# Write to database
db = DatabaseConnect()
try:
    db.connect()
    query = f"INSERT INTO {DEFAULT_TABLES['FCT_TRACKERS']} ..."
    # Your database operations here
finally:
    db.disconnect()
```

## Error Handling

Each module implements comprehensive error handling:

1. Database Errors
   ```python
   try:
       db.connect()
   except Exception as e:
       logger.error(f"Database connection failed: {str(e)}")
       raise
   ```

2. DataFrame Processing Errors
   ```python
   try:
       df = df.normalize()
   except ValueError as e:
       logger.error(f"Normalization failed: {str(e)}")
       raise
   ```

For information about how these tools are used in practice, please refer to the README in the scripts directory.