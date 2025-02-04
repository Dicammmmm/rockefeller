# Tools Directory

This directory contains essential utility modules that provide the foundation for the Rockefeller project's functionality. Each module is designed with specific responsibilities and integrates seamlessly with the main scripts.

## Database Connection (db_connect.py)

The DatabaseConnect class provides a robust and secure way to interact with our PostgreSQL database across different environments.

### Key Features

The module implements several sophisticated features for database management:

1. Environment-Aware Configuration
   The connection manager automatically adapts to different environments:
   ```python
   # Environment detection and setup
   env = os.getenv("DB_MODE", "dev")  # Defaults to dev if not specified
   ```

2. Secure Credential Management
   Credentials are handled securely through environment variables:
   ```python
   # Example .env configuration
   DB_USERNAME_PROD=your_username
   DB_PASSWORD_PROD=your_password
   ```

3. Connection Lifecycle Management
   The module provides complete connection lifecycle handling:
   ```python
   db = DatabaseConnect()
   try:
       db.connect()
       # Perform database operations
   finally:
       db.disconnect()  # Ensures proper cleanup
   ```

### Best Practices

When working with DatabaseConnect, consider these recommendations:

1. Always use context management or try-finally blocks to ensure proper connection cleanup
2. Implement connection pooling for high-frequency operations
3. Use the test_connection() method before critical operations
4. Configure appropriate logging levels based on your environment

## DataFrame Manipulation (df_manipulation.py)

The ReadyDF class provides a unified interface for working with both pandas and polars DataFrames, focusing on data cleaning and standardization.

### Key Features

The module offers several sophisticated DataFrame manipulation capabilities:

1. Automatic Type Detection
   The normalize method intelligently handles different DataFrame types:
   ```python
   # Works with both pandas and polars
   normalized_df = ReadyDF.normalize(your_dataframe)
   ```

2. Column Standardization
   Implements consistent column naming conventions:
   - Converts to lowercase
   - Replaces special characters with underscores
   - Handles edge cases and duplicates

3. Method Injection
   Adds normalize method directly to DataFrame classes:
   ```python
   import pandas as pd
   from tools.df_manipulation import ReadyDF
   
   df = pd.DataFrame(...)
   normalized = df.normalize()  # Direct access to normalization
   ```

### Data Type Management

The module includes comprehensive data type handling:
```python
schema = {
    'tracker': 'string',
    'date': 'string',
    'open': 'float64',
    # ... additional fields
}
```

## Standards Module (standards.py)

The standards module centralizes table name management and provides consistent naming conventions across the project.

### Key Features

1. Table Name Management
   Provides function-based access to table names:
   ```python
   from tools.standards import DEFAULT_TABLES
   
   dim_trackers = DEFAULT_TABLES["DIM_TRACKERS"]
   fct_trackers = DEFAULT_TABLES["FCT_TRACKERS"]
   ```

2. Type Safety
   Implements type-safe table name access:
   ```python
   def dim_trackers() -> str:
       return "dim_trackers"
   ```

### Integration

The standards module integrates seamlessly with other components:
```python
from tools.standards import DEFAULT_TABLES
from tools.db_connect import DatabaseConnect

db = DatabaseConnect()
db.cursor.execute(f"SELECT * FROM {DEFAULT_TABLES['DIM_TRACKERS']}")
```

## Common Patterns and Usage

Here are some common patterns for using these tools together:

1. Database Operations with Standardized Tables
   ```python
   from tools.db_connect import DatabaseConnect
   from tools.standards import DEFAULT_TABLES
   
   db = DatabaseConnect()
   try:
       db.connect()
       query = f"SELECT * FROM {DEFAULT_TABLES['DIM_TRACKERS']}"
       db.cursor.execute(query)
   finally:
       db.disconnect()
   ```

2. DataFrame Processing and Database Writing
   ```python
   from tools.df_manipulation import ReadyDF
   from tools.db_connect import DatabaseConnect
   import pandas as pd
   
   # Process DataFrame
   df = pd.DataFrame(your_data)
   normalized_df = ReadyDF.normalize(df)
   
   # Write to database
   db = DatabaseConnect()
   try:
       db.connect()
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
       normalized_df = ReadyDF.normalize(df)
   except ValueError as e:
       logger.error(f"Normalization failed: {str(e)}")
       raise
   ```

## Logging

All modules use Python's logging framework with consistent formatting:
```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

## Future Enhancements

Several improvements are planned for these utilities:
1. Enhanced connection pooling capabilities
2. Additional DataFrame transformation options
3. Extended table name management features
4. Improved type hinting and validation

For information about how these tools are used in practice, please refer to the README in the scripts directory.