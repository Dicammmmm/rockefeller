# Scripts Directory

This directory contains the core data collection and verification scripts for the Rockefeller project. These scripts work together to maintain an up-to-date and accurate financial database.

## Collector Script (collector.py)

The collector script serves as the primary data gathering engine of the Rockefeller project. It employs parallel processing to efficiently collect financial data from Yahoo Finance while managing system resources effectively.

### Implementation Details

The collector script utilizes a multi-stage approach to data gathering:

1. Initial Data Collection
   The script first attempts to gather 5-year historical data for all trackers. This provides the most comprehensive dataset when available.

2. Fallback Mechanisms
   If the 5-year collection fails for certain trackers, the script automatically falls back to:
   - 1-year historical data collection
   - 5-day historical data collection
   This tiered approach ensures we capture as much data as possible even for restricted or newly listed securities.

### Process Flow

1. The script begins by retrieving active trackers from the database using `_get_trackers()`
2. Data collection occurs in parallel chunks using Python's multiprocessing capabilities
3. Each chunk processes multiple trackers simultaneously, with configurable chunk sizes
4. Financial data is written to the database in real-time as it's collected
5. Failed collections are automatically queued for retry with shorter time periods

### Error Handling

The collector implements comprehensive error handling:
- Connection failures are logged and retried
- Invalid tickers are identified and marked
- Database write failures trigger automatic rollbacks
- Process crashes are contained and logged

### Performance Considerations

The script includes several performance optimizations:
- Dynamic process pool sizing based on CPU cores
- Configurable chunk sizes to balance memory usage
- Efficient database connection pooling
- Optimized data type handling for database inserts

## Verify Script (verify.py)

The verify script ensures data quality by validating tracker status and maintaining the accuracy of our tracker database. It acts as a gatekeeper for the data collection process.

### Implementation Details

The verification process follows a two-step approach:

1. Primary Verification
   - Attempts to fetch 1-year historical data
   - Successful fetches confirm active status
   - Failed fetches trigger secondary verification

2. Secondary Verification
   - Attempts to fetch 1-day historical data
   - Confirms whether failures are due to delisting or data restrictions
   - Updates tracker status in the database accordingly

### Process Flow

1. Retrieves the complete list of trackers from the database
2. Performs primary verification on all trackers
3. Conducts secondary verification on failed trackers
4. Updates tracker status in the database
5. Generates verification logs for monitoring

### Error Handling

The verify script includes robust error handling:
- Distinguishes between temporary failures and actual delistings
- Manages database connection issues gracefully
- Provides detailed logging of verification results
- Maintains data integrity during updates

### Best Practices for Usage

When working with these scripts, consider the following recommendations:

1. Regular Execution
   Run the verify script before the collector to ensure optimal data collection:
   ```bash
   python verify.py && python collector.py
   ```

2. Monitoring
   Check the logs regularly for patterns in verification failures:
   ```python
   import logging
   logging.getLogger().setLevel(logging.INFO)
   ```

3. Database Maintenance
   Periodically review and clean up delisted trackers:
   ```sql
   SELECT * FROM dim_trackers WHERE delisted = TRUE;
   ```

## Integration with Tools

Both scripts rely heavily on the utility modules in the tools directory:
- DatabaseConnect for database operations
- ReadyDF for DataFrame manipulation
- Standards for consistent table naming

## Future Improvements

Several enhancements are planned for these scripts:
1. Implementation of retry mechanisms with exponential backoff
2. Addition of real-time monitoring capabilities
3. Enhanced parallel processing optimization
4. Integration with external monitoring systems

For detailed information about the utility modules used by these scripts, please refer to the README in the tools directory.