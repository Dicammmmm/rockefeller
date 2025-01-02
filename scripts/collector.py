import yfinance as yf
import pandas as pd
from datetime import datetime
import time
import logging
from tools.db_connect import DatabaseConnect
import psycopg2

logger = logging.getLogger(__name__)


def fetch_ticker_data(symbol):
    try:
        logger.info(f"Fetching data for {symbol}")
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="6mo", actions=True)

        if hist.empty:
            logger.warning(f"No data found for {symbol}")
            return None

        # Reset index to make Date a column
        hist = hist.reset_index()

        # Add symbol column
        hist['symbol'] = symbol

        # Convert column names to lowercase
        hist.columns = hist.columns.str.lower()

        # Rename 'stock splits' to 'stock_splits'
        hist = hist.rename(columns={'stock splits': 'stock_splits'})

        # Reorder columns in desired order
        ordered_columns = [
            'symbol',
            'date',
            'open',
            'high',
            'low',
            'close',
            'volume',
            'dividends',
            'stock_splits'
        ]

        hist = hist[ordered_columns]

        logger.debug(f"Successfully fetched {len(hist)} records for {symbol}")
        return hist

    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {str(e)}")
        return None


def main():
    db = DatabaseConnect()

    try:
        # Connect to database
        db.connect()

        # Fetch tickers
        logger.info("Fetching ticker symbols from database...")
        db.cursor.execute("SELECT symbol FROM prod.tickers_dim")
        symbols = [row[0] for row in db.cursor.fetchall()]
        logger.info(f"Found {len(symbols)} symbols to process")

        for symbol in symbols:
            logger.info(f"Processing {symbol}...")
            hist_data = fetch_ticker_data(symbol)

            if hist_data is not None:
                try:
                    # Convert DataFrame to list of tuples for insertion
                    values_list = [tuple(x) for x in hist_data.to_numpy()]

                    # Generate the insert query
                    columns = ', '.join(hist_data.columns)
                    placeholders = ', '.join(['%s'] * len(hist_data.columns))
                    insert_query = f"""
                        INSERT INTO prod.tickers_metrics ({columns})
                        VALUES ({placeholders})
                    """

                    # Execute batch insert
                    db.cursor.executemany(insert_query, values_list)
                    db.conn.commit()
                    logger.info(f"Successfully wrote {len(hist_data)} records for {symbol}")

                except Exception as e:
                    db.conn.rollback()
                    logger.error(f"Error writing data for {symbol}: {str(e)}")

            # Add delay to avoid hitting rate limits
            time.sleep(1)

    except Exception as e:
        logger.error(f"Main process error: {str(e)}")
        raise

    finally:
        logger.info("Cleaning up database connection...")
        db.disconnect()


if __name__ == "__main__":
    main()
