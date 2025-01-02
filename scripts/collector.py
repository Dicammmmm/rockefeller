import yfinance as yf
import pandas as pd
import time
import logging
from tools.db_connect import DatabaseConnect
import concurrent.futures
import threading

logger = logging.getLogger(__name__)


def fetch_ticker_data(symbol):
    try:
        thread_id = threading.get_ident()
        logger.info(f"Thread {thread_id}: Fetching data for {symbol}")
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="6mo", actions=True)

        if hist.empty:
            logger.warning(f"Thread {thread_id}: No data found for {symbol}")
            return None

        hist = hist.reset_index()
        hist['symbol'] = symbol
        hist.columns = hist.columns.str.lower()
        hist = hist.rename(columns={'stock splits': 'stock_splits'})

        ordered_columns = [
            'symbol', 'date', 'open', 'high', 'low',
            'close', 'volume', 'dividends', 'stock_splits'
        ]
        hist = hist[ordered_columns]

        logger.debug(f"Thread {thread_id}: Successfully fetched {len(hist)} records for {symbol}")
        return hist

    except Exception as e:
        logger.error(f"Thread {thread_id}: Error fetching data for {symbol}: {str(e)}")
        return None


def process_symbol(symbol):
    thread_id = threading.get_ident()
    db = DatabaseConnect()
    try:
        db.connect()
        hist_data = fetch_ticker_data(symbol)

        if hist_data is not None:
            values_list = [tuple(x) for x in hist_data.to_numpy()]
            columns = ', '.join(hist_data.columns)
            placeholders = ', '.join(['%s'] * len(hist_data.columns))
            insert_query = f"""
               INSERT INTO prod.tickers_metrics ({columns})
               VALUES ({placeholders})
           """
            db.cursor.executemany(insert_query, values_list)
            db.conn.commit()
            logger.info(f"Thread {thread_id}: Successfully wrote {len(hist_data)} records for {symbol}")

    except Exception as e:
        if hasattr(db, 'conn'):
            db.conn.rollback()
        logger.error(f"Thread {thread_id}: Error processing {symbol}: {str(e)}")
    finally:
        if hasattr(db, 'conn'):
            db.disconnect()
        time.sleep(1)


def main():
    db = DatabaseConnect()
    try:
        db.connect()
        logger.info("Fetching ticker symbols from database...")
        db.cursor.execute("SELECT symbol FROM prod.tickers_dim")
        symbols = [row[0] for row in db.cursor.fetchall()]
        logger.info(f"Found {len(symbols)} symbols to process")

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.map(process_symbol, symbols)

    except Exception as e:
        logger.error(f"Main process error: {str(e)}")
        raise
    finally:
        db.disconnect()


if __name__ == "__main__":
    main()