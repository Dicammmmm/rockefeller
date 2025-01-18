import logging
import numpy as np
import yfinance as yf
import pandas as pd
from tools.db_connect import DatabaseConnect
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Any, Optional
import threading
from contextlib import contextmanager
import time
from functools import wraps

# Setup logging and database connection
logger = logging.getLogger(__name__)
thread_local = threading.local()
DB_RETRY_ATTEMPTS = 3
DB_RETRY_DELAY = 1  # seconds

@contextmanager
def get_db_connection():
    """
    Context manager for thread-local database connections
    """
    if not hasattr(thread_local, "db"):
        thread_local.db = DatabaseConnect()
    
    try:
        thread_local.db.connect()
        yield thread_local.db
    finally:
        thread_local.db.disconnect()

def retry_on_db_error(func):
    """
    Decorator to retry database operations
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        for attempt in range(DB_RETRY_ATTEMPTS):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == DB_RETRY_ATTEMPTS - 1:
                    raise
                logger.warning(f"Database operation failed, attempt {attempt + 1}/{DB_RETRY_ATTEMPTS}: {str(e)}")
                time.sleep(DB_RETRY_DELAY)
    return wrapper

@retry_on_db_error
def _get_trackers() -> List[str]:
    """
    Fetches a list of trackers from the database.
    """
    with get_db_connection() as db:
        db.cursor.execute("SELECT ARRAY_AGG(tracker) FROM dim_trackers WHERE delisted = FALSE")
        trackers: Optional[List[str]] = db.cursor.fetchone()[0]
        
        if not trackers:
            logger.warning("No trackers found in database")
            return []
            
        logger.info(f"Successfully fetched {len(trackers)} trackers from the database")
        return trackers

def _process_single_tracker(tracker: str, period: str = "5y") -> Tuple[str, List[str]]:
    """
    Process a single tracker and return its category for potential reprocessing
    """
    try:
        with get_db_connection() as db:
            ticker = yf.Ticker(tracker)
            history = ticker.history(period=period)

            if len(history) == 0:
                logger.warning(f"No historical data found for tracker {tracker}")
                return tracker, []

            latest_date = history.index[-1]
            
            # Validate essential data points
            if any(pd.isna(history[col].iloc[-1]) for col in ['Open', 'High', 'Low', 'Close', 'Volume']):
                logger.warning(f"Missing essential price data for {tracker}")
                return tracker, []

            financials = {
                "tracker": str(tracker),
                "date": latest_date,
                "open": float(history['Open'].iloc[-1]),
                "high": float(history['High'].iloc[-1]),
                "low": float(history['Low'].iloc[-1]),
                "close": float(history['Close'].iloc[-1]),
                "volume": int(history['Volume'].iloc[-1]),
                "dividends": float(ticker.dividends.iloc[-1]) if len(ticker.dividends) > 0 else np.nan,
                "stock_splits": float(ticker.splits.iloc[-1]) if len(ticker.splits) > 0 else np.nan
            }
            
            # Add financial ratios with validation
            for key, info_key in [
                ("operating_margin", "operatingMargins"),
                ("gross_margin", "grossMargins"),
                ("net_profit_margin", "profitMargins"),
                ("roa", "returnOnAssets"),
                ("roe", "returnOnEquity"),
                ("ebitda", "ebitda"),
                ("quick_ratio", "quickRatio"),
                ("operating_cashflow", "operatingCashflow"),
                ("working_capital", "workingCapital"),
                ("p_e", "forwardPE"),
                ("p_b", "priceToBook"),
                ("p_s", "priceToSales"),
                ("dividend_yield", "dividendYield"),
                ("eps", "trailingEps"),
                ("debt_to_asset", "debtToAssets"),
                ("debt_to_equity", "debtToEquity"),
                ("interest_coverage_ratio", "interestCoverage")
            ]:
                value = ticker.info.get(info_key, np.nan)
                financials[key] = float(value) if value is not None else np.nan

            try:
                db.cursor.execute("""
                    INSERT INTO fact_trackers (
                        tracker, date, open, high, low, close, volume, dividends, stock_splits,
                        operating_margin, gross_margin, net_profit_margin, roa, roe, ebitda, quick_ratio,
                        operating_cashflow, working_capital, p_e, p_b, p_s, dividend_yield, eps,
                        debt_to_asset, debt_to_equity, interest_coverage_ratio
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, tuple(financials.values()))
                db.conn.commit()
                logger.info(f"Successfully inserted data for {tracker}")
                return tracker, []

            except Exception as e:
                logger.error(f"Error inserting data for {tracker}: {str(e)}")
                db.conn.rollback()
                return tracker, []

    except Exception as e:
        error_msg = str(e)
        if "Period" in error_msg:
            if any(p in error_msg for p in ['5y', '2y', '1y']):
                logger.info(f"Added {tracker} to second pass due to period error")
                return tracker, ["second_pass"]
            elif '5d' in error_msg:
                logger.info(f"Added {tracker} to third pass due to limited period options")
                return tracker, ["third_pass"]
        
        logger.error(f"Unexpected error for {tracker}: {error_msg}")
        return tracker, []

def _process_data_parallel(trackers: List[str], period: str = "5y", max_workers: int = 10) -> Tuple[List[str], List[str]]:
    """
    Processes trackers in parallel using threading
    """
    second_pass = []
    third_pass = []
    processed_count = 0
    total_trackers = len(trackers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_tracker = {
            executor.submit(_process_single_tracker, tracker, period): tracker
            for tracker in trackers
        }

        for future in as_completed(future_to_tracker):
            tracker = future_to_tracker[future]
            try:
                _, passes = future.result()
                if "second_pass" in passes:
                    second_pass.append(tracker)
                elif "third_pass" in passes:
                    third_pass.append(tracker)
                
                processed_count += 1
                if processed_count % 10 == 0:
                    logger.info(f"Progress: {processed_count}/{total_trackers} trackers processed")
                    
            except Exception as e:
                logger.error(f"Tracker {tracker} generated an exception: {str(e)}")

    return second_pass, third_pass

def main() -> None:
    """
    Main function to fetch and process data with parallel processing
    """
    try:
        trackers = _get_trackers()
        if not trackers:
            logger.error("No trackers to process")
            return

        # First pass with 5y period
        logger.info("Starting first pass with 5y period")
        second_pass, third_pass = _process_data_parallel(trackers)

        # Second pass with 1y period
        if second_pass:
            logger.info(f"Processing {len(second_pass)} trackers in second pass")
            additional_third_pass, _ = _process_data_parallel(second_pass, "1y")
            third_pass.extend(additional_third_pass)

        # Third pass with 5d period
        if third_pass:
            logger.info(f"Processing {len(third_pass)} trackers in third pass")
            _process_data_parallel(third_pass, "5d")

        logger.info("Data processing completed successfully")

    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    main()
