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

def _insert_financial_data(db, tracker: str, date: pd.Timestamp, history_row: pd.Series, 
                          ticker_info: Dict[str, Any]) -> None:
    """
    Insert a single day's financial data into the database
    """
    financials = {
        "tracker": str(tracker),
        "date": date,
        "open": float(history_row['Open']),
        "high": float(history_row['High']),
        "low": float(history_row['Low']),
        "close": float(history_row['Close']),
        "volume": int(history_row['Volume']),
        "dividends": float(history_row['Dividends']),
        "stock_splits": float(history_row['Stock Splits']),
        "operating_margin": float(ticker_info.get("operatingMargins", np.nan)),
        "gross_margin": float(ticker_info.get("grossMargins", np.nan)),
        "net_profit_margin": float(ticker_info.get("profitMargins", np.nan)),
        "roa": float(ticker_info.get("returnOnAssets", np.nan)),
        "roe": float(ticker_info.get("returnOnEquity", np.nan)),
        "ebitda": float(ticker_info.get("ebitda", np.nan)),
        "quick_ratio": float(ticker_info.get("quickRatio", np.nan)),
        "operating_cashflow": float(ticker_info.get("operatingCashflow", np.nan)),
        "working_capital": float(ticker_info.get("workingCapital", np.nan)),
        "p_e": float(ticker_info.get("forwardPE", np.nan)),
        "p_b": float(ticker_info.get("priceToBook", np.nan)),
        "p_s": float(ticker_info.get("priceToSales", np.nan)),
        "dividend_yield": float(ticker_info.get("dividendYield", np.nan)),
        "eps": float(ticker_info.get("trailingEps", np.nan)),
        "debt_to_asset": float(ticker_info.get("debtToAssets", np.nan)),
        "debt_to_equity": float(ticker_info.get("debtToEquity", np.nan)),
        "interest_coverage_ratio": float(ticker_info.get("interestCoverage", np.nan))
    }

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
        ON CONFLICT (tracker, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            dividends = EXCLUDED.dividends,
            stock_splits = EXCLUDED.stock_splits,
            operating_margin = EXCLUDED.operating_margin,
            gross_margin = EXCLUDED.gross_margin,
            net_profit_margin = EXCLUDED.net_profit_margin,
            roa = EXCLUDED.roa,
            roe = EXCLUDED.roe,
            ebitda = EXCLUDED.ebitda,
            quick_ratio = EXCLUDED.quick_ratio,
            operating_cashflow = EXCLUDED.operating_cashflow,
            working_capital = EXCLUDED.working_capital,
            p_e = EXCLUDED.p_e,
            p_b = EXCLUDED.p_b,
            p_s = EXCLUDED.p_s,
            dividend_yield = EXCLUDED.dividend_yield,
            eps = EXCLUDED.eps,
            debt_to_asset = EXCLUDED.debt_to_asset,
            debt_to_equity = EXCLUDED.debt_to_equity,
            interest_coverage_ratio = EXCLUDED.interest_coverage_ratio
    """, tuple(financials.values()))

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

            # Get the most recent info for financial ratios
            ticker_info = ticker.info
            
            # Process each day's data
            rows_processed = 0
            for date, row in history.iterrows():
                try:
                    # Skip days with missing essential data
                    if any(pd.isna(row[col]) for col in ['Open', 'High', 'Low', 'Close', 'Volume']):
                        continue

                    _insert_financial_data(db, tracker, date, row, ticker_info)
                    rows_processed += 1

                    # Commit every 100 rows to avoid large transactions
                    if rows_processed % 100 == 0:
                        db.conn.commit()
                        logger.debug(f"Committed {rows_processed} rows for {tracker}")

                except Exception as e:
                    logger.error(f"Error processing row for {tracker} on {date}: {str(e)}")
                    db.conn.rollback()
                    continue

            # Final commit for any remaining rows
            db.conn.commit()
            logger.info(f"Successfully processed {rows_processed} days of data for {tracker}")
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
