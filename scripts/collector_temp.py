import logging
import numpy as np
import yfinance as yf
import pandas as pd
from tools.db_connect import DatabaseConnect
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Any
import threading

# Setup logging and database connection
db = DatabaseConnect()
logger = logging.getLogger(__name__)
thread_local = threading.local()


def get_db_connection():
    """
    Creates thread-local database connections
    """
    if not hasattr(thread_local, "db"):
        thread_local.db = DatabaseConnect()
    return thread_local.db


def _get_trackers() -> List[str]:
    """
    Fetches a list of trackers from the database.
    """
    try:
        db = get_db_connection()
        db.connect()
        db.cursor.execute("SELECT ARRAY_AGG(tracker) FROM dim_trackers WHERE delisted = FALSE")
        trackers: List[str] = db.cursor.fetchone()[0]
        logger.info(f"Successfully fetched {len(trackers)} trackers from the database")
        return trackers
    except Exception as e:
        logger.error(f"Error fetching trackers from the database: {str(e)}")
        raise
    finally:
        db.disconnect()


def _process_single_tracker(tracker: str, period: str = "5y") -> Tuple[str, List[str]]:
    """
    Process a single tracker and return its category for potential reprocessing
    """
    db = get_db_connection()
    db.connect()

    try:
        ticker = yf.Ticker(tracker)
        history = ticker.history(period=period)

        if len(history) > 0:
            latest_date = history.index[-1]

            financials = {
                "tracker": str(tracker),
                "date": latest_date,
                "open": float(history['Open'].iloc[-1]),
                "high": float(history['High'].iloc[-1]),
                "low": float(history['Low'].iloc[-1]),
                "close": float(history['Close'].iloc[-1]),
                "volume": int(history['Volume'].iloc[-1]),
                "dividends": float(ticker.dividends.iloc[-1]) if len(ticker.dividends) > 0 else np.nan,
                "stock_splits": float(ticker.splits.iloc[-1]) if len(ticker.splits) > 0 else np.nan,
                "operating_margin": float(ticker.info.get("operatingMargins", np.nan)),
                "gross_margin": float(ticker.info.get("grossMargins", np.nan)),
                "net_profit_margin": float(ticker.info.get("profitMargins", np.nan)),
                "roa": float(ticker.info.get("returnOnAssets", np.nan)),
                "roe": float(ticker.info.get("returnOnEquity", np.nan)),
                "ebitda": float(ticker.info.get("ebitda", np.nan)),
                "quick_ratio": float(ticker.info.get("quickRatio", np.nan)),
                "operating_cashflow": float(ticker.info.get("operatingCashflow", np.nan)),
                "working_capital": float(ticker.info.get("workingCapital", np.nan)),
                "p_e": float(ticker.info.get("forwardPE", np.nan)),
                "p_b": float(ticker.info.get("priceToBook", np.nan)),
                "p_s": float(ticker.info.get("priceToSales", np.nan)),
                "dividend_yield": float(ticker.info.get("dividendYield", np.nan)),
                "eps": float(ticker.info.get("trailingEps", np.nan)),
                "debt_to_asset": float(ticker.info.get("debtToAssets", np.nan)),
                "debt_to_equity": float(ticker.info.get("debtToEquity", np.nan)),
                "interest_coverage_ratio": float(ticker.info.get("interestCoverage", np.nan))
            }

            try:
                db.cursor.execute("""
                    INSERT INTO fact_trackers (
                        tracker, date, open, high, low, close, volume, dividends, stock_splits,
                        operating_margin, gross_margin, net_profit_margin, roa, roe, ebitda, quick_ratio,
                        operating_cashflow, working_capital, p_e, p_b, p_s, dividend_yield, eps, debt_to_asset,
                        debt_to_equity, interest_coverage_ratio
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
        if f"Period '{period}' is invalid, must be one of ['1d', '5d', '1mo', '3mo', '6mo', '1y', 'ytd', 'max']" in error_msg:
            logger.info(f"Added {tracker} to second pass due to period error")
            return tracker, ["second_pass"]
        elif f"Period '{period}' is invalid, must be one of ['1d', '5d']" in error_msg:
            logger.info(f"Added {tracker} to third pass due to limited period options")
            return tracker, ["third_pass"]
        else:
            logger.error(f"Unexpected error for {tracker}: {error_msg}")
            return tracker, []
    finally:
        db.disconnect()


def _process_data_parallel(trackers: List[str], period: str = "5y", max_workers: int = 10) -> Tuple[
    List[str], List[str]]:
    """
    Processes trackers in parallel using threading
    """
    second_pass = []
    third_pass = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_tracker = {
            executor.submit(_process_single_tracker, tracker, period): tracker
            for tracker in trackers
        }

        # Process completed tasks
        for future in as_completed(future_to_tracker):
            tracker = future_to_tracker[future]
            try:
                _, passes = future.result()
                if "second_pass" in passes:
                    second_pass.append(tracker)
                elif "third_pass" in passes:
                    third_pass.append(tracker)
            except Exception as e:
                logger.error(f"Tracker {tracker} generated an exception: {str(e)}")

    return second_pass, third_pass


def main() -> None:
    """
    Main function to fetch and process data with parallel processing
    """
    try:
        trackers = _get_trackers()

        # First pass with 5y period
        second_pass, third_pass = _process_data_parallel(trackers)

        # Second pass with 1y period
        if second_pass:
            logger.info(f"Processing {len(second_pass)} trackers in second pass")
            second_pass_results = _process_data_parallel(second_pass, "1y")

        # Third pass with 5d period
        if third_pass:
            logger.info(f"Processing {len(third_pass)} trackers in third pass")
            third_pass_results = _process_data_parallel(third_pass, "5d")

    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise


if __name__ == "__main__":
    main()