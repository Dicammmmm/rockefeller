import os
import logging
import numpy as np
import yfinance as yf
from typing import List, Dict, Any, Tuple
from multiprocessing import Pool, cpu_count
from tools.db_connect import DatabaseConnect


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _get_trackers() -> list:
    """
    Fetches a list of trackers from the database.
    Returns:
        List of active trackers.
    """
    db = DatabaseConnect()
    try:
        db.connect()
        db.cursor.execute("SELECT ARRAY_AGG(tracker) FROM dim_trackers WHERE delisted = FALSE")
        trackers: list = db.cursor.fetchone()[0]
        logger.info(f"Successfully fetched {len(trackers)} trackers from the database")
        return trackers
    except Exception as e:
        logger.error(f"Error fetching trackers from the database: {str(e)}")
        raise
    finally:
        db.disconnect()


def _write_data(db: DatabaseConnect, financial: Dict[str, Any], tracker: str, date: Any) -> bool:
    """
    Write a single financial record to the database.

    Args:
        db: Database connection
        financial: Dictionary of financial data
        tracker: Tracker symbol
        date: Date of the data

    Returns:
        bool: True if successful, False if failed
    """
    try:
        db.cursor.execute("""
            INSERT INTO fact_trackers (
                tracker, date, open, high, low, close, volume, dividends, stock_splits,
                operating_margin, gross_margin, net_profit_margin, roa, roe, ebitda, 
                quick_ratio, operating_cashflow, working_capital, p_e, p_b, p_s, 
                dividend_yield, eps, debt_to_asset, debt_to_equity, interest_coverage_ratio
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, tuple(financial.values()))
        return True
    except Exception as e:
        logger.error(f"Error inserting data for {tracker} on {date}: {str(e)}")
        db.conn.rollback()
        return False


def _process_chunk(chunk_data: Tuple[List[str], str]) -> Tuple[List[str], List[str]]:
    """
    Process a chunk of trackers in parallel.

    Args:
        chunk_data: Tuple containing (list of trackers, period string)

    Returns:
        Tuple containing (1y pass list, 5d pass list)
    """
    trackers, period = chunk_data
    y1_pass: list = []
    d5_pass: list = []

    # Create a database connection for this process
    db = DatabaseConnect()
    db.connect()

    logger.info(f"Process {os.getpid()} starting to process {len(trackers)} trackers")

    try:
        for tracker in trackers:
            ticker = yf.Ticker(tracker)
            try:
                history = ticker.history(period=period)

                if len(history) > 0:
                    info = ticker.info
                    successful_writes = 0

                    # Process all historical data points
                    for date, row in history.iterrows():
                        financials = {
                            "tracker": str(tracker),
                            "date": date,
                            "open": float(row['Open']),
                            "high": float(row['High']),
                            "low": float(row['Low']),
                            "close": float(row['Close']),
                            "volume": int(row['Volume']),
                            "dividends": float(row['Dividends']) if 'Dividends' in row else np.nan,
                            "stock_splits": float(row['Stock Splits']) if 'Stock Splits' in row else np.nan,
                            "operating_margin": float(info.get("operatingMargins", np.nan)),
                            "gross_margin": float(info.get("grossMargins", np.nan)),
                            "net_profit_margin": float(info.get("profitMargins", np.nan)),
                            "roa": float(info.get("returnOnAssets", np.nan)),
                            "roe": float(info.get("returnOnEquity", np.nan)),
                            "ebitda": float(info.get("ebitda", np.nan)),
                            "quick_ratio": float(info.get("quickRatio", np.nan)),
                            "operating_cashflow": float(info.get("operatingCashflow", np.nan)),
                            "working_capital": float(info.get("workingCapital", np.nan)),
                            "p_e": float(info.get("forwardPE", np.nan)),
                            "p_b": float(info.get("priceToBook", np.nan)),
                            "p_s": float(info.get("priceToSales", np.nan)),
                            "dividend_yield": float(info.get("dividendYield", np.nan)),
                            "eps": float(info.get("trailingEps", np.nan)),
                            "debt_to_asset": float(info.get("debtToAssets", np.nan)),
                            "debt_to_equity": float(info.get("debtToEquity", np.nan)),
                            "interest_coverage_ratio": float(info.get("interestCoverage", np.nan))
                        }

                        if _write_data(db, financials, tracker, date):
                            successful_writes += 1

                    # Commit after all dates for this ticker are processed
                    if successful_writes > 0:
                        db.conn.commit()
                        logger.info(f"Successfully inserted {successful_writes} records for {tracker}")

            except Exception as e:
                error_msg = str(e)
                if f"Period '{period}' is invalid, must be one of ['1d', '5d', '1mo', '3mo', '6mo', '1y', 'ytd', 'max']" in error_msg:
                    y1_pass.append(tracker)
                    logger.info(f"Added {tracker} to second pass due to period error")
                elif f"Period '{period}' is invalid, must be one of ['1d', '5d']" in error_msg:
                    d5_pass.append(tracker)
                    logger.info(f"Added {tracker} to third pass due to limited period options")
                else:
                    logger.error(f"Unexpected error for {tracker}: {error_msg}")
                continue

    except Exception as e:
        logger.error(f"Error in chunk processing: {str(e)}")
        raise

    finally:
        db.disconnect()
        logger.info(f"Process {os.getpid()} completed processing {len(trackers)} trackers")

    return y1_pass, d5_pass


def _parallel_get_data(trackers: List[str], period: str = "5y", max_workers: int = None) -> Tuple[List[str], List[str]]:
    """
    Fetches historical data for a list of trackers in parallel.

    Args:
        trackers: List of trackers to fetch data for
        period: Time period for historical data, default is 5 years
        max_workers: Maximum number of processes (defaults to CPU count)

    Returns:
        Tuple containing (1y pass list, 5d pass list)
    """
    if max_workers is None:
        max_workers = cpu_count()

    # Calculate optimal chunk size based on number of trackers and cores
    chunk_size = max(1, min(100, len(trackers) // max_workers))  # Cap at 100 trackers per chunk

    # Split trackers into chunks
    tracker_chunks = [trackers[i:i + chunk_size] for i in range(0, len(trackers), chunk_size)]

    # Prepare input data for parallel processing
    chunk_data = [(chunk, period) for chunk in tracker_chunks]

    try:
        with Pool(processes=max_workers) as pool:
            logger.info(f"Starting parallel processing with {max_workers} workers")
            results = pool.map(_process_chunk, chunk_data)

            # Combine results
            y1_pass = []
            d5_pass = []

            for y1, d5 in results:
                y1_pass.extend(y1)
                d5_pass.extend(d5)

            logger.info(f"Parallel processing completed using {max_workers} cores")
            return y1_pass, d5_pass

    except Exception as e:
        logger.error(f"Error in parallel processing: {str(e)}")
        raise

def main() -> None:
    """
    Main function to fetch and process data using parallel processing.
    """
    try:
        logger.info("Starting data processing")

        # Get list of trackers
        trackers = _get_trackers()
        logger.info(f"Retrieved {len(trackers)} trackers to process")

        # Process initial batch
        y1_pass, d5_pass = _parallel_get_data(trackers, period="5y")

        # Process second pass if needed
        if y1_pass:
            logger.info(f"Processing {len(y1_pass)} trackers in second pass")
            _, remaining_d5 = _parallel_get_data(y1_pass, period="1y")
            d5_pass.extend(remaining_d5)

        # Process third pass if needed
        if d5_pass:
            logger.info(f"Processing {len(d5_pass)} trackers in third pass")
            _, _ = _parallel_get_data(d5_pass, period="5d")

        logger.info("All processing completed successfully")

    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise


if __name__ == "__main__":
    main()
