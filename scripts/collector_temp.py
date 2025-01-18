import logging
import numpy as np
import yfinance as yf
import pandas as pd
from tools.db_connect import DatabaseConnect
from multiprocessing import Pool, cpu_count
import os

# Setup logging
logger = logging.getLogger(__name__)


def _get_trackers() -> list:
    """
    Fetches a list of trackers from the database.
    :return: Returns a list of trackers.
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


def _process_single_tracker(args: tuple) -> tuple[str, str]:
    """
    Process a single tracker in its own process.
    :param args: Tuple containing (tracker, period)
    :return: Tuple of (tracker, status) where status is either 'success', 'second_pass', or 'third_pass'
    """
    tracker, period = args

    # Each process gets its own database connection
    db = DatabaseConnect()
    db.connect()

    logger.info(f"Process {os.getpid()} processing tracker {tracker}")

    try:
        ticker = yf.Ticker(tracker)
        try:
            # Get historical price data
            history = ticker.history(period=period)

            if len(history) > 0:
                # Get additional financial metrics
                info = ticker.info

                # Process each row in the historical data
                for date, row in history.iterrows():
                    financials = {
                        "tracker": str(tracker),
                        "date": date,
                        "open": float(row['Open']),
                        "high": float(row['High']),
                        "low": float(row['Low']),
                        "close": float(row['Close']),
                        "volume": int(row['Volume']),
                        "dividends": float(row['Dividends']) if 'Dividends' in row else None,
                        "stock_splits": float(row['Stock Splits']) if 'Stock Splits' in row else None,
                        "operating_margin": float(info.get("operatingMargins", None)),
                        "gross_margin": float(info.get("grossMargins", None)),
                        "net_profit_margin": float(info.get("profitMargins", None)),
                        "roa": float(info.get("returnOnAssets", None)),
                        "roe": float(info.get("returnOnEquity", None)),
                        "ebitda": float(info.get("ebitda", None)),
                        "quick_ratio": float(info.get("quickRatio", None)),
                        "operating_cashflow": float(info.get("operatingCashflow", None)),
                        "working_capital": float(info.get("workingCapital", None)),
                        "p_e": float(info.get("forwardPE", None)),
                        "p_b": float(info.get("priceToBook", None)),
                        "p_s": float(info.get("priceToSales", None)),
                        "dividend_yield": float(info.get("dividendYield", None)),
                        "eps": float(info.get("trailingEps", None)),
                        "debt_to_asset": float(info.get("debtToAssets", None)),
                        "debt_to_equity": float(info.get("debtToEquity", None)),
                        "interest_coverage_ratio": float(info.get("interestCoverage", None))
                    }

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

                    except Exception as e:
                        logger.error(f"Error inserting data for {tracker} on {date}: {str(e)}")
                        db.conn.rollback()
                        continue

                # Commit after all rows for a ticker are processed
                db.conn.commit()
                logger.info(f"Successfully inserted time series data for {tracker}")
                return tracker, "success"

        except Exception as e:
            error_msg = str(e)
            if f"Period '{period}' is invalid, must be one of ['1d', '5d', '1mo', '3mo', '6mo', '1y', 'ytd', 'max']" in error_msg:
                logger.info(f"Added {tracker} to second pass due to period error")
                return tracker, "second_pass"
            elif f"Period '{period}' is invalid, must be one of ['1d', '5d']" in error_msg:
                logger.info(f"Added {tracker} to third pass due to limited period options")
                return tracker, "third_pass"
            else:
                logger.error(f"Unexpected error for {tracker}: {error_msg}")
                return tracker, "error"

    except Exception as e:
        logger.error(f"Error processing tracker {tracker}: {str(e)}")
        return tracker, "error"
    finally:
        db.disconnect()


def _process_data_parallel(trackers: list, period: str = "5y", max_workers: int = None) -> tuple[list, list]:
    """
    Process trackers in parallel using multiprocessing.
    :param trackers: List of trackers to process
    :param period: Time period for historical data
    :param max_workers: Maximum number of processes (defaults to CPU count)
    :return: Tuple containing lists of trackers for second and third pass
    """
    if max_workers is None:
        max_workers = cpu_count()  # Use number of CPU cores

    second_pass = []
    third_pass = []

    # Create arguments list for multiprocessing
    args = [(tracker, period) for tracker in trackers]

    # Process trackers in parallel
    with Pool(processes=max_workers) as pool:
        results = pool.map(_process_single_tracker, args)

        # Process results
        for tracker, status in results:
            if status == "second_pass":
                second_pass.append(tracker)
            elif status == "third_pass":
                third_pass.append(tracker)

    return second_pass, third_pass


def main() -> None:
    """
    Main function to fetch and process data.
    """
    try:
        trackers = _get_trackers()

        # Process initial batch with parallel processing
        second_pass, third_pass = _process_data_parallel(trackers)

        if second_pass:
            logger.info(f"Found {len(second_pass)} trackers for second pass")
            second_pass_results = _process_data_parallel(second_pass, "1y")

        if third_pass:
            logger.info(f"Found {len(third_pass)} trackers for third pass")
            third_pass_results = _process_data_parallel(third_pass, "5d")

    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise


if __name__ == "__main__":
    main()
