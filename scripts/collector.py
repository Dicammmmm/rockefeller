import logging
import numpy as np
import yfinance as yf
import pandas as pd
from tools.db_connect import DatabaseConnect

# Setup and test connection to the database
db = DatabaseConnect()
logger = logging.getLogger(__name__)
if db.test_connection():
    logger.info("Database connection successful")
else:
    logger.error("Database connection failed")

def _get_trackers() -> list:
    """
    Fetches a list of trackers from the database.
    :return: Returns a list of trackers.
    """
    try:
        db.connect()
        db.cursor.execute("SELECT ARRAY_AGG(tracker) FROM dim_trackers WHERE delisted = FALSE")
        trackers: list = db.cursor.fetchone()[0]
        logger.info(f"Successfully fetched {len(trackers)} trackers from the database")
        return trackers

    except Exception as e:
        logger.error(f"Error fetching trackers from the database: {str(e)}")
        raise

def _process_data(trackers: list, period: str = "5y") -> tuple[list, list]:
    """
    Fetches historical data for a list of trackers.
    :param trackers: List of trackers to fetch data for.
    :param period: Time period for historical data
    :return: Tuple containing lists of trackers for second and third pass
    """
    second_pass = []
    third_pass = []

    try:
        for tracker in trackers:
            ticker = yf.Ticker(tracker)
            try:
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

                    except Exception as e:
                        logger.error(f"Error inserting data for {tracker}: {str(e)}")
                        db.conn.rollback()
                        continue

            except Exception as e:
                error_msg = str(e)
                if f"Period '{period}' is invalid, must be one of ['1d', '5d', '1mo', '3mo', '6mo', '1y', 'ytd', 'max']" in error_msg:
                    second_pass.append(tracker)
                    logger.info(f"Added {tracker} to second pass due to period error")
                elif f"Period '{period}' is invalid, must be one of ['1d', '5d']" in error_msg:
                    third_pass.append(tracker)
                    logger.info(f"Added {tracker} to third pass due to limited period options")
                else:
                    logger.error(f"Unexpected error for {tracker}: {error_msg}")
                continue

    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        raise

    return second_pass, third_pass

def main() -> None:
    """
    Main function to fetch and process data.
    """
    try:
        db.connect()
        trackers = _get_trackers()
        second_pass, third_pass = _process_data(trackers)

        if second_pass:
            logger.info(f"Found {len(second_pass)} trackers for second pass")
            second_pass_results = _process_data(second_pass, "1y")

        if third_pass:
            logger.info(f"Found {len(third_pass)} trackers for third pass")
            third_pass_results = _process_data(third_pass, "5d")

    finally:
        db.disconnect()

if __name__ == "__main__":
    main()