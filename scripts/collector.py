import logging
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

def _process_data(trackers: list) -> None:
    """
    Fetches historical data for a list of trackers.
    :param trackers: List of trackers to fetch data for.
    :return: A DataFrame containing historical data for the trackers.
    """
    final_data: pd.DataFrame = pd.DataFrame()

    try:
        for tracker in trackers:
            ticker = yf.Ticker(tracker)

            history = ticker.history(period="5y")

            financials = {
                "tracker": tracker,
                "date": history.index[-1],  # Get the date from history's last entry
                "open": history['Open'].iloc[-1],
                "high": history['High'].iloc[-1],
                "low": history['Low'].iloc[-1],  # Added low price
                "close": history['Close'].iloc[-1],
                "volume": history['Volume'].iloc[-1],
                "dividends": ticker.dividends,
                "stock_splits": ticker.splits,
                "operating_margin": ticker.info.get("operatingMargins"),
                "gross_margin": ticker.info.get("grossMargins"),
                "net_profit_margin": ticker.info.get("profitMargins"),
                "roa": ticker.info.get("returnOnAssets"),
                "roe": ticker.info.get("returnOnEquity"),
                "ebitda": ticker.info.get("ebitda"),
                "quick_ratio": ticker.info.get("quickRatio"),
                "operating_cashflow": ticker.info.get("operatingCashflow"),
                "working_capital": ticker.info.get("workingCapital"),
                "p_e": ticker.info.get("forwardPE"),
                "p_b": ticker.info.get("priceToBook"),
                "p_s": ticker.info.get("priceToSales"),
                "dividend_yield": ticker.info.get("dividendYield"),
                "eps": ticker.info.get("trailingEps"),
                "debt_to_asset": ticker.info.get("debtToAssets"),
                "debt_to_equity": ticker.info.get("debtToEquity"),
                "interest_coverage_ratio": ticker.info.get("interestCoverage")
            }
            df_row = pd.DataFrame([financials])
            final_data = pd.concat([final_data, df_row], ignore_index=True)

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
            """)
            db.conn.commit()

    except Exception as e:
        logger.error(f"Error fetching in fetching data: {str(e)}")
        raise

    finally:
        db.disconnect()

def main() -> None:
    """
    Main function to fetch and process data.
    """
    trackers = _get_trackers()
    _process_data(trackers)

if __name__ == "__main__":

    main()