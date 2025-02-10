import logging
import pandas as pd
import yfinance as yf
from yfinance import Ticker
from tools.db_connect import DatabaseConnect
from tools.standards import DEFAULT_TABLES

# Setup and test connection to the database
db = DatabaseConnect()
logger = logging.getLogger(__name__)

if db.test_connection():
    logger.info("Database connection successful")
else:
    logger.error("Database connection failed")

DIM_TRACKERS = DEFAULT_TABLES["DIM_TRACKERS"]

def get_trackers() -> list|None:
    """
    Fetches a list of trackers from the database.
    :return: Returns a list of trackers.
    """
    trackers: list = []

    try:
         db.connect()
         db.cursor.execute(f"SELECT ARRAY_AGG(tracker) FROM {DIM_TRACKERS}")
         trackers = db.cursor.fetchone()[0]
         logger.info(f"Successfully fetched {len(trackers)} trackers from the database")

         return trackers

    except Exception as e:
         logger.error(f"Error fetching trackers from the database: {str(e)}")
         raise

    finally:
         db.disconnect()


def verify_trackers(trackers: list) -> None:
    """
    Checks and verifies whether the tracker has been delisted or not.
    :param trackers: List of trackers from the database.
    :return: None
    """
    try:
        db.connect()
        recheck_trackers: list = [] # Some trackers need to be rechecked with a shorter period.

        # First pass
        for tracker in trackers:
            try:
                ticker: Ticker = yf.Ticker(tracker)
                history: pd.DataFrame = ticker.history(period="1d")

                if not history.empty:
                    logger.info(f"Ticker {tracker} is valid.")
                    db.cursor.execute(f"UPDATE {DIM_TRACKERS} SET delisted = FALSE WHERE tracker = %s", (tracker,))

            except Exception as e:
                if "Period '1d' is invalid" in str(e):
                    logger.info(f"tracker {tracker} needs recheck with shorter period")
                    recheck_trackers.append(tracker)

                else:
                    logger.warning(f"Ticker {tracker} possibly delisted: {str(e)}")
                    db.cursor.execute("UPDATE dim_trackers SET delisted = TRUE WHERE tracker = %s", (tracker,))

        # Second pass
        for tracker in recheck_trackers:
            try:
                ticker: Ticker = yf.Ticker(tracker)
                history: pd.DataFrame = ticker.history(period="1d")

                if not history.empty:
                    logger.info(f"Ticker {tracker} is valid (after recheck).")
                    db.cursor.execute(f"UPDATE {DIM_TRACKERS} SET delisted = FALSE WHERE tracker = %s", (tracker,))

                else:
                    logger.warning(f"Ticker {tracker} is invalid (after recheck).")
                    db.cursor.execute(f"UPDATE {DIM_TRACKERS} SET delisted = TRUE WHERE tracker = %s", (tracker,))

            except Exception as e:
                logger.warning(f"Ticker {tracker} failed recheck: {str(e)}")
                db.cursor.execute(f"UPDATE {DIM_TRACKERS} SET delisted = TRUE WHERE tracker = %s", (tracker,))

    except Exception as e:
        logger.error(f"Error verifying trackers: {str(e)}")
        raise

    finally:
        db.disconnect()

def main() -> None:
    """
    Main function to run the tracker verification process.
    :return: None
    """
    logger.info("Starting tracker verification process...")

    try:
        trackers: list = get_trackers()

        logger.info("Starting tracker verification...")
        verify_trackers(trackers)

    except Exception as e:
        logger.error(f"Error during tracker verification: {str(e)}")
        raise

    logger.info(f"Tracker verification process completed. Verified {len(trackers)} trackers.")


if __name__ == "__main__":
    main()