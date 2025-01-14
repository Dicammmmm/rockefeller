import os
import logging
import threading
import pandas as pd
import numpy as np
import yfinance as yf
from yfinance import Ticker
from tools.df_manipulation import ReadyDF
from tools.db_connect import DatabaseConnect
import threading
from queue import Queue
from typing import List

# Setup and test connection to the database
db = DatabaseConnect()
logger = logging.getLogger(__name__)

if db.test_connection():
    logger.info("Database connection successful")
else:
    logger.error("Database connection failed")


def get_trackers() -> list:
    """
    Fetches a list of trackers from the database.
    :return: Returns a list of trackers.
    """
    trackers: list = []

    try:
         db.connect()
         db.cursor.execute("SELECT ARRAY_AGG(tracker) FROM dim_trackers")
         trackers = db.cursor.fetchone()[0]
         logger.info(f"Successfully fetched {len(trackers)} trackers from the database")

         return trackers

    except Exception as e:
         logger.error(f"Error fetching trackers from the database: {str(e)}")
         raise

    finally:
         db.disconnect()

def get_historical_data(trackers: list) -> pd.DataFrame:
    """
    Fetches historical data for all trackers.
    :param trackers: List of trackers from the database.
    :return: history: List of historical data for the trackers.
    """
    history = []
    try:
        for tracker in trackers:
            ticker: Ticker = yf.Ticker(tracker)
            history_temp: pd.DataFrame = ticker.history(period="5y") # Temporary dataframe to store historical data

            if not history_temp.empty:
                history_temp["tracker"] = tracker
                history.append(history_temp)
                logger.info(f"Successfully fetched historical data for {tracker}")

        if history:
            history = pd.concat(history, axis=0)
            history.set_index(["tracker", history.index], inplace=True)
            logger.info(f"Successfully fetched historical data for {len(trackers)} trackers")

        return history

    except Exception as e:
        logger.error(f"Error fetching historical data for {tracker}: {str(e)}")
        raise

def get_profitability_ratios(trackers: list) -> pd.DataFrame:
    """
    Fetches profitability ratios for all trackers.
    :param trackers: List of trackers from the database.
    :return: profitability_ratios: List of profitability ratios for the trackers.
    """
    profitability_ratios = []

    try:
        for tracker in trackers:
            ticker: Ticker = yf.Ticker(tracker)
            profitability_ratios_temp: pd.DataFrame = ticker.financials

            if not profitability_ratios_temp.empty:
                profitability_ratios_temp["tracker"] = tracker
                profitability_ratios.append(profitability_ratios_temp)
                logger.info(f"Successfully fetched profitability ratios for {tracker}")

        if profitability_ratios:
            profitability_ratios = pd.concat(profitability_ratios, axis=0)
            profitability_ratios.set_index(["tracker", profitability_ratios.index], inplace=True)
            logger.info(f"Successfully fetched profitability ratios for {len(trackers)} trackers")

        return profitability_ratios

    except Exception as e:
        logger.error(f"Error fetching profitability ratios for {tracker}: {str(e)}")
        raise

def get_liquidity_ratios(trackers: list) -> pd.DataFrame:
    """
    Fetches liquidity ratios for all trackers.
    :param trackers: List of trackers from the database.
    :return: liquidity_ratios: List of liquidity ratios for the trackers.
    """
    liquidity_ratios = []

    try:
        for tracker in trackers:
            ticker: Ticker = yf.Ticker(tracker)
            liquidity_ratios_temp: pd.DataFrame = ticker.balance_sheet

            if not liquidity_ratios_temp.empty:
                liquidity_ratios_temp["tracker"] = tracker
                liquidity_ratios.append(liquidity_ratios_temp)
                logger.info(f"Successfully fetched liquidity ratios for {tracker}")

        if liquidity_ratios:
            liquidity_ratios = pd.concat(liquidity_ratios, axis=0)
            liquidity_ratios.set_index(["tracker", liquidity_ratios.index], inplace=True)
            logger.info(f"Successfully fetched liquidity ratios for {len(trackers)} trackers")

        return liquidity_ratios

    except Exception as e:
        logger.error(f"Error fetching liquidity ratios for {tracker}: {str(e)}")
    raise

def get_valuation_ratios(trackers: list) -> pd.DataFrame:
    """
    Fetches valuation ratios for all trackers.
    :param trackers: List of trackers from the database.
    :return: valuation_ratios: List of valuation ratios for the trackers.
    """
    valuation_ratios = []

    try:
        for tracker in trackers:
            ticker: Ticker = yf.Ticker(tracker)
            valuation_ratios_temp: pd.DataFrame = ticker.financials

            if not valuation_ratios_temp.empty:
                valuation_ratios_temp["tracker"] = tracker
                valuation_ratios.append(valuation_ratios_temp)
                logger.info(f"Successfully fetched valuation ratios for {tracker}")

        if valuation_ratios:
            valuation_ratios = pd.concat(valuation_ratios, axis=0)
            valuation_ratios.set_index(["tracker", valuation_ratios.index], inplace=True)
            logger.info(f"Successfully fetched valuation ratios for {len(trackers)} trackers")

        return valuation_ratios

    except Exception as e:
        logger.error(f"Error fetching valuation ratios for {tracker}: {str(e)}")
    raise

def get_debt_ratios(trackers: list) -> pd.DataFrame:
    """
    Fetches debt ratios for all trackers.
    :param trackers: List of trackers from the database.
    :return: debt_ratios: List of debt ratios for the trackers.
    """
    debt_ratios = []

    try:
        for tracker in trackers:
            ticker: Ticker = yf.Ticker(tracker)
            debt_ratios_temp: pd.DataFrame = ticker.balance_sheet

            if not debt_ratios_temp.empty:
                debt_ratios_temp["tracker"] = tracker
                debt_ratios.append(debt_ratios_temp)
                logger.info(f"Successfully fetched debt ratios for {tracker}")

        if debt_ratios:
            debt_ratios = pd.concat(debt_ratios, axis=0)
            debt_ratios.set_index(["tracker", debt_ratios.index], inplace=True)
            logger.info(f"Successfully fetched debt ratios for {len(trackers)} trackers")

            return debt_ratios

    except Exception as e:
        logger.error(f"Error fetching debt ratios for {tracker}: {str(e)}")
        raise


def worker(queue: Queue, results: dict, function, trackers: List) -> None:
    """Worker to execute data collection functions"""
    try:
        result = function(trackers)
        results[function.__name__] = result
    except Exception as e:
        print(f"Error in {function.__name__}: {e}")
    finally:
        queue.task_done()


def main():
    trackers = get_trackers()

    # Set up threading
    queue = Queue()
    results = {}
    threads = []

    # List of functions to run in parallel
    functions = [
        get_historical_data,
        get_profitability_ratios,
        get_liquidity_ratios,
        get_valuation_ratios,
        get_debt_ratios
    ]

    # Start a thread for each function
    for func in functions:
        t = threading.Thread(target=worker, args=(queue, results, func, trackers))
        t.start()
        threads.append(t)

    # Wait for all tasks to complete
    queue.join()

    # Wait for all threads to finish
    for t in threads:
        t.join()

    # Combine results
    combined_data = pd.concat([
        results['get_historical_data'],
        results['get_profitability_ratios'],
        results['get_liquidity_ratios'],
        results['get_valuation_ratios'],
        results['get_debt_ratios']
    ], axis=1)

    combined_data.reset_index()
    combined_data['date'] = combined_data['date'].dt.strftime('%Y-%m-%d')

    # Finalize and write data
    finalized_df = ReadyDF.finalize_trackers(combined_data)

    # Generate insert statement
    columns = ', '.join(finalized_df.columns)
    values = ', '.join(['%s' for _ in finalized_df.columns])
    insert_stmt = f"INSERT INTO financial_trackers ({columns}) VALUES ({values})"

    # Convert DataFrame to list of tuples
    data = [tuple(row) for row in finalized_df.to_numpy()]

    # Write to database
    db = DatabaseConnect()
    try:
        db.connect()
        db.cursor.executemany(insert_stmt, data)
        db.conn.commit()
        print(f"Successfully inserted {len(finalized_df)} rows")
    except Exception as e:
        db.conn.rollback()
        print(f"Error: {e}")
        raise
    finally:
        db.disconnect()

if __name__ == "__main__":
    main()