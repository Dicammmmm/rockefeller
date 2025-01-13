import os
import logging
import threading
import pandas as pd
import numpy as np
from tools.df_manipulation import ReadyDF
from tools.db_connect import DatabaseConnect

# Setup and test connection to the database
db = DatabaseConnect()
logger = logging.getLogger(__name__)
if db.test_connection():
    logger.info("Database connection successful")
else:
    logger.error("Database connection failed")

def get_trackers() -> list:
    symbols = []

    try:
         db.connect()
         db.cursor.execute("SELECT symbol FROM dim_trackers")
         symbols = db.cursor.fetchall()
         logger.info(f"Successfully fetched {len(symbols)} symbols from the database")

         return symbols

    except Exception as e:
         logger.error(f"Error fetching symbols from the database: {str(e)}")
         raise

    finally:
         db.disconnect()

