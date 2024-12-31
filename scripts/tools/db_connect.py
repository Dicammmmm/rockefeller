import os
import logging
import psycopg2
from datetime import datetime
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
os.environ['PATH'] = r'C:\Program Files\PostgreSQL\17\bin;' + os.environ['PATH']
now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Check production mode and set logging level
if os.getenv("DB_NAME") == "prod":
    logging.info("Running in production mode")
    DB_USER = os.getenv("DB_USERNAME_PROD")
    DB_PASSWORD = os.getenv("DB_PASSWORD_PROD")
    DB_NAME = os.getenv("DB_NAME_PROD")
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filename=f'prod_{now}.log')

if os.getenv("DB_NAME") == "dev":
    logging.info("Running in development mode")
    DB_USER = os.getenv("DB_USERNAME_DEV")
    DB_PASSWORD = os.getenv("DB_PASSWORD_DEV")
    DB_NAME = os.getenv("DB_NAME_DEV")
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def db_connect(DB_USER: str, DB_PASSWORD: str, DB_NAME: str) -> psycopg2.extensions.connection:
    """
        Main connector to the database. Takes in environment variables and returns a connection to the database.
        :param DB_USER: Username for the database. -> String
        :param DB_PASSWORD: Password for the database. -> String
        :param DB_NAME: Name of the database. -> String
        :return: Connection to the database as 'connection'.
    """
    connection = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=os.getenv("DB_HOST")
    )

    return connection

def db_test_connection() -> bool:
    """
    Test connection to the database.
    :return: True if connection is successful, False if unsuccessful.
    """

    try:
        connection = db_connect(DB_USER, DB_PASSWORD, DB_NAME)
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM tickers")
        test = cursor.fetchone()

        if test:
            logging.info("Connection to the database successful.")
            cursor.close()
            connection.close()
            return True
        else:
            logging.error("Connection to the database failed.")
            return False

    except Exception as e:
        logging.error(f"Error connecting to the {os.getenv('DB_NAME')}.")
        logging.error(f"Error connecting to the database: {e}")

        return False

db_test_connection()