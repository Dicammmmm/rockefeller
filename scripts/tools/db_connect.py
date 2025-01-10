import os
import logging
import psycopg2
from datetime import datetime
from dotenv import load_dotenv


def setup_logging() -> str:
    """
    Configure logging settings based on the current environment.

    Returns:
        str: Current environment setting ('dev' or 'prod')

    Configuration details:
        - Production: DEBUG level logging
        - Development: INFO level logging
        Common format: 'timestamp - name - level - message'
    """

    env: str = os.getenv("DB_MODE", "dev")  # Default to dev if not set

    # Common logging format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    if env == "prod":
        logging.basicConfig(
            level=logging.DEBUG,
            format=log_format,
        )
    else:
        # Console only for development
        logging.basicConfig(
            level=logging.INFO,
            format=log_format
        )

    return env


class DatabaseConnect:
    """
    A class for managing PostgreSQL database connections with environment-specific configurations.

    This class handles database connection management, including environment-based setup,
    logging configuration, and connection lifecycle management.

    Methods:
        __init__(): Initialize the database connection manager
        setup_logging(): Configure logging based on environment
        setup_credentials(): Set up environment-specific database credentials
        connect(): Establish database connection
        disconnect(): Close active database connections
        test_connection(): Verify database connectivity

    Attributes:
        env (str): Current environment ('dev' or 'prod')
        logger (Logger): Logging instance for the class
        conn (psycopg2.extensions.connection): PostgreSQL connection object
        cursor (psycopg2.extensions.cursor): PostgreSQL cursor object
        db_name (str): Name of the target database
        db_user (str): Database username
        db_password (str): Database password
        db_host (str): Database host address
    """

    def __init__(self):
        """
        Initialize the DatabaseConnect instance.

        Sets up environment variables, configures logging, and initializes database
        connection attributes. No active connection is established during initialization.

        Environment variables required:
            - DB_NAME: Database name and environment indicator ('dev' or 'prod')
            - DB_USERNAME_DEV: Development database username
            - DB_PASSWORD_DEV: Development database password
            - DB_USERNAME_PROD: Production database username
            - DB_PASSWORD_PROD: Production database password
            - DB_HOST: Database host address
        """

        # Load environment variables
        load_dotenv()
        self.now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        # Setup logging and environment
        self.env = setup_logging()
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Running in {self.env} mode")

        # Set database credentials
        self.setup_credentials()

        # Initialize connection objects
        self.conn = None
        self.cursor = None

    def setup_credentials(self) -> None:
        """
        Configure database credentials based on the current environment.

        Sets the following instance attributes:
            - db_user: Environment-specific username
            - db_password: Environment-specific password
            - db_name: Database name
            - db_schema: Database schema name
            - db_host: Database host address

        The credentials are loaded from environment variables set up in the .env file.
        """

        if self.env == "prod":
            self.db_schema = os.getenv("DB_SCHEMA_PROD")
            self.db_user = os.getenv("DB_USERNAME_PROD")
            self.db_password = os.getenv("DB_PASSWORD_PROD")

        else:
            self.db_schema = os.getenv("DB_SCHEMA_DEV")
            self.db_user = os.getenv("DB_USERNAME_DEV")
            self.db_password = os.getenv("DB_PASSWORD_DEV")

        self.db_name = os.getenv("DB_NAME")
        self.db_host = os.getenv("DB_HOST")

    def connect(self) -> bool:
        """
        Establish a connection to the configured database.

        Creates both a connection and cursor object for database operations.
        Connection details are logged for debugging purposes.

        Returns:
            bool: True if connection is successfully established

        Raises:
            psycopg2.Error: If connection attempt fails

        Example:
            >>> db = DatabaseConnect()
            >>> db.connect()
            True
        """

        try:
            self.conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                options=f'-c search_path={self.db_schema}'
            )
            self.cursor = self.conn.cursor()
            self.logger.debug(f"Successfully connected to database {self.db_name}")
            return True

        except psycopg2.Error as e:
            self.logger.error(f"Failed to connect to database {self.db_name}: {str(e)}")
            raise

    def disconnect(self) -> None:
        """
        Close any active database connections and cursors.

        Safely closes both cursor and connection objects if they exist.
        Connection closure is logged for debugging purposes.

        Note:
            This method should be called when database operations are complete
            to free up database resources.
        """

        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            self.logger.debug("Database connection closed")

    def test_connection(self) -> bool:
        """
        Verify database connectivity by executing a test query.

        Attempts to connect to the database and query the prod.tickers_dim table.
        The connection is automatically closed after the test, regardless of the outcome.

        Returns:
            bool: True if connection and query are successful, False otherwise

        Example:
            >>> db = DatabaseConnect()
            >>> db.test_connection()
            True
        """

        try:
            self.connect()
            self.logger.info("Testing database connection...")
            self.cursor.execute("SELECT COUNT(*) FROM trackers_stg") # WILL RESULT IN AN ERROR IF RUN IN "DEV" MODE AS THE TABLE DOES NOT EXIST
            count = self.cursor.fetchone()[0]
            self.logger.info(f"Successfully queried tickers_stg table. Count: {count}")
            return True

        except Exception as e:
            self.logger.error(f"Database connection test failed: {str(e)}")
            return False

        finally:
            self.disconnect()
