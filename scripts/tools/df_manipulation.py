import re
import logging
import pandas as pd
import polars as pl
from typing import Union, Literal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cache regex pattern at module level for better performance
NORMALIZE_PATTERN = re.compile(r'[^a-zA-Z0-9_]')

def normalize_pd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize and validate column names in a pandas DataFrame.
    :args:
        df (pd.DataFrame): The DataFrame to normalize.
    :return:
        pd.DataFrame: The normalized DataFrame.
    :raises:
        TypeError: If the input is not a pandas DataFrame.
        ValueError: If the DataFrame is empty.
    :example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'Column1': [1, 2], 'Column2': [3, 4]})
        >>> result = normalize_pd(df)  # Store result without displaying
    """

    try:
        # Check if input is a Pandas DataFrame
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input must be a pandas DataFrame")

        # Check if DataFrame is empty
        elif df.empty:
            raise ValueError("Input DataFrame is empty")

        # Normalize column names
        df.columns = [NORMALIZE_PATTERN.sub('_', str(col).lower()) for col in df.columns]
        logger.info("DataFrame columns normalized successfully")

    # Raise exception if any error occurs
    except Exception as e:
        logger.error(f"Error normalizing DataFrame: {e}")
        raise

    return df

def normalize_pl(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalize and validate column names in a polars DataFrame.
    :args:
        df (pl.DataFrame): The DataFrame to normalize.
    :return:
        pl.DataFrame: The normalized DataFrame.
    :raises:
        TypeError: If the input is not a polars DataFrame.
        ValueError: If the DataFrame is empty.
    :example:
        >>> import polars as pl
        >>> df = pl.DataFrame({'Column1': [1, 2], 'Column2': [3, 4]})
        >>> result = normalize_pl(df)  # Store result without displaying
    """

    try:
        # Check if input is a Polars DataFrame
        if not isinstance(df, pl.DataFrame):
            raise TypeError("Input must be a polars DataFrame")

        # Check if DataFrame is empty
        elif df.is_empty():
            raise ValueError("Input DataFrame is empty")

        # Normalize column names
        new_columns = [NORMALIZE_PATTERN.sub('_', str(col).lower()) for col in df.columns]
        df = df.rename(dict(zip(df.columns, new_columns)))
        logger.info("DataFrame columns normalized successfully")

    # Raise exception if any error occurs
    except Exception as e:
        logger.error(f"Error normalizing DataFrame: {e}")
        raise

    return df