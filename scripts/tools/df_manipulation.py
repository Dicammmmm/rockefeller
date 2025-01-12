"""
DataFrame Column Normalization Module

This module provides functionality to normalize DataFrame column names in both pandas
and polars DataFrames. It automatically adds a normalize() method to DataFrame 
objects when imported, allowing for seamless integration with existing DataFrame 
operations.

The normalization process:
- Converts all column names to lowercase
- Replaces special characters with underscores
- Maintains alphanumeric characters and underscores
- Preserves the original data while only modifying column names

Example:
    Basic usage with either DataFrame type:
    >>> from tools.df_manipulation import ReadyDF
    >>> import pandas as pd
    >>> df = pd.read_csv('data.csv')
    >>> df = ReadyDF.normalize(df)  # Using the unified normalize function

    Or use it directly on the DataFrame:
    >>> df = df.normalize()
"""

import re
import logging
import pandas as pd
import polars as pl
from typing import Union, Literal

# Set up logging configuration
logger = logging.getLogger(__name__)

class ReadyDF:
    """
    A utility class that extends DataFrame functionality with column normalization.

    This class provides both static normalization methods and automatically adds
    a normalize() method to DataFrame objects when imported. You can either use
    the class method ReadyDF.normalize() which automatically detects the DataFrame
    type, or use the .normalize() method directly on DataFrame objects.

    Class Attributes:
        NORMALIZE_PATTERN (re.Pattern): A compiled regular expression pattern that
            matches any characters that aren't letters, numbers, or underscores.
    """

    # The regex pattern matches any non-alphanumeric and non-underscore characters
    NORMALIZE_PATTERN = re.compile(r'[^a-zA-Z0-9_]')

    @staticmethod
    def _normalize_pd(df: pd.DataFrame) -> pd.DataFrame:
        """
        Internal method to normalize pandas DataFrame column names.

        Args:
            df (pd.DataFrame): The pandas DataFrame to normalize

        Returns:
            pd.DataFrame: DataFrame with normalized column names
        """
        try:
            if df.empty:
                raise ValueError("Input DataFrame is empty")

            df.columns = [ReadyDF.NORMALIZE_PATTERN.sub('_', str(col).lower())
                         for col in df.columns]
            logger.info("Pandas DataFrame columns normalized successfully")
            return df

        except Exception as e:
            logger.error(f"Error normalizing DataFrame: {e}")
            raise

    @staticmethod
    def _normalize_pl(df: pl.DataFrame) -> pl.DataFrame:
        """
        Internal method to normalize polars DataFrame column names.

        Args:
            df (pl.DataFrame): The polars DataFrame to normalize

        Returns:
            pl.DataFrame: DataFrame with normalized column names
        """
        try:
            if df.is_empty():
                raise ValueError("Input DataFrame is empty")

            new_columns = [ReadyDF.NORMALIZE_PATTERN.sub('_', str(col).lower())
                          for col in df.columns]
            df = df.rename(dict(zip(df.columns, new_columns)))
            logger.info("Polars DataFrame columns normalized successfully")
            return df

        except Exception as e:
            logger.error(f"Error normalizing DataFrame: {e}")
            raise

    @staticmethod
    def normalize(df: Union[pd.DataFrame, pl.DataFrame]) -> Union[pd.DataFrame, pl.DataFrame]:
        """
        Normalize column names in either pandas or polars DataFrames.

        This unified method automatically detects the DataFrame type and applies
        the appropriate normalization. It converts all column names to lowercase
        and replaces special characters with underscores.

        Args:
            df (Union[pd.DataFrame, pl.DataFrame]): The DataFrame to normalize.
                Can be either a pandas or polars DataFrame.

        Returns:
            Union[pd.DataFrame, pl.DataFrame]: The DataFrame with normalized column
                names. Returns the same type as the input DataFrame.

        Raises:
            TypeError: If the input is neither a pandas nor a polars DataFrame
            ValueError: If the DataFrame is empty
            Exception: If any error occurs during normalization

        Examples:
            Using with pandas DataFrame:
            >>> import pandas as pd
            >>> df = pd.DataFrame({'Column Name!': [1, 2], 'Another@Col': [3, 4]})
            >>> df = ReadyDF.normalize(df)
            >>> print(df.columns)
            ['column_name', 'another_col']

            Using with polars DataFrame:
            >>> import polars as pl
            >>> df = pl.DataFrame({'Column Name!': [1, 2], 'Another@Col': [3, 4]})
            >>> df = ReadyDF.normalize(df)
            >>> print(df.columns)
            ['column_name', 'another_col']
        """
        # First, determine the type of DataFrame we're working with
        if isinstance(df, pd.DataFrame):
            return ReadyDF._normalize_pd(df)
        elif isinstance(df, pl.DataFrame):
            return ReadyDF._normalize_pl(df)
        else:
            raise TypeError("Input must be either a pandas or polars DataFrame")


# Add the normalize methods to DataFrame classes when the module is imported
# This allows for direct usage of .normalize() on DataFrame objects
pd.DataFrame.normalize = ReadyDF._normalize_pd
pl.DataFrame.normalize = ReadyDF._normalize_pl