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
    >>> import pandas as pd
    >>> # Create sample data inline
    >>> df = pd.DataFrame({'Column Name!': [1, 2], 'Another@Col': [3, 4]})
    >>> df = ReadyDF.normalize(df)
    >>> list(df.columns)
    ['column_name', 'another_col']

    >>> import polars as pl
    >>> df = pl.DataFrame({'Column Name!': [1, 2], 'Another@Col': [3, 4]})
    >>> df = ReadyDF.normalize(df)
    >>> df.columns
    ['column_name', 'another_col']
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

    # Modified pattern to handle consecutive special characters
    NORMALIZE_PATTERN = re.compile(r'[^a-zA-Z0-9]+')

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

            df.columns = [ReadyDF.NORMALIZE_PATTERN.sub('_', str(col).lower()).strip('_')
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

            new_columns = [ReadyDF.NORMALIZE_PATTERN.sub('_', str(col).lower()).strip('_')
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
        """
        if isinstance(df, pd.DataFrame):
            return ReadyDF._normalize_pd(df)
        elif isinstance(df, pl.DataFrame):
            return ReadyDF._normalize_pl(df)
        else:
            raise TypeError("Input must be either a pandas or polars DataFrame")

    @staticmethod
    def finalize_trackers(df: Union[pd.DataFrame, pl.DataFrame]) -> Union[pd.DataFrame, pl.DataFrame]:
        """
        Finalize the DataFrame for writing to the main database.
        Normalizes column names, reorders columns, and sets correct data types.

        Args:
            df (Union[pd.DataFrame, pl.DataFrame]): Input DataFrame to process

        Returns:
            Union[pd.DataFrame, pl.DataFrame]: Processed DataFrame with correct column
                order and data types
        """
        # First normalize the DataFrame
        df = ReadyDF.normalize(df)

        # Define column order to match the fact table
        columns_order = [
            'symbol',
            'date',

            # Historical Price Data
            'open',
            'high',
            'low',
            'close',
            'volume',
            'dividends',
            'stock_splits',

            # Profitability Ratios
            'operating_margin',
            'gross_margin',
            'net_profit_margin',
            'roa',
            'roe',
            'ebitda',

            # Liquidity Ratios
            'current_ratio',
            'quick_ratio',
            'operating_cash_flow',
            'working_capital',

            # Valuation Ratios
            'p_e',
            'p_b',
            'p_s',
            'dividend_yield',
            'eps',

            # Debt Ratios
            'debt_to_asset',
            'debt_to_equity',
            'interest_coverage_ratio'
        ]

        try:
            if isinstance(df, pl.DataFrame):
                # Define schema for Polars DataFrame
                schema = {
                    'symbol': pl.Utf8,
                    'date': pl.Utf8,
                    'open': pl.Float64,
                    'high': pl.Float64,
                    'low': pl.Float64,
                    'close': pl.Float64,
                    'volume': pl.Float64,
                    'dividends': pl.Float64,
                    'stock_splits': pl.Float64,
                    'operating_margin': pl.Float64,
                    'gross_margin': pl.Float64,
                    'net_profit_margin': pl.Float64,
                    'roa': pl.Float64,
                    'roe': pl.Float64,
                    'ebitda': pl.Float64,
                    'current_ratio': pl.Float64,
                    'quick_ratio': pl.Float64,
                    'operating_cash_flow': pl.Float64,
                    'working_capital': pl.Float64,
                    'p_e': pl.Float64,
                    'p_b': pl.Float64,
                    'p_s': pl.Float64,
                    'dividend_yield': pl.Float64,
                    'eps': pl.Float64,
                    'debt_to_asset': pl.Float64,
                    'debt_to_equity': pl.Float64,
                    'interest_coverage_ratio': pl.Float64
                }

                return df.select([pl.col(col).cast(schema[col]) for col in columns_order])

            elif isinstance(df, pd.DataFrame):
                # Define schema for pandas DataFrame
                schema = {
                    'symbol': 'string',
                    'date': 'string',
                    'open': 'float64',
                    'high': 'float64',
                    'low': 'float64',
                    'close': 'float64',
                    'volume': 'float64',
                    'dividends': 'float64',
                    'stock_splits': 'float64',
                    'operating_margin': 'float64',
                    'gross_margin': 'float64',
                    'net_profit_margin': 'float64',
                    'roa': 'float64',
                    'roe': 'float64',
                    'ebitda': 'float64',
                    'current_ratio': 'float64',
                    'quick_ratio': 'float64',
                    'operating_cash_flow': 'float64',
                    'working_capital': 'float64',
                    'p_e': 'float64',
                    'p_b': 'float64',
                    'p_s': 'float64',
                    'dividend_yield': 'float64',
                    'eps': 'float64',
                    'debt_to_asset': 'float64',
                    'debt_to_equity': 'float64',
                    'interest_coverage_ratio': 'float64'
                }

                return df[columns_order].astype(schema)

            else:
                raise TypeError("Unsupported DataFrame type")

        except Exception as e:
            logger.error(f"Error in finalizing trackers: {e}")
            raise

# Add the normalize methods to DataFrame classes when the module is imported
pd.DataFrame.normalize = ReadyDF._normalize_pd
pl.DataFrame.normalize = ReadyDF._normalize_pl