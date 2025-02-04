"""
DataFrame Column Normalization Module

This module provides functionality to normalize DataFrame column names and prepare data
for database insertion. It works with both pandas and polars DataFrames, handling
column name standardization and data type conversion appropriately.

Example:
    >>> import pandas as pd
    >>> # Create a DataFrame with messy column names
    >>> df = pd.DataFrame({'Column Name!': [1, 2], 'Another@Col': [3, 4]})
    >>> df = df.normalize()  # Standardize column names
    >>> df = df.finalize_trackers()  # Prepare for database insertion
"""

import re
import logging
import pandas as pd
import polars as pl
from typing import Union

logger = logging.getLogger(__name__)

class ReadyDF:
    """
    A utility class that extends DataFrame functionality with column normalization
    and finalization methods.
    """

    # Pattern for normalizing column names - matches any non-alphanumeric characters
    NORMALIZE_PATTERN = re.compile(r'[^a-zA-Z0-9]+')

    # Define the standard column order and types for the tracker fact table
    TRACKER_COLUMNS = [
        'tracker',              # Required string
        'date',                # Required string
        'open',                # Optional float
        'high',               # Optional float
        'low',                # Optional float
        'close',              # Optional float
        'volume',             # Optional float
        'dividends',          # Optional float
        'stock_splits',       # Optional float
        'operating_margin',   # Optional float
        'gross_margin',       # Optional float
        'net_profit_margin',  # Optional float
        'roa',               # Optional float
        'roe',               # Optional float
        'ebitda',            # Optional float
        'quick_ratio',       # Optional float
        'operating_cash_flow', # Optional float
        'working_capital',    # Optional float
        'p_e',               # Optional float
        'p_b',               # Optional float
        'p_s',               # Optional float
        'dividend_yield',     # Optional float
        'eps',               # Optional float
        'debt_to_asset',     # Optional float
        'debt_to_equity',    # Optional float
        'interest_coverage_ratio'  # Optional float
    ]

    @staticmethod
    def _normalize_pd(df: pd.DataFrame) -> pd.DataFrame:
        """
        Internal method to normalize pandas DataFrame column names.
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
    def _finalize_trackers_pd(df: pd.DataFrame) -> pd.DataFrame:
        """
        Internal method to finalize pandas DataFrame for database insertion.
        Handles required fields (tracker, date) and optional fields appropriately.
        """
        try:
            # First normalize column names
            df = ReadyDF._normalize_pd(df)

            # Create a DataFrame with the correct columns and None values
            result_df = pd.DataFrame(columns=ReadyDF.TRACKER_COLUMNS)

            # Copy existing data and enforce data types
            for col in ReadyDF.TRACKER_COLUMNS:
                if col in df.columns:
                    # Handle required string columns
                    if col in ['tracker', 'date']:
                        result_df[col] = df[col].fillna('UNKNOWN' if col == 'tracker' else '1970-01-01')
                        result_df[col] = result_df[col].astype(str)
                    else:
                        # Handle optional float columns
                        result_df[col] = pd.to_numeric(df[col], errors='coerce')

            logger.info("Pandas DataFrame finalized successfully")
            return result_df

        except Exception as e:
            logger.error(f"Error finalizing pandas DataFrame: {e}")
            raise

    @staticmethod
    def _finalize_trackers_pl(df: pl.DataFrame) -> pl.DataFrame:
        """
        Internal method to finalize polars DataFrame for database insertion.
        Handles required fields (tracker, date) and optional fields appropriately.
        """
        try:
            # First normalize column names
            df = ReadyDF._normalize_pl(df)

            # Create expressions for all columns
            expressions = []
            for col in ReadyDF.TRACKER_COLUMNS:
                if col in df.columns:
                    if col in ['tracker', 'date']:
                        # Handle required string columns
                        expr = (
                            pl.col(col)
                            .fill_null('UNKNOWN' if col == 'tracker' else '1970-01-01')
                            .cast(pl.Utf8)
                        )
                    else:
                        # Handle optional float columns
                        expr = pl.col(col).cast(pl.Float64, strict=False)
                else:
                    # Add missing columns with appropriate types and None values
                    expr = (
                        pl.lit(None)
                        .cast(pl.Utf8 if col in ['tracker', 'date'] else pl.Float64)
                        .alias(col)
                    )
                expressions.append(expr)

            # Apply all transformations and select columns in correct order
            df = df.with_columns(expressions).select(ReadyDF.TRACKER_COLUMNS)

            logger.info("Polars DataFrame finalized successfully")
            return df

        except Exception as e:
            logger.error(f"Error finalizing polars DataFrame: {e}")
            raise

    @staticmethod
    def normalize(df: Union[pd.DataFrame, pl.DataFrame]) -> Union[pd.DataFrame, pl.DataFrame]:
        """
        Normalize column names in either pandas or polars DataFrames.
        This standardizes column names by converting to lowercase and replacing
        special characters with underscores.
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
        Finalize DataFrame for database insertion by normalizing column names,
        reordering columns, and setting correct data types. Handles missing values
        appropriately, with only tracker and date being required fields.
        """
        if isinstance(df, pd.DataFrame):
            return ReadyDF._finalize_trackers_pd(df)
        elif isinstance(df, pl.DataFrame):
            return ReadyDF._finalize_trackers_pl(df)
        else:
            raise TypeError("Input must be either a pandas or polars DataFrame")

# Add instance methods to DataFrame classes
def normalize_pd(self):
    """Instance method for pandas DataFrame normalization."""
    return ReadyDF._normalize_pd(self)

def finalize_trackers_pd(self):
    """Instance method for pandas DataFrame finalization."""
    return ReadyDF._finalize_trackers_pd(self)

def normalize_pl(self):
    """Instance method for polars DataFrame normalization."""
    return ReadyDF._normalize_pl(self)

def finalize_trackers_pl(self):
    """Instance method for polars DataFrame finalization."""
    return ReadyDF._finalize_trackers_pl(self)

# Add module-level functions to pandas and polars
def pd_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Module-level function for pandas DataFrame normalization."""
    return ReadyDF._normalize_pd(df)

def pd_finalize_trackers(df: pd.DataFrame) -> pd.DataFrame:
    """Module-level function for pandas DataFrame finalization."""
    return ReadyDF._finalize_trackers_pd(df)

def pl_normalize(df: pl.DataFrame) -> pl.DataFrame:
    """Module-level function for polars DataFrame normalization."""
    return ReadyDF._normalize_pl(df)

def pl_finalize_trackers(df: pl.DataFrame) -> pl.DataFrame:
    """Module-level function for polars DataFrame finalization."""
    return ReadyDF._finalize_trackers_pl(df)

# Attach instance methods to DataFrame classes
pd.DataFrame.normalize = normalize_pd
pd.DataFrame.finalize_trackers = finalize_trackers_pd
pl.DataFrame.normalize = normalize_pl
pl.DataFrame.finalize_trackers = finalize_trackers_pl

# Attach module-level functions
pd.normalize = pd_normalize
pd.finalize_trackers = pd_finalize_trackers
pl.normalize = pl_normalize
pl.finalize_trackers = pl_finalize_trackers