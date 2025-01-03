import pandas as pd
import polars as pl
import numpy as np
import re
from typing import Union


def normalize(df: Union[pd.DataFrame, pl.DataFrame]) -> pl.DataFrame:
    """
    Normalize the data by converting all columns to valid names.
    :param df: Input DataFrame (pandas or polars)
    :return: Normalized Polars DataFrame
    :raises TypeError: If input is not pandas or polars DataFrame
    :raises ValueError: If DataFrame is empty
    """
    if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
        raise TypeError("Input must be pandas or polars DataFrame")

    if df.is_empty():
        raise ValueError("DataFrame cannot be empty")

    # Convert to polars if pandas
    if isinstance(df, pd.DataFrame): df = pl.from_pandas(df)

    # Normalize column names
    new_columns = [re.sub(r'[\[\]\(\),\s]', '_', col.lower()).strip('_') for col in df.columns]
    df = df.select(pl.col('*')).rename(dict(zip(df.columns, new_columns)))

    return df