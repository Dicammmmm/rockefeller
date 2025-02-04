import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials

# #get google sheets API
# scopes = [
#     "https://www.googleapis.com/auth/spreadsheets"
# ]
#
# creds = Credentials.from_service_account_file("Credentials.json", scopes = scopes)
# client = gspread.authorize(creds)
# sheet_id ="1o4raJIaoZo140OuAbVIWBWxBdkWRidipRcSsE5WymXA"
# sheet = client.open_by_key(sheet_id)
# values_list = sheet.sheet1.row_values(1)
#
# print(values_list)


def get_balance_sheet_history(ticker, years=4):
    """Downloads the balance sheet data for the given ticker
    for the specified number of years.

    Args:
        ticker (str): The stock ticker symbol.
        years (int, optional): The number of years of data to retrieve.
                              Defaults to 4.

    Returns:
        pandas.DataFrame: A DataFrame containing the balance sheet data,
                         or None if an error occurs.
    """
    try:
        stock = yf.Ticker(ticker)
        # Get the current year
        current_year = pd.to_datetime('today').year - 1
        # Calculate the start year
        start_year = current_year - 3

        # Download balance sheet data for each year
        balance_sheets = []  # Initialize the list here
        for year in range(start_year, current_year + 1):
            try:
                # Get the balance sheet for the specific year
                balance_sheet = stock.balance_sheet
                # Add a 'Year' column
                balance_sheet['Year'] = year
                balance_sheets.append(balance_sheet)
            except Exception as e:
                print(f"Error getting balance sheet for {year}: {e}")

        return pd.concat(balance_sheets, ignore_index=True)  # Added return statement

    except Exception as e:
        print(f"Error getting balance sheet history for {ticker}: {e}")
        return None

def get_income_statement_history(ticker, years=4):
    """Downloads the income statement data for the given ticker
    for the specified number of years.

    Args:
        ticker (str): The stock ticker symbol.
        years (int, optional): The number of years of data to retrieve.
                              Defaults to 4.

    Returns:
        pandas.DataFrame: A DataFrame containing the income statement,
                         or None if an error occurs.
    """
    try:
        stock = yf.Ticker(ticker)
        # Get the current year
        current_year = pd.to_datetime('today').year - 1
        # Calculate the start year
        start_year = current_year - (years - 1)  # Adjusted to use the 'years' parameter

        # Download income statement data for each year
        income_statements = []
        for year in range(start_year, current_year + 1):
            try:
                # Get the income statement for the specific year
                income_statement = stock.income_stmt
                # Add a 'Year' column
                income_statement['Year'] = year
                income_statements.append(income_statement)  # Append to the list
            except Exception as e:
                print(f"Error getting income statement for {year}: {e}")

        return pd.concat(income_statements, ignore_index=True)

    except Exception as e:
        print(f"Error getting income statement history for {ticker}: {e}")
        return None


def get_cash_flow_statement_history(ticker, years=4):
    """Downloads the cash flow statement data for the given ticker
    for the specified number of years.

    Args:
        ticker (str): The stock ticker symbol.
        years (int, optional): The number of years of data to retrieve.
                              Defaults to 4.

    Returns:
        pandas.DataFrame: A DataFrame containing the cash flow statement,
                         or None if an error occurs.
    """
    try:
        stock = yf.Ticker(ticker)
        # Get the current year
        current_year = pd.to_datetime('today').year - 1
        # Calculate the start year
        start_year = current_year - (years - 1)

        # Download cash flow statement data for each year
        cash_flow_statements = []
        for year in range(start_year, current_year + 1):
            try:
                # Get the cash flow statement for the specific year
                cash_flow_statement = stock.cashflow
                # Add a 'Year' column
                cash_flow_statement['Year'] = year
                cash_flow_statements.append(cash_flow_statement)
            except Exception as e:
                print(f"Error getting cash flow statement for {year}: {e}")

        return pd.concat(cash_flow_statements, ignore_index=True)

    except Exception as e:
        print(f"Error getting cash flow statement history for {ticker}: {e}")
        return None

stock = get_income_statement_history("AAPL",4)

print(stock)




