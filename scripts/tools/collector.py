import threading
import logging
import yfinance as yf
import pandas as pd
import concurrent.futures
from datetime import datetime, timedelta
from typing import List, Tuple, Dict
from .db_connect import DatabaseConnect


class Collector:
    def __init__(self, max_threads: int = 5, symbols_per_block: int = 100):
        # Initialize database connection
        self.db = DatabaseConnect()
        if not self.db.test_connection():
            raise ConnectionError("Failed to connect to the database")

        # Threading parameters
        self.max_threads = max_threads
        self.symbols_per_block = symbols_per_block

        # Symbol tracking
        self.n_symbols = 0
        self.symbols = []
        self.lock = threading.Lock()

    def get_active_symbols(self) -> Tuple[int, List[str]]:
        """
        Retrieves active symbols from dim_trackers table.

        Returns:
            Tuple containing:
            - Number of active symbols (int)
            - List of active symbol strings (List[str])
        """
        try:
            self.db.connect()
            self.db.cursor.execute("""
                SELECT symbol
                FROM dim_trackers
                WHERE delisted = FALSE
            """)
            symbols = [row[0] for row in self.db.cursor.fetchall()]
            self.n_symbols = len(symbols)
            self.symbols = symbols
            return self.n_symbols, self.symbols

        except Exception as e:
            self.logger.error(f"Error retrieving symbols: {e}")
            return 0, []

        finally:
            self.db.disconnect()

    def verify_symbol_status(self, symbol: str) -> bool:
        """Verifies if a symbol has recent trading data."""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=180)

            ticker = yf.Ticker(symbol)
            if symbol.endswith('W'):
                hist = ticker.history(period='5d')
            else:
                try:
                    hist = ticker.history(start=start_date, end=end_date)
                except Exception:
                    hist = ticker.history(period='5d')

            is_delisted = hist.empty

            with self.lock:
                self.db.connect()
                self.db.cursor.execute("""
                    UPDATE dim_trackers 
                    SET delisted = %s
                    WHERE symbol = %s
                """, (is_delisted, symbol))
                self.db.conn.commit()
                self.db.disconnect()

            return not is_delisted

        except Exception as e:
            print(f"Error verifying {symbol}: {e}")
            return False

    def create_symbol_blocks(self) -> List[List[str]]:
        """
        Organizes verified symbols into blocks of specified size for efficient processing.

        Think of this as creating smaller research teams, each handling a manageable
        subset of companies rather than trying to research everything at once.

        Returns:
            List of symbol blocks, where each block contains up to symbols_per_block symbols
        """
        symbol_blocks = []
        for i in range(0, len(self.symbols), self.symbols_per_block):
            block = self.symbols[i:i + self.symbols_per_block]
            symbol_blocks.append(block)
        return symbol_blocks

    def collect_financial_data(self, symbol: str) -> Dict:
        """
        Collects financial data with special handling for different security types.

        This method adapts its data collection approach based on the security type:
        - Regular stocks: Collect full financial data
        - Warrants and special securities: Collect only available trading data

        Args:
            symbol: The stock symbol to collect data for

        Returns:
            List of dictionaries containing the collected financial data
        """
        try:
            ticker = yf.Ticker(symbol)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5 * 365)  # 5 years

            # Adapt history period based on security type
            if symbol.endswith('W'):
                historical_data = ticker.history(period='5d')
            else:
                try:
                    historical_data = ticker.history(
                        start=start_date,
                        end=end_date,
                        actions=True
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to get full history for {symbol}, trying shorter period: {e}")
                    historical_data = ticker.history(period='5d')

            # Get financial info only for regular stocks
            info = {}
            if not symbol.endswith('W'):
                try:
                    info = ticker.info or {}
                except Exception as e:
                    self.logger.warning(f"Failed to get financial info for {symbol}: {e}")
                    info = {}

            # Process each day's data
            daily_records = []
            for date, row in historical_data.iterrows():
                record = {
                    'symbol': symbol,
                    'date': date,

                    # Historical Price Data (always try to get these)
                    'open': row.get('Open'),
                    'high': row.get('High'),
                    'low': row.get('Low'),
                    'close': row.get('Close'),
                    'volume': row.get('Volume'),
                    'dividends': row.get('Dividends', 0),
                    'stock_splits': row.get('Stock Splits', 0),
                }

                # Add financial ratios only for regular stocks
                if not symbol.endswith('W'):
                    record.update({
                        'operating_margin': info.get('operatingMargins'),
                        'gross_margin': info.get('grossMargins'),
                        'net_profit_margin': info.get('profitMargins'),
                        'roa': info.get('returnOnAssets'),
                        'roe': info.get('returnOnEquity'),
                        'ebitda': info.get('ebitda'),
                        'current_ratio': info.get('currentRatio'),
                        'quick_ratio': info.get('quickRatio'),
                        'operating_cash_flow': info.get('operatingCashflow'),
                        'working_capital': info.get('totalCurrentAssets', 0) -
                                           info.get('totalCurrentLiabilities', 0),
                        'p_e': info.get('forwardPE'),
                        'p_b': info.get('priceToBook'),
                        'p_s': info.get('priceToSales'),
                        'dividend_yield': info.get('dividendYield'),
                        'eps': info.get('forwardEps'),
                        'debt_to_asset': info.get('totalDebt', 0) /
                                         info.get('totalAssets', 1) if info.get('totalAssets') else None,
                        'debt_to_equity': info.get('debtToEquity'),
                        'interest_coverage_ratio': info.get('ebitda', 0) /
                                                   info.get('interestExpense', 1) if info.get(
                            'interestExpense') else None
                    })
                else:
                    # For warrants, set all financial ratios to NULL
                    record.update({
                        'operating_margin': None, 'gross_margin': None,
                        'net_profit_margin': None, 'roa': None, 'roe': None,
                        'ebitda': None, 'current_ratio': None, 'quick_ratio': None,
                        'operating_cash_flow': None, 'working_capital': None,
                        'p_e': None, 'p_b': None, 'p_s': None,
                        'dividend_yield': None, 'eps': None,
                        'debt_to_asset': None, 'debt_to_equity': None,
                        'interest_coverage_ratio': None
                    })

                daily_records.append(record)

            return daily_records

        except Exception as e:
            self.logger.error(f"Error collecting data for {symbol}: {e}")
            return None
    def process_symbol_block(self, symbol_block: List[str]) -> None:
        """
        Processes multiple symbols simultaneously using a thread pool.
        
        This method creates a team of workers (threads) that can each handle data collection
        and database writing independently. Think of it like having multiple researchers
        gathering data at the same time, where each one:
        1. Collects financial data for their assigned symbol
        2. Writes the collected data to the database
        3. Reports their progress back to the main process
        """
        def process_single_symbol(symbol: str) -> bool:
            """
            Handles the complete processing of a single symbol.
            
            This is like giving one researcher a specific company to research:
            1. They gather all available data about the company
            2. They prepare and format the data properly
            3. They safely store the data in our database
            """
            try:
                # Collect the financial data for this symbol
                financial_data = self.collect_financial_data(symbol)
                if not financial_data:
                    print(f"No data available for {symbol}")
                    return False
    
                # Write the data to the database using our thread-safe lock
                with self.lock:
                    self.write_to_database(financial_data)
                
                print(f"Successfully processed {symbol}")
                return True
                
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
                return False
    
        # Create a thread pool and submit all symbols for processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            # Submit all symbols to the thread pool at once
            future_to_symbol = {
                executor.submit(process_single_symbol, symbol): symbol 
                for symbol in symbol_block
            }
            
            # Process results as they complete (in any order)
            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    success = future.result()
                    if not success:
                        print(f"Failed to process {symbol}")
                except Exception as e:
                    print(f"Unexpected error processing {symbol}: {e}")

    def write_to_database(self, records: List[Dict]) -> None:
        """
        Writes financial data to the fact_trackers table with precise type handling.

        This method carefully converts incoming data to match our PostgreSQL schema:
        - TEXT for symbol
        - TIMESTAMP for date
        - DOUBLE PRECISION for all numeric fields

        The method includes transaction management and error handling to ensure
        data integrity, especially important when dealing with financial data.
        """
        try:
            self.db.connect()

            # Begin transaction
            with self.lock:  # Ensure thread safety
                for record in records:
                    # Prepare cleaned record with proper type handling
                    cleaned_record = {
                        # Primary key fields
                        'symbol': str(record['symbol']),
                        'date': record['date'],

                        # Price data - DOUBLE PRECISION
                        'open': float(record['open']) if record.get('open') is not None else None,
                        'high': float(record['high']) if record.get('high') is not None else None,
                        'low': float(record['low']) if record.get('low') is not None else None,
                        'close': float(record['close']) if record.get('close') is not None else None,
                        'volume': float(record['volume']) if record.get('volume') is not None else None,
                        'dividends': float(record['dividends']) if record.get('dividends') is not None else None,
                        'stock_splits': float(record['stock_splits']) if record.get(
                            'stock_splits') is not None else None,

                        # Profitability ratios - DOUBLE PRECISION
                        'operating_margin': float(record['operating_margin']) if record.get(
                            'operating_margin') is not None else None,
                        'gross_margin': float(record['gross_margin']) if record.get(
                            'gross_margin') is not None else None,
                        'net_profit_margin': float(record['net_profit_margin']) if record.get(
                            'net_profit_margin') is not None else None,
                        'roa': float(record['roa']) if record.get('roa') is not None else None,
                        'roe': float(record['roe']) if record.get('roe') is not None else None,
                        'ebitda': float(record['ebitda']) if record.get('ebitda') is not None else None,

                        # Liquidity ratios - DOUBLE PRECISION
                        'current_ratio': float(record['current_ratio']) if record.get(
                            'current_ratio') is not None else None,
                        'quick_ratio': float(record['quick_ratio']) if record.get('quick_ratio') is not None else None,
                        'operating_cash_flow': float(record['operating_cash_flow']) if record.get(
                            'operating_cash_flow') is not None else None,
                        'working_capital': float(record['working_capital']) if record.get(
                            'working_capital') is not None else None,

                        # Valuation ratios - DOUBLE PRECISION
                        'p_e': float(record['p_e']) if record.get('p_e') is not None else None,
                        'p_b': float(record['p_b']) if record.get('p_b') is not None else None,
                        'p_s': float(record['p_s']) if record.get('p_s') is not None else None,
                        'dividend_yield': float(record['dividend_yield']) if record.get(
                            'dividend_yield') is not None else None,
                        'eps': float(record['eps']) if record.get('eps') is not None else None,

                        # Debt ratios - DOUBLE PRECISION
                        'debt_to_asset': float(record['debt_to_asset']) if record.get(
                            'debt_to_asset') is not None else None,
                        'debt_to_equity': float(record['debt_to_equity']) if record.get(
                            'debt_to_equity') is not None else None,
                        'interest_coverage_ratio': float(record['interest_coverage_ratio']) if record.get(
                            'interest_coverage_ratio') is not None else None
                    }

                    # Execute the INSERT with ON CONFLICT handling
                    self.db.cursor.execute("""
                        INSERT INTO fact_trackers (
                            symbol, date,
                            open, high, low, close, volume, dividends, stock_splits,
                            operating_margin, gross_margin, net_profit_margin,
                            roa, roe, ebitda,
                            current_ratio, quick_ratio, operating_cash_flow,
                            working_capital,
                            p_e, p_b, p_s, dividend_yield, eps,
                            debt_to_asset, debt_to_equity, interest_coverage_ratio
                        ) VALUES (
                            %(symbol)s, %(date)s,
                            %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s,
                            %(dividends)s, %(stock_splits)s,
                            %(operating_margin)s, %(gross_margin)s, %(net_profit_margin)s,
                            %(roa)s, %(roe)s, %(ebitda)s,
                            %(current_ratio)s, %(quick_ratio)s, %(operating_cash_flow)s,
                            %(working_capital)s,
                            %(p_e)s, %(p_b)s, %(p_s)s, %(dividend_yield)s, %(eps)s,
                            %(debt_to_asset)s, %(debt_to_equity)s, %(interest_coverage_ratio)s
                        )
                        ON CONFLICT (symbol, date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            dividends = EXCLUDED.dividends,
                            stock_splits = EXCLUDED.stock_splits,
                            operating_margin = EXCLUDED.operating_margin,
                            gross_margin = EXCLUDED.gross_margin,
                            net_profit_margin = EXCLUDED.net_profit_margin,
                            roa = EXCLUDED.roa,
                            roe = EXCLUDED.roe,
                            ebitda = EXCLUDED.ebitda,
                            current_ratio = EXCLUDED.current_ratio,
                            quick_ratio = EXCLUDED.quick_ratio,
                            operating_cash_flow = EXCLUDED.operating_cash_flow,
                            working_capital = EXCLUDED.working_capital,
                            p_e = EXCLUDED.p_e,
                            p_b = EXCLUDED.p_b,
                            p_s = EXCLUDED.p_s,
                            dividend_yield = EXCLUDED.dividend_yield,
                            eps = EXCLUDED.eps,
                            debt_to_asset = EXCLUDED.debt_to_asset,
                            debt_to_equity = EXCLUDED.debt_to_equity,
                            interest_coverage_ratio = EXCLUDED.interest_coverage_ratio
                    """, cleaned_record)

                    self.db.conn.commit()

        except Exception as e:
            self.logger.error(f"Error writing to database: {e}")
            self.db.conn.rollback()
            raise

        finally:
            self.db.disconnect()
    
