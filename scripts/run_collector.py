from datetime import datetime
from tools.collector import Collector
import time


def main():
    """
    Main function that orchestrates the entire data collection process.
    Instead of logging to files, this version provides console output
    for monitoring progress and errors.
    """
    print(f"\nStarting data collection process at {datetime.now()}")

    try:
        # Initialize collector with conservative thread settings
        collector = Collector(max_threads=3, symbols_per_block=50)

        # Get all active symbols
        print("\nRetrieving active symbols...")
        n_symbols, symbols = collector.get_active_symbols()
        print(f"Found {n_symbols} symbols to process")

        # Verify all symbols' status
        print("\nVerifying symbol status...")
        active_count = 0
        for symbol in symbols:
            try:
                is_active = collector.verify_symbol_status(symbol)
                if is_active:
                    active_count += 1
                if active_count % 100 == 0:  # Show progress every 100 symbols
                    print(f"Verified {active_count} active symbols")
                time.sleep(0.1)  # Respect API rate limits
            except Exception as e:
                print(f"Error verifying {symbol}: {e}")

        # Get updated list of active symbols
        n_symbols, active_symbols = collector.get_active_symbols()
        print(f"\nStarting data collection for {n_symbols} verified active symbols")

        # Process symbols in blocks
        symbol_blocks = collector.create_symbol_blocks()
        total_blocks = len(symbol_blocks)

        for block_num, symbol_block in enumerate(symbol_blocks, 1):
            print(f"\nProcessing block {block_num} of {total_blocks}")

            for symbol in symbol_block:
                try:
                    # Collect and store financial data
                    financial_data = collector.collect_financial_data(symbol)
                    if financial_data:
                        collector.write_to_database(financial_data)
                        print(f"Successfully processed {symbol}")
                    else:
                        print(f"No data collected for {symbol}")

                    time.sleep(0.5)  # Respect API rate limits

                except Exception as e:
                    print(f"Error processing {symbol}: {e}")
                    continue

            print(f"Completed block {block_num}. Taking a short break...")
            time.sleep(2)  # Break between blocks

        print("\nData collection process completed successfully!")

    except Exception as e:
        print(f"\nFatal error in data collection process: {e}")
        raise


if __name__ == "__main__":
    main()