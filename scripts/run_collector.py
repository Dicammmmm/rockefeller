from datetime import datetime
from tools.collector import Collector
import time

def main():
    """
    Orchestrates the parallel data collection process across multiple symbols.
    
    This script acts like a project manager that:
    1. Sets up the data collection team (initializes the collector)
    2. Organizes the work into manageable chunks (creates symbol blocks)
    3. Assigns work to teams (processes blocks in parallel)
    4. Monitors progress and handles any issues that arise
    """
    print(f"\nStarting parallel data collection at {datetime.now()}")

    try:
        # Initialize collector with parallel processing settings
        # We'll use 10 threads per block and process 200 symbols at a time
        collector = Collector(max_threads=10, symbols_per_block=200)
        
        # Get all symbols we need to process
        print("\nRetrieving symbols...")
        n_symbols, symbols = collector.get_active_symbols()
        print(f"Found {n_symbols} symbols to process")

        # Organize symbols into blocks for processing
        symbol_blocks = collector.create_symbol_blocks()
        total_blocks = len(symbol_blocks)
        
        print(f"\nProcessing {total_blocks} blocks with {collector.max_threads} threads per block")
        
        # Process each block with parallel threads
        for block_num, symbol_block in enumerate(symbol_blocks, 1):
            print(f"\nStarting block {block_num} of {total_blocks}")
            block_start_time = time.time()
            
            # Process this block's symbols in parallel
            collector.process_symbol_block(symbol_block)
            
            # Calculate and display block processing time
            block_duration = time.time() - block_start_time
            print(f"Completed block {block_num} in {block_duration:.2f} seconds")
            
            # Brief pause between blocks to manage resource usage
            time.sleep(1)

        print(f"\nData collection completed at {datetime.now()}")

    except Exception as e:
        print(f"\nFatal error in collection process: {e}")
        raise

if __name__ == "__main__":
    main()
