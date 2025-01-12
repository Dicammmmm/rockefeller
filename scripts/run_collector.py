from datetime import datetime
import time
from tools.collector import Collector


def format_time(seconds):
    """Formats seconds into hours:minutes:seconds"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def main():
    print(f"\n🚀 Starting parallel data collection at {datetime.now()}")
    start_time = time.time()

    try:
        # Initialize collector
        collector = Collector(max_threads=10, symbols_per_block=200)

        print("\n📊 Retrieving symbols...")
        n_symbols, symbols = collector.get_active_symbols()
        print(f"Found {n_symbols} symbols to process")

        # Create processing blocks
        symbol_blocks = collector.create_symbol_blocks()
        total_blocks = len(symbol_blocks)

        print(f"\n📈 Processing {total_blocks} blocks with {collector.max_threads} threads per block")

        # Process blocks
        successful_blocks = 0
        for block_num, symbol_block in enumerate(symbol_blocks, 1):
            print(f"\n🔄 Starting block {block_num} of {total_blocks}")
            print(f"Time elapsed: {format_time(time.time() - start_time)}")
            block_start_time = time.time()

            # Process block
            collector.process_symbol_block(symbol_block)

            # Show block timing
            block_duration = time.time() - block_start_time
            successful_blocks += 1

            print(f"\n✅ Completed block {block_num}")
            print(f"Block duration: {format_time(block_duration)}")
            print(f"Average time per block: {format_time((time.time() - start_time) / block_num)}")
            print(
                f"Estimated time remaining: {format_time((time.time() - start_time) / block_num * (total_blocks - block_num))}")

            # Short pause between blocks
            time.sleep(1)

        # Show final summary
        total_time = time.time() - start_time
        print(f"\n🎉 Data collection completed at {datetime.now()}")
        print(f"Total time elapsed: {format_time(total_time)}")
        print(f"Successful blocks: {successful_blocks}/{total_blocks}")
        print(f"Average time per block: {format_time(total_time / total_blocks)}")

    except Exception as e:
        print(f"\n❌ Fatal error in collection process: {e}")
        raise


if __name__ == "__main__":
    main()