import asyncio
import logging
from datetime import datetime
from cryptofeed import FeedHandler
from cryptofeed.exchanges import BinanceFutures
from cryptofeed.defines import L2_BOOK

# Setup logging
logging.basicConfig(level=logging.INFO)
logging.getLogger('cryptofeed').setLevel(logging.DEBUG)

# Order book callback
async def order_book_callback(book, receipt_timestamp):
    try:
        # Directly access the bids and asks
        bids = [(float(price), float(book.book.bids[price])) for price in list(book.book.bids.keys())]
        asks = [(float(price), float(book.book.asks[price])) for price in list(book.book.asks.keys())]

        data = {
            "timestamp": datetime.utcnow().isoformat(),
            "bids": bids,
            "asks": asks
        }

        print("\n📘 Order Book Snapshot:")
        print(f"Timestamp: {data['timestamp']}")
        print(f"Bids: {data['bids'][:5]} ...")  # Show top 5 bids
        print(f"Asks: {data['asks'][:5]} ...")  # Show top 5 asks
        print("=" * 80)

    except Exception as e:
        print(f"❌ Error handling order book: {e}")

# Main function
def main():
    f = FeedHandler()
    f.add_feed(BinanceFutures(
        symbols=['BTC-USDT-PERP'],
        channels=[L2_BOOK],
        callbacks={L2_BOOK: order_book_callback}
    ))

    print("📡 Binance Futures Feed started... waiting for L2 Book data")
    f.run()

if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    main()
