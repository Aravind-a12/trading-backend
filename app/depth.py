import psycopg2
import time

SYMBOL = "BTCUSDT"

# % Band ranges from best bid/ask
BANDS = [
    (0, 0.01),
    (0.01, 0.025),
    (0.025, 0.05),
    (0.05, 0.10),
    (0.10, 0.25),
]

DB_CONFIG = {
    "dbname": "orderbook",
    "user": "postgres",
    "password": "Sql123#",
    "host": "localhost",
    "port": "5432"
}


def get_best_prices_with_ts():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT best_bid, best_ask, ts FROM bookticker_summary
        WHERE symbol = %s
        ORDER BY ts DESC LIMIT 1
    """, (SYMBOL,))
    row = cur.fetchone()
    conn.close()

    if row:
        best_bid, best_ask, ts = float(row[0]), float(row[1]), row[2]
        return best_bid, best_ask, ts
    return 0, 0, None


def get_orderbook_side_at_ts(side, ts):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT price, quantity FROM orderbook_levels
        WHERE symbol = %s AND side = %s AND ts <= %s
        ORDER BY ts DESC LIMIT 1000
    """, (SYMBOL, side, ts))
    rows = cur.fetchall()
    conn.close()

    return [(float(price), float(qty)) for price, qty in rows if qty >= 0.01]



def calculate_band_volumes():
    best_bid, best_ask, ts = get_best_prices_with_ts()
    if best_bid == 0 or best_ask == 0 or ts is None:
        print("❌ Best prices not available.")
        return

    bids = get_orderbook_side_at_ts('bid', ts)
    asks = get_orderbook_side_at_ts('ask', ts)

    bid_bands = [0.0] * len(BANDS)
    ask_bands = [0.0] * len(BANDS)

    # Bids
    for price, qty in bids:
        pct_diff = (best_bid - price) / best_bid
        for i, (low, high) in enumerate(BANDS):
            if low < pct_diff <= high:
                bid_bands[i] += qty * price
                break

    # Asks
    for price, qty in asks:
        pct_diff = (price - best_ask) / best_ask
        for i, (low, high) in enumerate(BANDS):
            if low < pct_diff <= high:
                ask_bands[i] += qty * price
                break

    print(f"\n📊 Depth Band Summary at {time.strftime('%X')} (ts: {ts})")
    print("Band Range      |   Bids($M) |   Asks($M) |  Delta($M) | Imbalance")
    print("-" * 70)

    for i, (low, high) in enumerate(BANDS):
        b = round(bid_bands[i] / 1_000_000, 2)
        a = round(ask_bands[i] / 1_000_000, 2)
        delta = round(b - a, 2)
        imbalance = round(b / a, 2) if a > 0 else float('inf')
        print(f"{int(low*100):>2}–{int(high*100):<4}%        | {b:>10} | {a:>10} | {delta:>10} | {imbalance:>9}")

    print("-" * 70)


if __name__ == "__main__":
    while True:
        calculate_band_volumes()
        time.sleep(1)
