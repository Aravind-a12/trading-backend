from fastapi import APIRouter, Query
import psycopg2
import pandas as pd
from typing import List

router = APIRouter()

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="depth_data",
    user="postgres",
    password="Sql123#",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()


@router.get("/depth-deltas")
def get_depth_deltas(bands: List[float] = Query([0.001, 0.005, 0.01])):
    """
    Calculate bid/ask depth, delta, and imbalance % for multiple depth bands (TRDR-style).
    """
    # Get latest available snapshot timestamp
    cursor.execute("SELECT ts FROM orderbook_snapshots ORDER BY ts DESC LIMIT 1")
    row = cursor.fetchone()
    if not row:
        return {"error": "No snapshot found."}
    
    ts = row[0]

    # Get mid price
    cursor.execute("SELECT best_bid, best_ask FROM orderbook_snapshots WHERE ts = %s", (ts,))
    res = cursor.fetchone()
    if not res:
        return {"error": "Missing best bid/ask for snapshot."}
    best_bid, best_ask = res
    mid = (best_bid + best_ask) / 2

    # Load orderbook levels for this timestamp
    df = pd.read_sql_query(f"""
        SELECT side, price, quantity
        FROM orderbook_levels
        WHERE ts = {ts}
    """, conn)

    df["pct_diff"] = abs(df["price"] - mid) / mid

    results = []

    for band in bands:
        df_band = df[df["pct_diff"] <= band]
        bid_qty = df_band[df_band["side"] == "bid"]["quantity"].sum()
        ask_qty = df_band[df_band["side"] == "ask"]["quantity"].sum()
        total = bid_qty + ask_qty
        imbalance = ((bid_qty - ask_qty) / total * 100) if total > 0 else 0

        results.append({
            "timestamp": ts,
            "band_percent": band,
            "bid_qty": round(bid_qty, 2),
            "ask_qty": round(ask_qty, 2),
            "imbalance_percent": round(imbalance, 2),
            "delta": round(bid_qty - ask_qty, 2)
        })

    return {
        "symbol": "BTCUSDT",
        "depth_bands": results
    }
