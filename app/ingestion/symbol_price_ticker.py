"""
Binance price-ticker helpers (Futures + Spot).

• One file, two sync helper functions
• Optional `symbol` argument; falls back to DEFAULT_SYMBOL
• Stand-alone demo when run as script
"""

from __future__ import annotations

import os
from typing import Dict

import requests
from dotenv import load_dotenv

# ────────────────────────── CONFIG ────────────────────────────────────────
load_dotenv()

FUTURES_BASE_URL  = os.getenv("FUTURES_BASE_URL", "https://fapi.binance.com")
SPOT_BASE_URL     = os.getenv("SPOT_BASE_URL",    "https://api.binance.com")
DEFAULT_SYMBOL    = os.getenv("DEFAULT_SYMBOL",   "BTCUSDT")  # ← EDIT HERE
symbol            = DEFAULT_SYMBOL                # ← requested line

# ────────────────────────── HELPERS ───────────────────────────────────────
def get_futures_ticker(sym: str | None = None) -> Dict:
    """GET /fapi/v1/ticker/price (USD-M Futures)."""
    sym   = (sym or symbol).upper()
    url   = f"{FUTURES_BASE_URL}/fapi/v1/ticker/price"
    resp  = requests.get(url, params={"symbol": sym}, timeout=5)
    resp.raise_for_status()
    return resp.json()


def get_spot_ticker(sym: str | None = None) -> Dict:
    """GET /api/v3/ticker/price (Spot)."""
    sym   = (sym or symbol).upper()
    url   = f"{SPOT_BASE_URL}/api/v3/ticker/price"
    resp  = requests.get(url, params={"symbol": sym}, timeout=5)
    resp.raise_for_status()
    return resp.json()

# ────────────────────────── DEMO ──────────────────────────────────────────
if __name__ == "__main__":
    print("Futures:", get_futures_ticker())
    print("Spot   :", get_spot_ticker())
