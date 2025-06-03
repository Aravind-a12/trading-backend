"""
Fetch Binance Futures Premium-Index (mark-price / funding-rate).

• REST call -> JSON (no persistence)
• Optional symbol filter
• Stand-alone demo when run as script
"""

import os
from dotenv import load_dotenv
import requests
from typing import Union, List, Dict

# ──────────────────────────── ENV ──────────────────────────────────────────
load_dotenv()

FUTURES_BASE_URL      = os.getenv("FUTURES_BASE_URL", "https://fapi.binance.com")
PREMIUM_INDEX_ENDPOINT = os.getenv("PREMIUM_INDEX_ENDPOINT", "/fapi/v1/premiumIndex")

# ──────────────────────────── CORE ─────────────────────────────────────────
def get_premium_index(symbol: str | None = None) -> Union[Dict, List[Dict]]:
    """
    Fetch premium-index info (mark price & next funding rate).

    Args:
        symbol: optional trading pair, e.g. 'BTCUSDT'. If omitted,
                Binance returns the full list for every contract.

    Returns:
        JSON dict (single symbol) or list[dict] (all symbols)
    """
    url     = f"{FUTURES_BASE_URL}{PREMIUM_INDEX_ENDPOINT}"
    params  = {"symbol": symbol.upper()} if symbol else {}

    resp = requests.get(url, params=params, timeout=5)
    resp.raise_for_status()
    return resp.json()

# ──────────────────────────── DEMO ─────────────────────────────────────────
if __name__ == "__main__":
    data = get_premium_index("BTCUSDT")
    print(data)
