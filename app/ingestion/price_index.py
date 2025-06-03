"""
Binance *Margin* Price-Index helper.

• MARKET_DATA endpoints → need X-MBX-APIKEY header (no signature)
• Stand-alone demo prints one symbol if key is present
"""

from __future__ import annotations
import os, requests
from typing import Dict, List
from dotenv import load_dotenv

load_dotenv()

API_KEY   = os.getenv("API_KEY")                # ← make sure this is set
BASE_URL  = os.getenv("BINANCE_SPOT_BASE_URL", "https://api.binance.com")
ALL_PAIRS = "/sapi/v1/margin/allPairs"          # list of margin-enabled pairs
PRICE_IDX = "/sapi/v1/margin/priceIndex"        # price-index for one pair

HEADERS = {"X-MBX-APIKEY": API_KEY} if API_KEY else None


# ───────────────────────── helpers ──────────────────────────
def _json_or_error(r: requests.Response) -> Dict:
    try:
        r.raise_for_status()
    except requests.HTTPError as exc:
        detail = r.json() if "application/json" in r.headers.get("Content-Type", "") else r.text
        raise RuntimeError(f"{exc} → {detail}") from None
    return r.json()


def list_margin_pairs() -> List[str]:
    """Return every symbol that supports margin price-index."""
    if not HEADERS:
        raise RuntimeError("API_KEY missing in .env – required for margin MARKET_DATA calls")
    r = requests.get(f"{BASE_URL}{ALL_PAIRS}", headers=HEADERS, timeout=5)
    data = _json_or_error(r)
    return [item["symbol"] for item in data]


def get_margin_price_index(symbol: str) -> Dict:
    """Query `/sapi/v1/margin/priceIndex` for *one* margin-enabled symbol."""
    if not HEADERS:
        raise RuntimeError("API_KEY missing in .env – required for margin MARKET_DATA calls")
    url = f"{BASE_URL}{PRICE_IDX}"
    r   = requests.get(url, params={"symbol": symbol.upper()}, headers=HEADERS, timeout=5)
    return _json_or_error(r)


# ───────────────────────── demo ─────────────────────────────
if __name__ == "__main__":
    wanted = "BNBBTC"        # BTCUSDT is NOT margin-enabled → returns 2014
    symbols = list_margin_pairs()
    if wanted not in symbols:
        print(f"⚠️  '{wanted}' is not margin-enabled. First 10 available:", symbols[:10])
    else:
        from pprint import pprint
        pprint(get_margin_price_index(wanted))
