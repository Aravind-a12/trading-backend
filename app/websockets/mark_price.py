"""
Real-time Mark-Price / Funding-Rate stream.

• USD-M Futures WebSocket
• Prints every update (you can persist to Redis if desired)
"""

import asyncio, json
from typing import Optional

import websockets

WS_BASE = "wss://fstream.binance.com/ws"

# ──────────────────────────── HELPERS ──────────────────────────────────────
def stream_name(symbol: Optional[str] = None, every_1s: bool = True) -> str:
    """
    Build the stream name according to Binance docs:

        • Single symbol:  <symbol>@markPrice or <symbol>@markPrice@1s
        • All symbols :   !markPrice@arr      or !markPrice@arr@1s
    """
    speed = "@1s" if every_1s else ""
    return (
        f"{symbol.lower()}@markPrice{speed}" if symbol
        else f"!markPrice@arr{speed}"
    )

async def listen_mark_price(symbol: Optional[str] = None, every_1s: bool = True):
    uri = f"{WS_BASE}/{stream_name(symbol, every_1s)}"
    async with websockets.connect(uri) as ws:
        print(f"🔌  Connected → {uri}")
        async for msg in ws:
            data = json.loads(msg)
            print(json.dumps(data, indent=2))     # ← replace with Redis, DB, etc.

# ──────────────────────────── DEMO ─────────────────────────────────────────
if __name__ == "__main__":
    # Stream BTCUSDT mark-price every second
    asyncio.run(listen_mark_price("BTCUSDT", every_1s=True))
