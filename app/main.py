import os
import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ──────────────────────────── ROUTER IMPORTS ──────────────────────────────
from app.routes import (
    trades,
    candles,
    open_interest,
    aggtrades,
    kline,
    exchangeinfo,
    account_bal,
    all_orders,
    open_orders,
    position_info,
    trade_history,
    websocket,
    order_manage,
    mark_price,         
    symbol_price_ticker,
    price_index,
)
from app.routes.orderbook import router as orderbook_router

# background WS listener
from app.ingestion.user_stream import start_user_stream

# ──────────────────────────── APP ─────────────────────────────────────────
app = FastAPI(title="Trading backend API")

# CORS (allow your front-end URL or keep the default)
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────── ROUTE REGISTRATION ─────────────────────────
app.include_router(trades.router,          prefix="/api/trades")
app.include_router(candles.router,         prefix="/api/candles")
app.include_router(open_interest.router,   prefix="/api/open-interest")
app.include_router(orderbook_router,       prefix="/api/orderbook")
app.include_router(aggtrades.router,       prefix="/api/aggtrades")
app.include_router(kline.router,           prefix="/api/kline")
app.include_router(exchangeinfo.router,    prefix="/api/exchangeinfo")
app.include_router(account_bal.router,     prefix="/api/account_bal")
app.include_router(all_orders.router,      prefix="/api/all_orders")
app.include_router(open_orders.router,     prefix="/api/open_orders")
app.include_router(position_info.router,   prefix="/api/position_info")
app.include_router(trade_history.router,   prefix="/api/trade_history")
app.include_router(order_manage.router,    prefix="/api/order_manage")
app.include_router(mark_price.router,      prefix="/api/mark_price")   # ← NEW
app.include_router(websocket.router)       # bare WS endpoints
app.include_router(symbol_price_ticker.router)          # ← NEW
app.include_router(price_index.router)  
# ──────────────────────────── ROOT & STARTUP ──────────────────────────────
@app.get("/")
def root():
    return {"message": "Trading backend API is running 🚀"}

@app.on_event("startup")
async def _on_startup() -> None:
    """Launch background Binance user-data stream."""
    asyncio.create_task(start_user_stream())

# ──────────────────────────── CLI ENTRY POINT ────────────────────────────
def run() -> None:
    """
    Spin up Uvicorn.

    Makes `python -m app.main` work, and is re-exported by app/__main__.py
    so you can also just `python -m app`.
    """
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=True)

if __name__ == "__main__":
    run()
