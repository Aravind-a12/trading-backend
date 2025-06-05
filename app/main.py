from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import (
    trades, candles, open_interest, aggtrades, kline, exchangeinfo, account_bal,
    all_orders, open_orders, position_info, trade_history, websocket ,order_manage # ✅ include websocket
)
from app.routes.orderbook import router as orderbook_router
from app.ingestion.user_stream import start_user_stream
import asyncio

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(trades.router, prefix="/api/trades")
app.include_router(candles.router, prefix="/api/candles")
app.include_router(open_interest.router, prefix="/api/open-interest")
app.include_router(orderbook_router, prefix="/api/orderbook")
app.include_router(aggtrades.router, prefix="/api/aggtrades")
app.include_router(kline.router, prefix="/api/kline")
app.include_router(exchangeinfo.router, prefix="/api/exchangeinfo")
app.include_router(account_bal.router, prefix="/api/account_bal")
app.include_router(all_orders.router, prefix="/api/all_orders")
app.include_router(open_orders.router, prefix="/api/open_orders")
app.include_router(position_info.router, prefix="/api/position_info")
app.include_router(trade_history.router, prefix="/api/trade_history")
app.include_router(order_manage.router, prefix="/api/order_manage")

# ✅ Include WebSocket router
app.include_router(websocket.router)

@app.get("/")
def root():
    return {"message": "Trading backend API is running 🚀"}

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(start_user_stream())
