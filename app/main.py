from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import trades, candles, open_interest
from app.routes.orderbook import router  # order book router
from app.routes import aggtrades  # aggtrades router
from app.routes import kline  # kline router
from app.routes import exchangeinfo # exchange info router
from app.routes import account_bal
from app.routes import all_orders
from app.routes import open_orders
from app.routes import position_info
from app.routes import trade_history

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(trades.router, prefix="/api/trades")
app.include_router(candles.router, prefix="/api/candles")
app.include_router(open_interest.router, prefix="/api/open-interest")
app.include_router(router, prefix="/api/orderbook")  # orderbook route
app.include_router(aggtrades.router, prefix="/api/aggtrades")
app.include_router(kline.router, prefix="/api/kline")
app.include_router(exchangeinfo.router, prefix="/api/exchangeinfo")
app.include_router(account_bal.router, prefix="/api/account_bal")
app.include_router(all_orders.router, prefix="/api/all_orders")
app.include_router(open_orders.router, prefix="/api/open_orders")
app.include_router(position_info.router, prefix="/api/position_info")
app.include_router(trade_history.router, prefix="/api/trade_history")

@app.get("/")
def root():
    return {"message": "Trading backend API is running 🚀"}
