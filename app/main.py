from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.routes import (
    trades, candles, open_interest, aggtrades, kline, exchangeinfo, account_bal,
    all_orders, open_orders, position_info, trade_history, websocket ,order_manage # ✅ include websocket
)
from app.routes.orderbook import router as orderbook_router
from app.routes.news import router as news_router
from app.ingestion.user_stream import start_user_stream
from app.NEWS.simple_news import start_news_service, stop_news_service, get_service_status
import asyncio
import os

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    enable_user_stream = os.getenv("ENABLE_USER_STREAM", "false").lower() in {"1", "true", "yes"}
    if enable_user_stream:
        asyncio.create_task(start_user_stream())
    
    # Start news service if enabled
    enable_news = os.getenv("ENABLE_NEWS", "true").lower() in {"1", "true", "yes"}
    if enable_news:
        asyncio.create_task(start_news_service())
    
    yield
    
    # Shutdown
    if enable_news:
        await stop_news_service()

app = FastAPI(lifespan=lifespan)

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

# Include News router
app.include_router(news_router, prefix="/api/news")
    
@app.get("/")
def root():
    return {"message": "Trading backend API is running 🚀"}
