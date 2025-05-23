from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import trades, candles, open_interest, websocket
from app.routes.orderbook import router  # order book router

app = FastAPI()

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# REST API routers
app.include_router(trades.router, prefix="/api/trades")
app.include_router(candles.router, prefix="/api/candles")
app.include_router(open_interest.router, prefix="/api/open-interest")
app.include_router(router, prefix="/api/orderbook")

# ✅ WebSocket router
app.include_router(websocket.router)

@app.get("/")
def root():
    return {"message": "Trading backend API is running 🚀"}
