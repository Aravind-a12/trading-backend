from fastapi import APIRouter
from app.utils.redis_client import redis_client
import json

router = APIRouter()

# Fetch the latest order book snapshot
@router.get("/")
def get_latest_order_book():
    order_book = redis_client.zrevrange("order_book_snapshots", 0, 0)
    if order_book:
        return json.loads(order_book[0])  # Only the most recent order book
    return {"message": "No order book snapshot available"}

# Fetch order books within a specific timestamp range
@router.get("/range")
def get_order_book_in_range(start_ts: int, end_ts: int):
    order_books = redis_client.zrangebyscore("order_book_snapshots", start_ts, end_ts)
    return [json.loads(order_book) for order_book in order_books]

# Paginate the order books data
@router.get("/paginate")
def paginate_order_book(start: int = 0, end: int = 9):
    order_books = redis_client.zrevrange("order_book_snapshots", start, end)
    return [json.loads(order_book) for order_book in order_books]
