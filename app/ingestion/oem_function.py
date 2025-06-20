import asyncio
from datetime import datetime
import json
import redis

# Redis client setup
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
MAX_OEM_LOGS = 1000  # Limit number of logs to prevent overflow

async def log_oem_device_data(oem_data: dict, receipt_timestamp: float | None = None):
    try:
        log_entry = {
            "timestamp": int(receipt_timestamp or datetime.utcnow().timestamp()),
            "received_at": datetime.utcnow().isoformat(),
            "data": oem_data
        }

        redis_client.lpush("oem_device_logs", json.dumps(log_entry))
        redis_client.ltrim("oem_device_logs", 0, MAX_OEM_LOGS - 1)
        print("✅ OEM Device Log stored:", log_entry)

    except Exception as e:
        print(f"❌ Redis Insert Error (OEM Log): {e}")

def get_oem_data_from_user():
    print("Enter OEM Device Info:")
    return {
        "device_id": input("Device ID: "),
        "part_number": input("Part Number: "),
        "status": input("Status (e.g., active/fault): "),
        "temperature": float(input("Temperature (°C): ")),
        "event_type": input("Event Type: ")
    }

# Example usage
if __name__ == "__main__":
    user_data = get_oem_data_from_user()
    asyncio.run(log_oem_device_data(user_data))