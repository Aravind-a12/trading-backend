from datetime import datetime
from typing import List, Dict, Any, Optional
import json
import uuid
from app.utils.memory_storage import get_storage_client
from app.utils.encryption import encrypt_data, decrypt_data
from app.models.api_key import ExchangeType, PermissionType

async def create_api_key(user_id: str, api_key_data: Dict[str, Any]) -> Dict[str, Any]:
    """Create a new API key in storage."""
    storage_client = await get_storage_client()
    
    # Generate unique API key ID
    api_key_id = str(uuid.uuid4())
    
    # Encrypt sensitive data
    encrypted_api_key = encrypt_data(api_key_data["api_key"])
    encrypted_secret = encrypt_data(api_key_data["secret_key"])
    encrypted_passphrase = None
    if api_key_data.get("passphrase"):
        encrypted_passphrase = encrypt_data(api_key_data["passphrase"])
    
    api_key = {
        "id": api_key_id,
        "user_id": user_id,
        "exchange": api_key_data["exchange"],
        "name": api_key_data["name"],
        "api_key_encrypted": encrypted_api_key,
        "secret_key_encrypted": encrypted_secret,
        "passphrase_encrypted": encrypted_passphrase,
        "permissions": api_key_data.get("permissions", []),
        "is_active": api_key_data.get("is_active", True),
        "last_tested": None,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    }
    
    # Store API key
    await storage_client.set(f"api_key:{api_key_id}", json.dumps(api_key))
    
    # Add to user's API keys set
    await storage_client.sadd(f"user_api_keys:{user_id}", api_key_id)
    
    # Add to exchange index
    await storage_client.sadd(f"exchange_keys:{api_key_data['exchange']}", api_key_id)
    
    return api_key

async def get_user_api_keys(user_id: str) -> List[Dict[str, Any]]:
    """Get all API keys for a user."""
    storage_client = await get_storage_client()
    
    # Get all API key IDs for the user
    api_key_ids = await storage_client.smembers(f"user_api_keys:{user_id}")
    
    api_keys = []
    for api_key_id in api_key_ids:
        api_key_data = await storage_client.get(f"api_key:{api_key_id}")
        if api_key_data:
            api_key = json.loads(api_key_data)
            # Don't return encrypted data to client
            api_key.pop("api_key_encrypted", None)
            api_key.pop("secret_key_encrypted", None)
            api_key.pop("passphrase_encrypted", None)
            api_keys.append(api_key)
    
    return api_keys

async def get_api_key(api_key_id: str, user_id: str) -> Optional[Dict[str, Any]]:
    """Get a specific API key by ID."""
    storage_client = await get_storage_client()
    
    # Check if user owns this API key
    if not await storage_client.sismember(f"user_api_keys:{user_id}", api_key_id):
        return None
    
    api_key_data = await storage_client.get(f"api_key:{api_key_id}")
    if api_key_data:
        api_key = json.loads(api_key_data)
        # Don't return encrypted data to client
        api_key.pop("api_key_encrypted", None)
        api_key.pop("secret_key_encrypted", None)
        api_key.pop("passphrase_encrypted", None)
        return api_key
    
    return None

async def update_api_key(api_key_id: str, user_id: str, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Update an existing API key."""
    storage_client = await get_storage_client()
    
    # Check if user owns this API key
    if not await storage_client.sismember(f"user_api_keys:{user_id}", api_key_id):
        return None
    
    api_key_data = await storage_client.get(f"api_key:{api_key_id}")
    if not api_key_data:
        return None
    
    api_key = json.loads(api_key_data)
    
    # Update fields
    for field, value in update_data.items():
        if field in ["api_key", "secret_key", "passphrase"]:
            # Encrypt sensitive data
            if field == "api_key":
                api_key["api_key_encrypted"] = encrypt_data(value)
            elif field == "secret_key":
                api_key["secret_key_encrypted"] = encrypt_data(value)
            elif field == "passphrase":
                if value:
                    api_key["passphrase_encrypted"] = encrypt_data(value)
                else:
                    api_key["passphrase_encrypted"] = None
        else:
            api_key[field] = value
    
    api_key["updated_at"] = datetime.utcnow().isoformat()
    
    # Save updated API key
    await storage_client.set(f"api_key:{api_key_id}", json.dumps(api_key))
    
    # Don't return encrypted data to client
    api_key.pop("api_key_encrypted", None)
    api_key.pop("secret_key_encrypted", None)
    api_key.pop("passphrase_encrypted", None)
    
    return api_key

async def delete_api_key(api_key_id: str, user_id: str) -> bool:
    """Delete an API key."""
    storage_client = await get_storage_client()
    
    # Check if user owns this API key
    if not await storage_client.sismember(f"user_api_keys:{user_id}", api_key_id):
        return False
    
    # Get API key data to find exchange
    api_key_data = await storage_client.get(f"api_key:{api_key_id}")
    if api_key_data:
        api_key = json.loads(api_key_data)
        exchange = api_key.get("exchange")
        
        # Remove from exchange index
        if exchange:
            await storage_client.srem(f"exchange_keys:{exchange}", api_key_id)
    
    # Remove from user's API keys set
    await storage_client.srem(f"user_api_keys:{user_id}", api_key_id)
    
    # Delete the API key
    await storage_client.delete(f"api_key:{api_key_id}")
    
    return True

async def get_api_key_for_testing(api_key_id: str, user_id: str) -> Optional[Dict[str, Any]]:
    """Get API key with decrypted data for testing."""
    storage_client = await get_storage_client()
    
    # Check if user owns this API key
    if not await storage_client.sismember(f"user_api_keys:{user_id}", api_key_id):
        return None
    
    api_key_data = await storage_client.get(f"api_key:{api_key_id}")
    if not api_key_data:
        return None
    
    api_key = json.loads(api_key_data)
    
    # Decrypt sensitive data
    api_key["api_key"] = decrypt_data(api_key["api_key_encrypted"])
    api_key["secret_key"] = decrypt_data(api_key["secret_key_encrypted"])
    if api_key.get("passphrase_encrypted"):
        api_key["passphrase"] = decrypt_data(api_key["passphrase_encrypted"])
    
    return api_key

async def update_api_key_last_tested(api_key_id: str, user_id: str) -> bool:
    """Update the last tested timestamp for an API key."""
    storage_client = await get_storage_client()
    
    # Check if user owns this API key
    if not await storage_client.sismember(f"user_api_keys:{user_id}", api_key_id):
        return False
    
    api_key_data = await storage_client.get(f"api_key:{api_key_id}")
    if not api_key_data:
        return False
    
    api_key = json.loads(api_key_data)
    api_key["last_tested"] = datetime.utcnow().isoformat()
    api_key["updated_at"] = datetime.utcnow().isoformat()
    
    await storage_client.set(f"api_key:{api_key_id}", json.dumps(api_key))
    return True

async def toggle_api_key_status(api_key_id: str, user_id: str) -> Optional[Dict[str, Any]]:
    """Toggle API key active status."""
    storage_client = await get_storage_client()
    
    # Check if user owns this API key
    if not await storage_client.sismember(f"user_api_keys:{user_id}", api_key_id):
        return None
    
    api_key_data = await storage_client.get(f"api_key:{api_key_id}")
    if not api_key_data:
        return None
    
    api_key = json.loads(api_key_data)
    api_key["is_active"] = not api_key.get("is_active", True)
    api_key["updated_at"] = datetime.utcnow().isoformat()
    
    await storage_client.set(f"api_key:{api_key_id}", json.dumps(api_key))
    
    # Don't return encrypted data to client
    api_key.pop("api_key_encrypted", None)
    api_key.pop("secret_key_encrypted", None)
    api_key.pop("passphrase_encrypted", None)
    
    return api_key
