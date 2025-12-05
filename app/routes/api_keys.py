from fastapi import APIRouter, Depends, HTTPException, status
from typing import List
from app.models.api_key import ApiKeyCreate, ApiKeyUpdate, ApiKeyResponse, ApiKeyTestResponse
from app.utils.redis_auth import get_current_active_user
from app.utils.redis_api_keys import (
    create_api_key, get_user_api_keys as get_user_api_keys_util, get_api_key, update_api_key,
    delete_api_key, get_api_key_for_testing, update_api_key_last_tested,
    toggle_api_key_status
)
from app.utils.exchange_integration import test_api_key_connection

router = APIRouter()

@router.get("/", response_model=List[ApiKeyResponse])
async def get_user_api_keys(current_user: dict = Depends(get_current_active_user)):
    """Get all API keys for the current user."""
    api_keys = await get_user_api_keys_util(current_user["id"])
    return api_keys

@router.post("/", response_model=ApiKeyResponse)
async def create_api_key_endpoint(
    api_key_data: ApiKeyCreate,
    current_user: dict = Depends(get_current_active_user)
):
    """Create a new API key for the current user."""
    try:
        api_key = await create_api_key(current_user["id"], api_key_data.model_dump())
    except AttributeError:
        # Fallback for older Pydantic versions
        api_key = await create_api_key(current_user["id"], api_key_data.dict())
    
    # Remove encrypted data from response
    api_key.pop("api_key_encrypted", None)
    api_key.pop("secret_key_encrypted", None)
    api_key.pop("passphrase_encrypted", None)
    
    return api_key

@router.get("/{api_key_id}", response_model=ApiKeyResponse)
async def get_api_key_endpoint(
    api_key_id: str,
    current_user: dict = Depends(get_current_active_user)
):
    """Get a specific API key by ID."""
    api_key = await get_api_key(api_key_id, current_user["id"])
    
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    return api_key

@router.put("/{api_key_id}", response_model=ApiKeyResponse)
async def update_api_key_endpoint(
    api_key_id: str,
    api_key_update: ApiKeyUpdate,
    current_user: dict = Depends(get_current_active_user)
):
    """Update an existing API key."""
    update_data = api_key_update.dict(exclude_unset=True)
    api_key = await update_api_key(api_key_id, current_user["id"], update_data)
    
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    return api_key

@router.delete("/{api_key_id}")
async def delete_api_key_endpoint(
    api_key_id: str,
    current_user: dict = Depends(get_current_active_user)
):
    """Delete an API key."""
    success = await delete_api_key(api_key_id, current_user["id"])
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    return {"message": "API key deleted successfully"}

@router.get("/{api_key_id}/test", response_model=ApiKeyTestResponse)
async def test_api_key_endpoint(
    api_key_id: str,
    current_user: dict = Depends(get_current_active_user)
):
    """Test an API key connection."""
    api_key = await get_api_key_for_testing(api_key_id, current_user["id"])
    
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    # Test the connection
    test_result = await test_api_key_connection(
        api_key["exchange"],
        api_key["api_key"],
        api_key["secret_key"],
        api_key.get("passphrase")
    )
    
    # Update last tested timestamp
    await update_api_key_last_tested(api_key_id, current_user["id"])
    
    return ApiKeyTestResponse(**test_result)

@router.put("/{api_key_id}/toggle")
async def toggle_api_key_endpoint(
    api_key_id: str,
    current_user: dict = Depends(get_current_active_user)
):
    """Toggle API key active status."""
    api_key = await toggle_api_key_status(api_key_id, current_user["id"])
    
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    return {"message": f"API key {'activated' if api_key['is_active'] else 'deactivated'}"}
