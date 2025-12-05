from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from uuid import UUID, uuid4
from enum import Enum

class ExchangeType(str, Enum):
    BINANCE = "binance"
    BINANCE_FUTURES = "binance_futures"
    COINBASE = "coinbase"
    COINBASE_PRO = "coinbase_pro"
    KRAKEN = "kraken"
    KUCOIN = "kucoin"
    BYBIT = "bybit"
    OKX = "okx"

class PermissionType(str, Enum):
    READ = "read"
    TRADE = "trade"
    WITHDRAW = "withdraw"
    FUTURES = "futures"
    MARGIN = "margin"

class ApiKeyBase(BaseModel):
    exchange: ExchangeType
    name: str = Field(..., min_length=1, max_length=100)
    api_key: str = Field(..., min_length=1)
    secret_key: str = Field(..., min_length=1)
    passphrase: Optional[str] = None
    permissions: List[PermissionType] = Field(default_factory=list)
    is_active: bool = True

class ApiKeyCreate(ApiKeyBase):
    pass

class ApiKeyUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    api_key: Optional[str] = None
    secret_key: Optional[str] = None
    passphrase: Optional[str] = None
    permissions: Optional[List[PermissionType]] = None
    is_active: Optional[bool] = None

class ApiKeyResponse(BaseModel):
    id: UUID
    user_id: UUID
    exchange: ExchangeType
    name: str
    permissions: List[PermissionType]
    is_active: bool
    last_tested: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

class ApiKeyTestResponse(BaseModel):
    success: bool
    message: str
    exchange_info: Optional[dict] = None
    error: Optional[str] = None
