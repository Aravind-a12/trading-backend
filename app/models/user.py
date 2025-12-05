from pydantic import BaseModel, Field, EmailStr
from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID
from enum import Enum

class LoginProvider(str, Enum):
    EMAIL = "email"
    GOOGLE = "google"
    FACEBOOK = "facebook"
    TWITTER = "twitter"
    GITHUB = "github"

class UserBase(BaseModel):
    email: EmailStr
    username: str = Field(..., min_length=3, max_length=50)
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    profile_picture: Optional[str] = None

class UserCreate(UserBase):
    password: str = Field(..., min_length=8)

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class UserResponse(UserBase):
    id: UUID
    is_active: bool
    login_providers: List[LoginProvider] = []
    last_login: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

class UserInDB(UserResponse):
    hashed_password: Optional[str] = None
    oauth_providers: Dict[str, Dict[str, Any]] = {}

class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"

class TokenData(BaseModel):
    email: Optional[str] = None

# OAuth Models
class GoogleUserInfo(BaseModel):
    id: str
    email: str
    name: str
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    picture: Optional[str] = None
    verified_email: bool = False

class OAuthLoginRequest(BaseModel):
    provider: LoginProvider
    access_token: str
    id_token: Optional[str] = None

class OAuthCallbackRequest(BaseModel):
    code: str
    state: Optional[str] = None
