from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import json
import os
import httpx
from dotenv import load_dotenv
from app.utils.memory_storage import get_storage_client
from app.models.user import LoginProvider, GoogleUserInfo

load_dotenv()

# Security configuration
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-this-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto", bcrypt__rounds=12)

# JWT token bearer
security = HTTPBearer()

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """Hash a password."""
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Create a JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(token: str) -> Optional[str]:
    """Verify and decode a JWT token."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            return None
        return email
    except JWTError:
        return None

async def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    """Get user by email from storage."""
    storage_client = await get_storage_client()
    user_data = await storage_client.get(f"user:{email}")
    if user_data:
        return json.loads(user_data)
    return None

async def create_user(user_data: Dict[str, Any], provider: LoginProvider = LoginProvider.EMAIL) -> Dict[str, Any]:
    """Create a new user in storage."""
    storage_client = await get_storage_client()
    
    # Generate user ID (UUID format)
    import uuid
    user_id = str(uuid.uuid4())
    
    user = {
        "id": user_id,
        "email": user_data["email"],
        "username": user_data.get("username", user_data["email"].split("@")[0]),
        "first_name": user_data.get("first_name"),
        "last_name": user_data.get("last_name"),
        "profile_picture": user_data.get("profile_picture"),
        "hashed_password": get_password_hash(user_data["password"]) if user_data.get("password") else None,
        "is_active": True,
        "login_providers": [provider.value],
        "oauth_providers": {},
        "last_login": datetime.utcnow().isoformat(),
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    }
    
    # Store user by email and by ID
    await storage_client.set(f"user:{user['email']}", json.dumps(user))
    await storage_client.set(f"user_id:{user_id}", json.dumps(user))
    
    # Add to users set
    await storage_client.sadd("users", user_id)
    
    return user

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict[str, Any]:
    """Get the current authenticated user."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        token = credentials.credentials
        email = verify_token(token)
        if email is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    user = await get_user_by_email(email)
    if user is None:
        raise credentials_exception
    
    return user

async def get_current_active_user(current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    """Get the current active user."""
    if not current_user.get("is_active", False):
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user

# Google OAuth Functions
async def verify_google_token(access_token: str) -> Optional[GoogleUserInfo]:
    """Verify Google access token and get user info."""
    try:
        async with httpx.AsyncClient() as client:
            # Get user info from Google
            response = await client.get(
                f"https://www.googleapis.com/oauth2/v2/userinfo?access_token={access_token}"
            )
            
            if response.status_code == 200:
                user_data = response.json()
                return GoogleUserInfo(**user_data)
            else:
                return None
    except Exception as e:
        print(f"Error verifying Google token: {e}")
        return None

async def create_or_update_user_from_oauth(
    user_info: GoogleUserInfo, 
    provider: LoginProvider
) -> Dict[str, Any]:
    """Create or update user from OAuth provider."""
    storage_client = await get_storage_client()
    
    # Check if user exists
    existing_user = await get_user_by_email(user_info.email)
    
    if existing_user:
        # Update existing user with new provider
        if provider.value not in existing_user.get("login_providers", []):
            existing_user["login_providers"].append(provider.value)
        
        # Update OAuth provider info
        existing_user["oauth_providers"][provider.value] = {
            "id": user_info.id,
            "name": user_info.name,
            "picture": user_info.picture,
            "verified_email": user_info.verified_email,
            "last_login": datetime.utcnow().isoformat()
        }
        
        # Update profile info if not set
        if not existing_user.get("first_name") and user_info.given_name:
            existing_user["first_name"] = user_info.given_name
        if not existing_user.get("last_name") and user_info.family_name:
            existing_user["last_name"] = user_info.family_name
        if not existing_user.get("profile_picture") and user_info.picture:
            existing_user["profile_picture"] = user_info.picture
        
        existing_user["last_login"] = datetime.utcnow().isoformat()
        existing_user["updated_at"] = datetime.utcnow().isoformat()
        
        # Save updated user
        await storage_client.set(f"user:{existing_user['email']}", json.dumps(existing_user))
        await storage_client.set(f"user_id:{existing_user['id']}", json.dumps(existing_user))
        
        return existing_user
    else:
        # Create new user
        user_data = {
            "email": user_info.email,
            "username": user_info.name.replace(" ", "_").lower(),
            "first_name": user_info.given_name,
            "last_name": user_info.family_name,
            "profile_picture": user_info.picture,
            "password": None  # No password for OAuth users
        }
        
        new_user = await create_user(user_data, provider)
        
        # Add OAuth provider info
        new_user["oauth_providers"][provider.value] = {
            "id": user_info.id,
            "name": user_info.name,
            "picture": user_info.picture,
            "verified_email": user_info.verified_email,
            "last_login": datetime.utcnow().isoformat()
        }
        
        # Save user with OAuth info
        await storage_client.set(f"user:{new_user['email']}", json.dumps(new_user))
        await storage_client.set(f"user_id:{new_user['id']}", json.dumps(new_user))
        
        return new_user

async def update_user_last_login(user_email: str):
    """Update user's last login timestamp."""
    storage_client = await get_storage_client()
    user = await get_user_by_email(user_email)
    
    if user:
        user["last_login"] = datetime.utcnow().isoformat()
        user["updated_at"] = datetime.utcnow().isoformat()
        
        await storage_client.set(f"user:{user['email']}", json.dumps(user))
        await storage_client.set(f"user_id:{user['id']}", json.dumps(user))
