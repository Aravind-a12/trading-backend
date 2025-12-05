from fastapi import APIRouter, Depends, HTTPException, status
from app.models.user import (
    UserCreate, UserLogin, UserResponse, Token, 
    OAuthLoginRequest, OAuthCallbackRequest, LoginProvider
)
from app.utils.redis_auth import (
    get_user_by_email, create_user, verify_password, 
    create_access_token, get_current_active_user,
    verify_google_token, create_or_update_user_from_oauth,
    update_user_last_login
)
from datetime import timedelta

router = APIRouter()

@router.post("/register", response_model=UserResponse)
async def register_user(user_data: UserCreate):
    """Register a new user."""
    # Check if user already exists
    existing_user = await get_user_by_email(user_data.email)
    
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    # Create new user
    try:
        user = await create_user(user_data.model_dump(), LoginProvider.EMAIL)
    except AttributeError:
        # Fallback for older Pydantic versions
        user = await create_user(user_data.dict(), LoginProvider.EMAIL)
    
    # Remove sensitive data from response
    user.pop("hashed_password", None)
    
    return user

@router.post("/login", response_model=Token)
async def login_user(user_credentials: UserLogin):
    """Login user and return access token."""
    # Find user by email
    user = await get_user_by_email(user_credentials.email)
    
    if not user or not verify_password(user_credentials.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if not user.get("is_active", False):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )
    
    # Update last login
    await update_user_last_login(user["email"])
    
    # Create access token
    access_token_expires = timedelta(minutes=1440)  # 24 hours
    access_token = create_access_token(
        data={"sub": user["email"]}, expires_delta=access_token_expires
    )
    
    return {"access_token": access_token, "token_type": "bearer"}

@router.get("/me", response_model=UserResponse)
async def get_current_user_info(current_user: dict = Depends(get_current_active_user)):
    """Get current user information."""
    # Remove sensitive data from response
    current_user.pop("hashed_password", None)
    return current_user

@router.post("/logout")
async def logout_user():
    """Logout user (client should discard token)."""
    return {"message": "Successfully logged out"}

# OAuth Endpoints
@router.post("/oauth/google", response_model=Token)
async def google_oauth_login(oauth_request: OAuthLoginRequest):
    """Login with Google OAuth."""
    if oauth_request.provider != LoginProvider.GOOGLE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid provider for this endpoint"
        )
    
    # Verify Google token
    google_user_info = await verify_google_token(oauth_request.access_token)
    
    if not google_user_info:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Google access token"
        )
    
    # Create or update user
    user = await create_or_update_user_from_oauth(google_user_info, LoginProvider.GOOGLE)
    
    # Create access token
    access_token_expires = timedelta(minutes=1440)  # 24 hours
    access_token = create_access_token(
        data={"sub": user["email"]}, expires_delta=access_token_expires
    )
    
    return {"access_token": access_token, "token_type": "bearer"}

@router.get("/oauth/google/url")
async def get_google_oauth_url():
    """Get Google OAuth URL for frontend to redirect to."""
    import os
    google_client_id = os.getenv("GOOGLE_CLIENT_ID")
    
    if not google_client_id:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Google OAuth not configured"
        )
    
    # Generate state parameter for security
    import secrets
    state = secrets.token_urlsafe(32)
    
    # Store state temporarily (you might want to store this in Redis with expiration)
    redirect_uri = "http://localhost:3000/auth/callback/google"  # Your frontend callback URL
    
    google_oauth_url = (
        f"https://accounts.google.com/o/oauth2/v2/auth?"
        f"client_id={google_client_id}&"
        f"redirect_uri={redirect_uri}&"
        f"scope=openid%20email%20profile&"
        f"response_type=code&"
        f"state={state}"
    )
    
    return {
        "auth_url": google_oauth_url,
        "state": state
    }

@router.post("/oauth/callback/google", response_model=Token)
async def google_oauth_callback(callback_request: OAuthCallbackRequest):
    """Handle Google OAuth callback."""
    import os
    import httpx
    
    google_client_id = os.getenv("GOOGLE_CLIENT_ID")
    google_client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
    
    if not google_client_id or not google_client_secret:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Google OAuth not configured"
        )
    
    # Exchange code for access token
    token_url = "https://oauth2.googleapis.com/token"
    redirect_uri = "http://localhost:3000/auth/callback/google"
    
    token_data = {
        "client_id": google_client_id,
        "client_secret": google_client_secret,
        "code": callback_request.code,
        "grant_type": "authorization_code",
        "redirect_uri": redirect_uri
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(token_url, data=token_data)
            
            if response.status_code == 200:
                token_response = response.json()
                access_token = token_response.get("access_token")
                
                if not access_token:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="No access token received from Google"
                    )
                
                # Get user info and create/login user
                google_user_info = await verify_google_token(access_token)
                
                if not google_user_info:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Could not verify Google user"
                    )
                
                # Create or update user
                user = await create_or_update_user_from_oauth(google_user_info, LoginProvider.GOOGLE)
                
                # Create our JWT token
                access_token_expires = timedelta(minutes=1440)  # 24 hours
                jwt_token = create_access_token(
                    data={"sub": user["email"]}, expires_delta=access_token_expires
                )
                
                return {"access_token": jwt_token, "token_type": "bearer"}
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Failed to exchange code for token"
                )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"OAuth callback error: {str(e)}"
        )
