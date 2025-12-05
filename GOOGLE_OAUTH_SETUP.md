# Google OAuth Setup Guide

## 🔧 **Backend Configuration**

### **1. Google Cloud Console Setup**

1. **Go to [Google Cloud Console](https://console.cloud.google.com/)**
2. **Create a new project** or select existing one
3. **Enable Google+ API**:
   - Go to "APIs & Services" > "Library"
   - Search for "Google+ API" and enable it
4. **Create OAuth 2.0 Credentials**:
   - Go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "OAuth 2.0 Client IDs"
   - Choose "Web application"
   - Add authorized redirect URIs:
     - `http://localhost:3000/auth/callback/google` (for development)
     - `https://yourdomain.com/auth/callback/google` (for production)

### **2. Environment Variables**

Add these to your `.env` file:

```bash
# Google OAuth Configuration
GOOGLE_CLIENT_ID=your_google_client_id_here
GOOGLE_CLIENT_SECRET=your_google_client_secret_here
```

### **3. Backend Endpoints**

Your backend now supports these Google OAuth endpoints:

#### **Get Google OAuth URL**
```http
GET /api/auth/oauth/google/url
```

**Response:**
```json
{
  "auth_url": "https://accounts.google.com/o/oauth2/v2/auth?...",
  "state": "random_state_string"
}
```

#### **Handle OAuth Callback**
```http
POST /api/auth/oauth/callback/google
Content-Type: application/json

{
  "code": "authorization_code_from_google",
  "state": "state_string"
}
```

**Response:**
```json
{
  "access_token": "your_jwt_token",
  "token_type": "bearer"
}
```

#### **Direct Google Login (Alternative)**
```http
POST /api/auth/oauth/google
Content-Type: application/json

{
  "provider": "google",
  "access_token": "google_access_token"
}
```

## 🎨 **Frontend Integration**

### **Option 1: Redirect Flow (Recommended)**

```javascript
// 1. Get Google OAuth URL from backend
const getGoogleAuthUrl = async () => {
  const response = await fetch('http://localhost:8000/api/auth/oauth/google/url');
  const data = await response.json();
  return data.auth_url;
};

// 2. Redirect user to Google
const handleGoogleLogin = async () => {
  const authUrl = await getGoogleAuthUrl();
  window.location.href = authUrl;
};

// 3. Handle callback (in your callback page)
const handleCallback = async () => {
  const urlParams = new URLSearchParams(window.location.search);
  const code = urlParams.get('code');
  const state = urlParams.get('state');
  
  if (code) {
    const response = await fetch('http://localhost:8000/api/auth/oauth/callback/google', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ code, state })
    });
    
    const data = await response.json();
    
    if (data.access_token) {
      // Store token and redirect to dashboard
      localStorage.setItem('token', data.access_token);
      window.location.href = '/dashboard';
    }
  }
};
```

### **Option 2: Google Sign-In Button**

```javascript
// Using Google Sign-In JavaScript SDK
const handleGoogleSignIn = async (response) => {
  const { access_token } = response.credential;
  
  const backendResponse = await fetch('http://localhost:8000/api/auth/oauth/google', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      provider: 'google',
      access_token: access_token
    })
  });
  
  const data = await backendResponse.json();
  
  if (data.access_token) {
    localStorage.setItem('token', data.access_token);
    // Redirect to dashboard
  }
};
```

## 🔐 **User Data Storage**

The backend now stores comprehensive user information:

```json
{
  "id": "user-uuid",
  "email": "user@example.com",
  "username": "username",
  "first_name": "John",
  "last_name": "Doe",
  "profile_picture": "https://lh3.googleusercontent.com/...",
  "is_active": true,
  "login_providers": ["email", "google"],
  "oauth_providers": {
    "google": {
      "id": "google_user_id",
      "name": "John Doe",
      "picture": "https://lh3.googleusercontent.com/...",
      "verified_email": true,
      "last_login": "2024-01-01T12:00:00Z"
    }
  },
  "last_login": "2024-01-01T12:00:00Z",
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-01T12:00:00Z"
}
```

## 🚀 **Testing**

1. **Start your backend server**
2. **Set up Google OAuth credentials**
3. **Test the endpoints**:

```bash
# Get OAuth URL
curl http://localhost:8000/api/auth/oauth/google/url

# Test with a valid Google access token
curl -X POST http://localhost:8000/api/auth/oauth/google \
  -H "Content-Type: application/json" \
  -d '{"provider": "google", "access_token": "your_google_access_token"}'
```

## 🔒 **Security Notes**

- **State parameter**: Used to prevent CSRF attacks
- **Token expiration**: JWT tokens expire after 24 hours
- **HTTPS required**: Use HTTPS in production
- **Client secrets**: Never expose client secrets in frontend code
- **Redirect URIs**: Only allow trusted redirect URIs

## 📱 **Mobile App Integration**

For mobile apps, use the redirect flow with custom URL schemes:

```bash
# iOS
your-app://auth/callback/google

# Android
com.yourapp://auth/callback/google
```

Update the redirect URI in Google Cloud Console accordingly.
