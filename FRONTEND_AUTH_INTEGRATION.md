# Frontend Authentication Integration Guide

## 🎯 **What You Need to Change in Your Frontend**

### **1. Update Authentication API Calls**

#### **Before (Old Backend):**
```javascript
// Old registration
const registerUser = async (userData) => {
  const response = await fetch('/api/auth/register', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(userData)
  });
  return response.json();
};
```

#### **After (New Enhanced Backend):**
```javascript
// New enhanced registration
const registerUser = async (userData) => {
  const response = await fetch('http://localhost:8000/api/auth/register', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      email: userData.email,
      username: userData.username,
      password: userData.password,
      first_name: userData.firstName, // New field
      last_name: userData.lastName,   // New field
    })
  });
  
  if (!response.ok) {
    throw new Error('Registration failed');
  }
  
  return response.json();
};
```

### **2. Update Login Function**

#### **Before:**
```javascript
const loginUser = async (credentials) => {
  const response = await fetch('/api/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(credentials)
  });
  return response.json();
};
```

#### **After:**
```javascript
const loginUser = async (credentials) => {
  const response = await fetch('http://localhost:8000/api/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      email: credentials.email,
      password: credentials.password
    })
  });
  
  if (!response.ok) {
    throw new Error('Login failed');
  }
  
  const data = await response.json();
  
  // Store token for 24 hours
  localStorage.setItem('auth_token', data.access_token);
  localStorage.setItem('token_type', data.token_type);
  
  return data;
};
```

### **3. Add Google Sign-In Integration**

#### **Option A: Google Sign-In Button (Recommended)**
```javascript
// Add Google Sign-In script to your HTML
// <script src="https://accounts.google.com/gsi/client" async defer></script>

const initializeGoogleSignIn = () => {
  google.accounts.id.initialize({
    client_id: 'YOUR_GOOGLE_CLIENT_ID', // Get from Google Cloud Console
    callback: handleGoogleSignIn
  });
  
  google.accounts.id.renderButton(
    document.getElementById('google-signin-button'),
    { theme: 'outline', size: 'large' }
  );
};

const handleGoogleSignIn = async (response) => {
  try {
    // Send Google credential to your backend
    const backendResponse = await fetch('http://localhost:8000/api/auth/oauth/google', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        provider: 'google',
        access_token: response.credential
      })
    });
    
    if (backendResponse.ok) {
      const data = await backendResponse.json();
      localStorage.setItem('auth_token', data.access_token);
      localStorage.setItem('token_type', data.token_type);
      
      // Redirect to dashboard
      window.location.href = '/dashboard';
    } else {
      throw new Error('Google sign-in failed');
    }
  } catch (error) {
    console.error('Google sign-in error:', error);
    alert('Google sign-in failed. Please try again.');
  }
};
```

#### **Option B: Redirect Flow**
```javascript
const handleGoogleLogin = async () => {
  try {
    // Get Google OAuth URL from backend
    const response = await fetch('http://localhost:8000/api/auth/oauth/google/url');
    const data = await response.json();
    
    // Redirect to Google
    window.location.href = data.auth_url;
  } catch (error) {
    console.error('Error getting Google OAuth URL:', error);
  }
};

// Handle callback (in your callback page)
const handleGoogleCallback = async () => {
  const urlParams = new URLSearchParams(window.location.search);
  const code = urlParams.get('code');
  const state = urlParams.get('state');
  
  if (code) {
    try {
      const response = await fetch('http://localhost:8000/api/auth/oauth/callback/google', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ code, state })
      });
      
      if (response.ok) {
        const data = await response.json();
        localStorage.setItem('auth_token', data.access_token);
        localStorage.setItem('token_type', data.token_type);
        
        // Redirect to dashboard
        window.location.href = '/dashboard';
      } else {
        throw new Error('OAuth callback failed');
      }
    } catch (error) {
      console.error('OAuth callback error:', error);
      alert('Authentication failed. Please try again.');
    }
  }
};
```

### **4. Update User Profile Management**

#### **Get Current User:**
```javascript
const getCurrentUser = async () => {
  const token = localStorage.getItem('auth_token');
  
  if (!token) {
    return null;
  }
  
  try {
    const response = await fetch('http://localhost:8000/api/auth/me', {
      headers: {
        'Authorization': `Bearer ${token}`
      }
    });
    
    if (response.ok) {
      return await response.json();
    } else {
      // Token expired, clear storage
      localStorage.removeItem('auth_token');
      localStorage.removeItem('token_type');
      return null;
    }
  } catch (error) {
    console.error('Error getting user profile:', error);
    return null;
  }
};
```

#### **Enhanced User Profile Display:**
```javascript
const displayUserProfile = (user) => {
  return {
    id: user.id,
    email: user.email,
    username: user.username,
    firstName: user.first_name,
    lastName: user.last_name,
    profilePicture: user.profile_picture,
    loginProviders: user.login_providers, // ['email', 'google']
    lastLogin: user.last_login,
    createdAt: user.created_at
  };
};
```

### **5. Update API Key Management**

#### **Create API Key:**
```javascript
const createApiKey = async (apiKeyData) => {
  const token = localStorage.getItem('auth_token');
  
  const response = await fetch('http://localhost:8000/api/keys', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`
    },
    body: JSON.stringify({
      exchange: apiKeyData.exchange,
      name: apiKeyData.name,
      api_key: apiKeyData.apiKey,
      secret_key: apiKeyData.secretKey,
      permissions: apiKeyData.permissions,
      is_active: true
    })
  });
  
  if (!response.ok) {
    throw new Error('Failed to create API key');
  }
  
  return response.json();
};
```

#### **Get User's API Keys:**
```javascript
const getUserApiKeys = async () => {
  const token = localStorage.getItem('auth_token');
  
  const response = await fetch('http://localhost:8000/api/keys', {
    headers: {
      'Authorization': `Bearer ${token}`
    }
  });
  
  if (!response.ok) {
    throw new Error('Failed to fetch API keys');
  }
  
  return response.json();
};
```

### **6. Update Authentication Context/State Management**

#### **React Context Example:**
```javascript
// AuthContext.js
import React, { createContext, useContext, useState, useEffect } from 'react';

const AuthContext = createContext();

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const [token, setToken] = useState(localStorage.getItem('auth_token'));

  useEffect(() => {
    const initializeAuth = async () => {
      if (token) {
        try {
          const userData = await getCurrentUser();
          setUser(userData);
        } catch (error) {
          console.error('Auth initialization error:', error);
          setToken(null);
          localStorage.removeItem('auth_token');
        }
      }
      setLoading(false);
    };

    initializeAuth();
  }, [token]);

  const login = async (credentials) => {
    try {
      const data = await loginUser(credentials);
      setToken(data.access_token);
      const userData = await getCurrentUser();
      setUser(userData);
      return { success: true };
    } catch (error) {
      return { success: false, error: error.message };
    }
  };

  const logout = () => {
    setUser(null);
    setToken(null);
    localStorage.removeItem('auth_token');
    localStorage.removeItem('token_type');
  };

  const value = {
    user,
    token,
    loading,
    login,
    logout,
    isAuthenticated: !!user
  };

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};
```

### **7. Update Registration Form**

#### **Enhanced Registration Form:**
```javascript
const RegistrationForm = () => {
  const [formData, setFormData] = useState({
    email: '',
    username: '',
    password: '',
    confirmPassword: '',
    firstName: '',
    lastName: ''
  });

  const handleSubmit = async (e) => {
    e.preventDefault();
    
    if (formData.password !== formData.confirmPassword) {
      alert('Passwords do not match');
      return;
    }

    try {
      const result = await registerUser(formData);
      alert('Registration successful! Please login.');
      // Redirect to login page
    } catch (error) {
      alert(`Registration failed: ${error.message}`);
    }
  };

  return (
    <form onSubmit={handleSubmit}>
      <input
        type="email"
        placeholder="Email"
        value={formData.email}
        onChange={(e) => setFormData({...formData, email: e.target.value})}
        required
      />
      <input
        type="text"
        placeholder="Username"
        value={formData.username}
        onChange={(e) => setFormData({...formData, username: e.target.value})}
        required
      />
      <input
        type="text"
        placeholder="First Name"
        value={formData.firstName}
        onChange={(e) => setFormData({...formData, firstName: e.target.value})}
      />
      <input
        type="text"
        placeholder="Last Name"
        value={formData.lastName}
        onChange={(e) => setFormData({...formData, lastName: e.target.value})}
      />
      <input
        type="password"
        placeholder="Password"
        value={formData.password}
        onChange={(e) => setFormData({...formData, password: e.target.value})}
        required
      />
      <input
        type="password"
        placeholder="Confirm Password"
        value={formData.confirmPassword}
        onChange={(e) => setFormData({...formData, confirmPassword: e.target.value})}
        required
      />
      <button type="submit">Register</button>
    </form>
  );
};
```

### **8. Update Protected Routes**

#### **Route Protection:**
```javascript
const ProtectedRoute = ({ children }) => {
  const { isAuthenticated, loading } = useAuth();

  if (loading) {
    return <div>Loading...</div>;
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" />;
  }

  return children;
};

// Usage
<Route path="/dashboard" element={
  <ProtectedRoute>
    <Dashboard />
  </ProtectedRoute>
} />
```

### **9. Environment Configuration**

#### **Create .env file in your frontend:**
```bash
# Frontend .env
REACT_APP_BACKEND_URL=http://localhost:8000
REACT_APP_GOOGLE_CLIENT_ID=your_google_client_id_here
```

#### **Use environment variables:**
```javascript
const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || 'http://localhost:8000';

const loginUser = async (credentials) => {
  const response = await fetch(`${BACKEND_URL}/api/auth/login`, {
    // ... rest of the code
  });
};
```

### **10. Error Handling**

#### **Global Error Handler:**
```javascript
const handleApiError = (error, response) => {
  if (response?.status === 401) {
    // Unauthorized - redirect to login
    localStorage.removeItem('auth_token');
    window.location.href = '/login';
  } else if (response?.status === 403) {
    // Forbidden - token expired
    alert('Session expired. Please login again.');
    localStorage.removeItem('auth_token');
    window.location.href = '/login';
  } else {
    // Other errors
    console.error('API Error:', error);
    alert('An error occurred. Please try again.');
  }
};
```

## 🚀 **Quick Migration Checklist**

- [ ] Update all API endpoints to use `http://localhost:8000`
- [ ] Add `first_name` and `last_name` fields to registration
- [ ] Update user profile display with new fields
- [ ] Implement Google Sign-In (optional)
- [ ] Update token storage and management
- [ ] Add proper error handling for 401/403 responses
- [ ] Test all authentication flows
- [ ] Update API key management to use new endpoints

## 🔧 **Testing Your Integration**

1. **Test Registration:**
   ```javascript
   // Should create user with enhanced profile
   await registerUser({
     email: 'test@example.com',
     username: 'testuser',
     password: 'password123',
     firstName: 'Test',
     lastName: 'User'
   });
   ```

2. **Test Login:**
   ```javascript
   // Should return JWT token
   const result = await loginUser({
     email: 'test@example.com',
     password: 'password123'
   });
   ```

3. **Test API Key Creation:**
   ```javascript
   // Should create API key for authenticated user
   await createApiKey({
     exchange: 'binance_futures',
     name: 'My Binance Key',
     apiKey: 'your_api_key',
     secretKey: 'your_secret_key',
     permissions: ['read', 'trade']
   });
   ```

Your frontend is now ready to work with the enhanced authentication backend! 🎉
