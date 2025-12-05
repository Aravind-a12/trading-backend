# Trading Backend - Redis-based API Key Management System

This document explains how to set up and use the Redis-based API Key Management system in your trading backend.

## 🚀 Features

- **User Authentication**: Secure user registration and login with JWT tokens
- **API Key Management**: Add, edit, delete, and test API keys from multiple exchanges
- **Exchange Integration**: Support for Binance, Coinbase Pro, Kraken, KuCoin, Bybit, and OKX
- **Secure Storage**: All API keys are encrypted before storing in Redis
- **Connection Testing**: Test API key connections to verify they work correctly
- **Redis-based**: Uses your existing Redis infrastructure (no PostgreSQL needed!)

## 📋 Prerequisites

- Python 3.9+
- Redis server (you already have this!)
- Your existing trading backend setup

## 🛠️ Installation

### 1. Install Dependencies

```bash
cd trading-backend
pip install -r requirements.txt
```

### 2. Environment Configuration

Create a `.env` file in the `trading-backend` directory with the following variables:

```bash
# Security Configuration
SECRET_KEY=your-super-secret-key-change-this-in-production
ENCRYPTION_KEY=your-encryption-key-for-api-keys

# Existing Trading Configuration (keep your existing keys)
API_KEY=your_binance_api_key
API_SECRET=your_binance_secret_key
FUTURES_BASE_URL=https://fapi.binance.com
ACC_BALANCE_ENDPOINT=/fapi/v2/account
ALL_ORDERS_ENDPOINT=/fapi/v1/allOrders
OPEN_ORDERS_ENDPOINT=/fapi/v1/openOrders
POSITION_RISK_ENDPOINT=/fapi/v2/positionRisk
TRADE_HISTORY_ENDPOINT=/fapi/v1/userTrades

# User Stream Configuration
ENABLE_USER_STREAM=false

# Server Configuration
HOST=0.0.0.0
PORT=8000
DEBUG=true
```

### 3. Generate Encryption Key

The system will automatically generate an encryption key if none is provided. Add the generated key to your `.env` file.

## 🏃‍♂️ Running the Backend

```bash
cd trading-backend
python start.py
# or
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## 📚 API Endpoints

### Authentication

- `POST /api/auth/register` - User registration
- `POST /api/auth/login` - User login
- `GET /api/auth/me` - Get current user info
- `POST /api/auth/logout` - User logout

### API Key Management

- `GET /api/keys` - Get all user's API keys
- `POST /api/keys` - Add new API key
- `GET /api/keys/{id}` - Get specific API key
- `PUT /api/keys/{id}` - Update API key
- `DELETE /api/keys/{id}` - Delete API key
- `GET /api/keys/{id}/test` - Test API key connection
- `PUT /api/keys/{id}/toggle` - Enable/disable API key

### Exchange Information

- `GET /api/exchanges/supported` - Get list of supported exchanges
- `GET /api/exchanges/{slug}/info` - Get exchange details
- `GET /api/exchanges/{slug}/features` - Get exchange features

## 🔐 Security Features

- **Password Hashing**: Uses bcrypt for secure password storage
- **JWT Tokens**: Secure authentication with configurable expiration
- **API Key Encryption**: All sensitive data encrypted using Fernet
- **User Isolation**: Users can only access their own API keys
- **Redis Security**: Data stored in Redis with proper key isolation

## 💡 Usage Examples

### 1. User Registration

```bash
curl -X POST "http://localhost:8000/api/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "username": "trader123",
    "password": "securepassword123"
  }'
```

### 2. User Login

```bash
curl -X POST "http://localhost:8000/api/auth/login" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "password": "securepassword123"
  }'
```

### 3. Add API Key

```bash
curl -X POST "http://localhost:8000/api/keys" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "exchange": "binance_futures",
    "name": "My Binance Futures Key",
    "api_key": "your_api_key_here",
    "secret_key": "your_secret_key_here",
    "permissions": ["read", "trade"],
    "is_active": true
  }'
```

### 4. Test API Key

```bash
curl -X GET "http://localhost:8000/api/keys/KEY_ID/test" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

## 🔧 Frontend Integration

Update your frontend `apiKeyManager.ts` to use the new backend APIs:

```typescript
export class ApiKeyManager {
  private baseUrl = 'http://localhost:8000/api';
  private token: string;

  constructor(token: string) {
    this.token = token;
  }

  async getApiKeys(): Promise<ApiKey[]> {
    const response = await fetch(`${this.baseUrl}/keys`, {
      headers: { 'Authorization': `Bearer ${this.token}` }
    });
    return response.json();
  }

  async addApiKey(apiKey: Omit<ApiKey, 'id'>): Promise<ApiKey> {
    const response = await fetch(`${this.baseUrl}/keys`, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.token}` 
      },
      body: JSON.stringify(apiKey)
    });
    return response.json();
  }

  async testApiKey(id: string): Promise<ApiKeyTestResponse> {
    const response = await fetch(`${this.baseUrl}/keys/${id}/test`, {
      headers: { 'Authorization': `Bearer ${this.token}` }
    });
    return response.json();
  }
}
```

## 🗄️ Redis Data Structure

The system uses the following Redis key patterns:

### Users
- `user:{email}` - User data by email
- `user_id:{user_id}` - User data by ID
- `users` - Set of all user IDs

### API Keys
- `api_key:{api_key_id}` - API key data
- `user_api_keys:{user_id}` - Set of API key IDs for a user
- `exchange_keys:{exchange}` - Set of API key IDs for an exchange

## 🚨 Important Notes

1. **Never commit your `.env` file** - it contains sensitive information
2. **Change default passwords** - Update the default encryption keys
3. **Use HTTPS in production** - JWT tokens should be transmitted over secure connections
4. **Redis persistence** - Ensure Redis is configured for persistence if needed
5. **Monitor Redis memory** - API keys are stored in Redis, monitor memory usage

## 🐛 Troubleshooting

### Common Issues

1. **Redis Connection Error**: Check if Redis is running and accessible
2. **Encryption Errors**: Verify your `ENCRYPTION_KEY` is properly set
3. **Import Errors**: Make sure all required packages are installed
4. **JWT Errors**: Check your `SECRET_KEY` configuration

### Debug Mode

Enable debug mode in your `.env` file to get detailed error messages:

```bash
DEBUG=true
```

## 🔄 Data Migration

If you have existing API keys in localStorage, you can migrate them:

1. Export your existing API keys from localStorage
2. Use the new API endpoints to add them to the backend
3. Update your frontend to use the new backend APIs

## 📈 Benefits of Redis-based Approach

- **No Database Setup**: Uses your existing Redis infrastructure
- **Fast Performance**: Redis is optimized for fast read/write operations
- **Simple Deployment**: No need to manage PostgreSQL databases
- **Consistent Architecture**: Matches your existing Redis usage
- **Easy Scaling**: Redis can be easily scaled horizontally

## 🤝 Support

If you encounter any issues or have questions, please check the logs and ensure all dependencies are properly installed.
