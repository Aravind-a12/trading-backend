#!/usr/bin/env python3
"""
Startup script for Trading Backend
This script starts the FastAPI server with proper configuration.
"""

import os
import sys
import uvicorn
from dotenv import load_dotenv

def main():
    """Start the Trading Backend server."""
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    debug = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
    reload = os.getenv("RELOAD", "true").lower() in ("true", "1", "yes")
    
    print("🚀 Starting Trading Backend...")
    print(f"📍 Host: {host}")
    print(f"🔌 Port: {port}")
    print(f"🐛 Debug: {debug}")
    print(f"🔄 Reload: {reload}")
    print(f"🌐 URL: http://{host}:{port}")
    print(f"📚 API Docs: http://{host}:{port}/docs")
    
    # Check if database is configured
    if not os.getenv("DATABASE_URL"):
        print("⚠️  Warning: DATABASE_URL not set. Some features may not work.")
    
    if not os.getenv("SECRET_KEY"):
        print("⚠️  Warning: SECRET_KEY not set. Using default (not secure for production).")
    
    if not os.getenv("ENCRYPTION_KEY"):
        print("⚠️  Warning: ENCRYPTION_KEY not set. API key encryption may not work.")
    
    print("\n🎯 Starting server...")
    print("Press Ctrl+C to stop")
    
    try:
        uvicorn.run(
            "app.main:app",
            host=host,
            port=port,
            reload=reload,
            log_level="info" if debug else "warning"
        )
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
