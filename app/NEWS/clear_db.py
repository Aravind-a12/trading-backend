#!/usr/bin/env python3
"""
Simple script to clear PostgreSQL database for testing
"""

from sqlalchemy import create_engine, text

# Database config
PG_USER = "postgres"
PG_PASSWORD = "bladeterminal"
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "news_scraper"

def clear_database():
    """Clear the PostgreSQL database"""
    print("🗑️ Clearing PostgreSQL database...")
    
    try:
        # Connect to postgres database
        conn_string = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/postgres"
        engine = create_engine(conn_string)
        
        # Terminate connections
        with engine.connect() as conn:
            conn.execute(text(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{PG_DATABASE}' AND pid <> pg_backend_pid()
            """))
        
        # Drop and recreate database
        autocommit_engine = create_engine(conn_string, isolation_level="AUTOCOMMIT")
        with autocommit_engine.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{PG_DATABASE}"'))
            conn.execute(text(f'CREATE DATABASE "{PG_DATABASE}"'))
        
        print("✅ Database cleared successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("=" * 50)
    print("🗑️  CLEAR DATABASE SCRIPT  🗑️")
    print("=" * 50)
    
    response = input("Clear database? (y/N): ")
    if response.lower() in ['y', 'yes']:
        clear_database()
    else:
        print("❌ Cancelled")
