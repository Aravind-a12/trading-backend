# -*- coding: utf-8 -*-
"""
Configuration module for PostgreSQL Telegram News Scraper
Loads all configuration from environment variables
"""

import os
from dataclasses import dataclass
from typing import List
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from root .env file
config_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(config_path)

# ==================== TELEGRAM CONFIGURATION ====================
TELEGRAM_API_ID = int(os.getenv('TELEGRAM_API_ID', '20865704'))
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH', '222577f4f67263a8e7934f7d73a8c139')

# ==================== POSTGRESQL CONFIGURATION ====================
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'bladeterminal')
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DATABASE = os.getenv('PG_DATABASE', 'news_scraper')
PG_CONNECTION_STRING = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# ==================== GEMINI CONFIGURATION ====================
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', 'AIzaSyAo39_I9tMfR0uViILfRiANj4qkDoUUOpU')

# ==================== BOT CONFIGURATION ====================
BOT_TOKEN = os.getenv('BOT_TOKEN', '7658425897:AAExLPyyOoFQSiu7SqF8UkJNFtrBdaBxSa0')
CHANNEL_USERNAME = os.getenv('CHANNEL_USERNAME', '@BladeTerminalNews')

# ==================== PROCESSING CONFIGURATION ====================
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '8'))
CONCURRENT_API_CALLS = int(os.getenv('CONCURRENT_API_CALLS', '8'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
TIMEOUT_SECONDS = int(os.getenv('TIMEOUT_SECONDS', '30'))

# ==================== SCRAPING CONFIGURATION ====================
INITIAL_SCRAPE_HOURS = int(os.getenv('INITIAL_SCRAPE_HOURS', '3'))
REAL_TIME_INTERVAL_MINUTES = int(os.getenv('REAL_TIME_INTERVAL_MINUTES', '30'))
CHANNEL_USERNAMES = os.getenv('CHANNEL_USERNAMES', 'marketsAlpha,leviathan_news,infinityhedge,daytradingIG').split(',')

# ==================== DUPLICATE DETECTION CONFIGURATION ====================
DUPLICATE_THRESHOLD = float(os.getenv('DUPLICATE_THRESHOLD', '0.85'))
MAX_COMPARISON_NEWS = int(os.getenv('MAX_COMPARISON_NEWS', '100'))
DUPLICATE_CACHE_TTL = int(os.getenv('DUPLICATE_CACHE_TTL', '36000'))
RATE_LIMIT_CACHE_TTL = int(os.getenv('RATE_LIMIT_CACHE_TTL', '30'))

# ==================== BOT SENDING CONFIGURATION ====================
BOT_SEND_INTERVAL_MINUTES = int(os.getenv('BOT_SEND_INTERVAL_MINUTES', '1'))  # Check every minute
BOT_MAX_MESSAGES_PER_MINUTE = int(os.getenv('BOT_MAX_MESSAGES_PER_MINUTE', '20'))  # 20 messages per minute

# ==================== SESSION CONFIGURATION ====================
session_filename = os.getenv('SESSION_FILE', 'telegram_session.txt')
SESSION_FILE = str(Path(__file__).parent / session_filename)

# ==================== MODEL CONFIGURATION ====================
@dataclass
class ModelConfig:
    """Configuration for a Gemini model"""
    name: str
    rpm: int
    rpd: int
    priority: int
    
    def __post_init__(self):
        # Add safety buffer (80% of actual limits to prevent errors)
        self.safe_rpm = int(self.rpm * 0.8)
        self.safe_rpd = int(self.rpd * 0.8)

# Gemini model configurations
GEMINI_MODELS = [
    ModelConfig("gemini-2.5-flash", rpm=8, rpd=200, priority=1),
    ModelConfig("gemini-2.5-flash-lite", rpm=12, rpd=800, priority=2),
    ModelConfig("gemini-2.0-flash", rpm=12, rpd=150, priority=3),
    ModelConfig("gemini-2.0-flash-lite", rpm=25, rpd=180, priority=4),
    ModelConfig("gemma-3-27b-it", rpm=25, rpd=12000, priority=5),
]

MODEL_CONFIGS = {model.name: model for model in GEMINI_MODELS}

def get_config_summary() -> dict:
    """Get a summary of current configuration"""
    return {
        "telegram_api_id": TELEGRAM_API_ID,
        "postgres_database": PG_DATABASE,
        "postgres_host": PG_HOST,
        "gemini_models": len(GEMINI_MODELS),
        "channels": len(CHANNEL_USERNAMES),
        "batch_size": BATCH_SIZE,
        "concurrent_calls": CONCURRENT_API_CALLS,
        "bot_channel": CHANNEL_USERNAME,
        "session_file": SESSION_FILE
    }
