# -*- coding: utf-8 -*-
"""
Configuration module for PostgreSQL Telegram News Scraper
Loads all configuration from environment variables ONLY
"""

import os
from dataclasses import dataclass
from typing import List
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
config_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(config_path)

# ==================== TELEGRAM CONFIGURATION ====================
TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
if not TELEGRAM_API_ID:
    raise ValueError("TELEGRAM_API_ID must be set in .env file")
TELEGRAM_API_ID = int(TELEGRAM_API_ID)

TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
if not TELEGRAM_API_HASH:
    raise ValueError("TELEGRAM_API_HASH must be set in .env file")

# ==================== POSTGRESQL CONFIGURATION ====================
PG_USER = os.getenv('PG_USER')
if not PG_USER:
    raise ValueError("PG_USER must be set in .env file")

PG_PASSWORD = os.getenv('PG_PASSWORD')
if not PG_PASSWORD:
    raise ValueError("PG_PASSWORD must be set in .env file")

PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DATABASE = os.getenv('PG_DATABASE', 'news_scraper')
PG_CONNECTION_STRING = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# ==================== GEMINI CONFIGURATION ====================
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY must be set in .env file")

# ==================== BOT CONFIGURATION ====================
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN must be set in .env file")

CHANNEL_USERNAME = os.getenv('CHANNEL_USERNAME', '@BladeTerminalNews')

# ==================== PROCESSING CONFIGURATION ====================
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '8'))
CONCURRENT_API_CALLS = int(os.getenv('CONCURRENT_API_CALLS', '8'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
TIMEOUT_SECONDS = int(os.getenv('TIMEOUT_SECONDS', '30'))

# ==================== SCRAPING CONFIGURATION ====================
INITIAL_SCRAPE_HOURS = int(os.getenv('INITIAL_SCRAPE_HOURS', '3'))
REAL_TIME_INTERVAL_MINUTES = int(os.getenv('REAL_TIME_INTERVAL_MINUTES', '30'))

CHANNEL_USERNAMES_STR = os.getenv('CHANNEL_USERNAMES', '')
CHANNEL_USERNAMES = CHANNEL_USERNAMES_STR.split(',') if CHANNEL_USERNAMES_STR else []

# ==================== DUPLICATE DETECTION CONFIGURATION ====================
DUPLICATE_THRESHOLD = float(os.getenv('DUPLICATE_THRESHOLD', '0.85'))
MAX_COMPARISON_NEWS = int(os.getenv('MAX_COMPARISON_NEWS', '100'))
DUPLICATE_CACHE_TTL = int(os.getenv('DUPLICATE_CACHE_TTL', '36000'))
RATE_LIMIT_CACHE_TTL = int(os.getenv('RATE_LIMIT_CACHE_TTL', '30'))

# ==================== BOT SENDING CONFIGURATION ====================
BOT_SEND_INTERVAL_MINUTES = int(os.getenv('BOT_SEND_INTERVAL_MINUTES', '1'))
BOT_MAX_MESSAGES_PER_MINUTE = int(os.getenv('BOT_MAX_MESSAGES_PER_MINUTE', '20'))

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
    """Get a summary of current configuration (without exposing secrets)"""
    return {
        "telegram_api_id": "***" + str(TELEGRAM_API_ID)[-4:] if TELEGRAM_API_ID else "NOT SET",
        "postgres_database": PG_DATABASE,
        "postgres_host": PG_HOST,
        "gemini_models": len(GEMINI_MODELS),
        "channels": len(CHANNEL_USERNAMES),
        "batch_size": BATCH_SIZE,
        "concurrent_calls": CONCURRENT_API_CALLS,
        "bot_channel": CHANNEL_USERNAME,
        "session_file": SESSION_FILE
    }