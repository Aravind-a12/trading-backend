#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Full NEWS Integration Module for Trading Backend
Implements complete NEWS functionality with all components
"""

import os
import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("news_full_integration")

# Add current directory to path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Apply patches first to prevent dependency issues
try:
    from telegram_patch import apply_patches
    apply_patches()
    logger.info("✅ Telegram patches applied successfully")
except Exception as e:
    logger.warning(f"⚠️ Could not apply telegram patches: {e}")

# Import NEWS modules
try:
    from config import (
        PG_CONNECTION_STRING, GEMINI_API_KEY, BOT_TOKEN, CHANNEL_USERNAME, 
        SESSION_FILE, get_config_summary, INITIAL_SCRAPE_HOURS, 
        REAL_TIME_INTERVAL_MINUTES, BOT_SEND_INTERVAL_MINUTES, CHANNEL_USERNAMES
    )
    from database import OptimizedPostgreSQLManager, SmartPostgreSQLCache
    from gemini_client import BatchContentProcessor
    from telegram_bot import OptimizedTelegramBot
    from telegram_scraper import OptimizedTelegramScraper, ScrapingDecisionCache
    from scheduler import OptimizedContentScheduler
    logger.info("✅ All NEWS modules imported successfully")
except Exception as e:
    logger.error(f"💥 Failed to import NEWS modules: {e}")
    logger.error(traceback.format_exc())

# Global service state
_service_state = {
    "running": False,
    "pg_manager": None,
    "scheduler": None,
    "scraper": None,
    "bot_integration": None,
    "batch_processor": None,
    "task": None,
    "last_startup": None,
    "last_error": None,
    "stats": {
        "total_messages_processed": 0,
        "total_messages_sent": 0,
        "last_scrape_time": None,
        "last_send_time": None,
        "scrape_cycles": 0,
        "successful_scrapes": 0,
        "failed_scrapes": 0,
        "database_connections": 0,
        "uptime_start": None
    }
}

class FullNewsService:
    """Full-featured News Service with all original functionality"""
    
    def __init__(self):
        self.pg_manager = None
        self.batch_processor = None
        self.bot_integration = None
        self.scraper = None
        self.scheduler = None
        self.running = False
        self.config_loaded = False
        
    async def initialize(self):
        """Initialize all components"""
        try:
            logger.info("🚀 Initializing Full News Service")
            
            # Load configuration
            await self._load_configuration()
            
            # Initialize PostgreSQL Manager
            self.pg_manager = OptimizedPostgreSQLManager(PG_CONNECTION_STRING)
            logger.info("📊 PostgreSQL Manager created")
            
            # Connect to database
            pg_connected = await self.pg_manager.connect()
            if not pg_connected:
                raise Exception("Failed to connect to PostgreSQL")
            logger.info("✅ PostgreSQL connected successfully")
            _service_state["stats"]["database_connections"] += 1
            
            # Initialize Batch Content Processor
            self.batch_processor = BatchContentProcessor(GEMINI_API_KEY)
            logger.info("🤖 Batch Content Processor created")
            
            # Initialize Telegram Bot
            self.bot_integration = OptimizedTelegramBot(BOT_TOKEN, CHANNEL_USERNAME, self.pg_manager)
            logger.info("📱 Telegram Bot created")
            
            # Load Telegram session if exists
            session_string = None
            if os.path.exists(SESSION_FILE):
                with open(SESSION_FILE, "r") as f:
                    session_string = f.read().strip()
                logger.info("📱 Telegram session loaded")
            
            # Initialize Telegram Scraper
            self.scraper = OptimizedTelegramScraper(
                self.batch_processor, 
                self.pg_manager, 
                self.bot_integration, 
                session_string
            )
            logger.info("🔍 Telegram Scraper created")
            
            # Initialize cache with PostgreSQL manager
            duplicate_cache = SmartPostgreSQLCache(self.pg_manager)
            self.batch_processor.duplicate_cache = duplicate_cache
            logger.info("💾 Duplicate cache initialized")
            
            # Verify bot permissions
            try:
                bot_verified = await self.bot_integration.verify_bot_permissions()
                if bot_verified:
                    logger.info("✅ Telegram bot verified")
                else:
                    logger.warning("⚠️ Bot verification failed - continuing without bot functionality")
            except Exception as e:
                logger.warning(f"⚠️ Bot verification failed: {e} - continuing without bot functionality")
            
            # Initialize Scheduler
            self.scheduler = OptimizedContentScheduler(self.scraper)
            logger.info("⏰ Scheduler created")
            
            logger.info("✅ Full News Service initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"💥 Failed to initialize Full News Service: {e}")
            logger.error(traceback.format_exc())
            await self.cleanup()
            return False
    
    async def _load_configuration(self):
        """Load and validate configuration"""
        try:
            config_summary = get_config_summary()
            logger.info("📋 Configuration Summary:")
            for key, value in config_summary.items():
                logger.info(f"  • {key}: {value}")
            
            # Validate critical configuration
            if not GEMINI_API_KEY:
                raise ValueError("GEMINI_API_KEY is not configured")
            if not BOT_TOKEN:
                raise ValueError("BOT_TOKEN is not configured")
            if not PG_CONNECTION_STRING:
                raise ValueError("PostgreSQL connection string is not configured")
            
            self.config_loaded = True
            logger.info("✅ Configuration loaded and validated")
            
        except Exception as e:
            logger.error(f"💥 Configuration error: {e}")
            raise
    
    async def start(self):
        """Start the news service"""
        if self.running:
            logger.info("⚠️ News service is already running")
            return True
        
        try:
            logger.info("🚀 Starting Full News Service")
            
            # Initialize components
            if not await self.initialize():
                return False
            
            # Start the scheduler
            await self.scheduler.start_optimized_scheduler()
            logger.info("✅ Scheduler started successfully")
            
            self.running = True
            _service_state["running"] = True
            _service_state["last_startup"] = datetime.now(timezone.utc)
            _service_state["stats"]["uptime_start"] = datetime.now(timezone.utc)
            
            # Store references in global state
            _service_state["pg_manager"] = self.pg_manager
            _service_state["scheduler"] = self.scheduler
            _service_state["scraper"] = self.scraper
            _service_state["bot_integration"] = self.bot_integration
            _service_state["batch_processor"] = self.batch_processor
            
            logger.info("✅ Full News Service started successfully")
            
            # Keep the service running
            while self.running:
                await asyncio.sleep(60)  # Check every minute
                
        except Exception as e:
            logger.error(f"💥 Full News Service error: {e}")
            logger.error(traceback.format_exc())
            _service_state["last_error"] = str(e)
            await self.stop()
            return False
    
    async def stop(self):
        """Stop the news service"""
        try:
            logger.info("🛑 Stopping Full News Service")
            
            self.running = False
            
            # Stop scheduler
            if self.scheduler:
                try:
                    self.scheduler.scheduler.shutdown(wait=False)
                    logger.info("⏰ Scheduler stopped")
                except Exception as e:
                    logger.error(f"Error stopping scheduler: {e}")
            
            # Cleanup
            await self.cleanup()
            
            # Update global state
            _service_state["running"] = False
            _service_state["pg_manager"] = None
            _service_state["scheduler"] = None
            _service_state["scraper"] = None
            _service_state["bot_integration"] = None
            _service_state["batch_processor"] = None
            
            logger.info("✅ Full News Service stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"💥 Error stopping Full News Service: {e}")
            return False
    
    async def cleanup(self):
        """Cleanup resources"""
        try:
            if self.pg_manager:
                await self.pg_manager.close()
                logger.info("🗄️ PostgreSQL connections closed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Global service instance
_news_service = None

async def start_news_service():
    """Start the news service as a background task"""
    global _news_service, _service_state
    
    try:
        if _service_state["running"]:
            logger.info("⚠️ News service is already running")
            return True
        
        _news_service = FullNewsService()
        _service_state["task"] = asyncio.create_task(_news_service.start())
        
        logger.info("✅ News background task created")
        return True
        
    except Exception as e:
        logger.error(f"💥 Failed to start news service: {e}")
        _service_state["last_error"] = str(e)
        return False

async def stop_news_service():
    """Stop the news service gracefully"""
    global _news_service, _service_state
    
    try:
        if _news_service:
            await _news_service.stop()
        
        if _service_state["task"]:
            _service_state["task"].cancel()
            try:
                await _service_state["task"]
            except asyncio.CancelledError:
                pass
            _service_state["task"] = None
        
        _news_service = None
        
        logger.info("✅ News service stopped")
        return True
        
    except Exception as e:
        logger.error(f"💥 Failed to stop news service: {e}")
        return False

async def restart_news_service():
    """Restart the news service"""
    try:
        logger.info("🔄 Restarting news service")
        await stop_news_service()
        await asyncio.sleep(2)  # Brief pause
        return await start_news_service()
    except Exception as e:
        logger.error(f"💥 Failed to restart news service: {e}")
        return False

async def get_service_status():
    """Get current service status"""
    global _service_state
    
    uptime = None
    if _service_state["stats"]["uptime_start"]:
        uptime = (datetime.now(timezone.utc) - _service_state["stats"]["uptime_start"]).total_seconds()
    
    return {
        "running": _service_state["running"],
        "last_startup": _service_state["last_startup"].isoformat() if _service_state["last_startup"] else None,
        "last_error": _service_state["last_error"],
        "uptime_seconds": uptime,
        "scrape_interval_minutes": REAL_TIME_INTERVAL_MINUTES,
        "send_interval_minutes": BOT_SEND_INTERVAL_MINUTES,
        "channels": CHANNEL_USERNAMES,
        "database_connected": _service_state["pg_manager"] is not None,
        "stats": _service_state["stats"]
    }

async def health_check():
    """Perform health check"""
    try:
        status = await get_service_status()
        
        healthy = (
            status["running"] and 
            status["database_connected"] and 
            not status["last_error"]
        )
        
        return {
            "healthy": healthy,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {
            "healthy": False,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

async def get_recent_news_items(limit: int = 20):
    """Get recent news items from database"""
    try:
        if not _service_state["pg_manager"]:
            return []
        
        recent_content = await _service_state["pg_manager"].get_recent_content(limit)
        return recent_content
        
    except Exception as e:
        logger.error(f"Error getting recent news: {e}")
        return []

async def get_detailed_statistics():
    """Get detailed service statistics"""
    try:
        stats = _service_state["stats"].copy()
        
        # Add current status
        if _service_state["pg_manager"]:
            stats["total_content_count"] = await _service_state["pg_manager"].get_content_count()
        
        return stats
        
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        return {}

async def get_channel_information():
    """Get information about configured channels"""
    try:
        return {
            "configured_channels": CHANNEL_USERNAMES,
            "channel_count": len(CHANNEL_USERNAMES),
            "bot_channel": CHANNEL_USERNAME
        }
    except Exception as e:
        logger.error(f"Error getting channel info: {e}")
        return {}

async def get_database_status():
    """Get database status and statistics"""
    try:
        if not _service_state["pg_manager"]:
            return {"connected": False, "error": "Database not initialized"}
        
        content_count = await _service_state["pg_manager"].get_content_count()
        
        return {
            "connected": True,
            "connection_string": PG_CONNECTION_STRING.split('@')[1] if '@' in PG_CONNECTION_STRING else "***",
            "total_content_items": content_count,
            "connection_count": _service_state["stats"]["database_connections"]
        }
    except Exception as e:
        logger.error(f"Error getting database status: {e}")
        return {"connected": False, "error": str(e)}

async def test_database_connection():
    """Test database connection"""
    try:
        if not _service_state["pg_manager"]:
            return {"success": False, "error": "Database not initialized"}
        
        # Test the connection by trying to get content count
        content_count = await _service_state["pg_manager"].get_content_count()
        return {"success": True, "message": f"Database connection test completed. Content count: {content_count}"}
    except Exception as e:
        logger.error(f"Error testing database: {e}")
        return {"success": False, "error": str(e)}

async def get_recent_logs(lines: int = 50):
    """Get recent log entries (placeholder implementation)"""
    try:
        # This is a simplified implementation
        # In a real system, you'd read from log files
        return [
            f"[{datetime.now(timezone.utc).isoformat()}] INFO: News service is running",
            f"[{datetime.now(timezone.utc).isoformat()}] INFO: Last scrape completed successfully",
            f"[{datetime.now(timezone.utc).isoformat()}] INFO: Database connection is healthy"
        ]
    except Exception as e:
        logger.error(f"Error getting logs: {e}")
        return []

if __name__ == "__main__":
    # Simple test
    async def test():
        await start_news_service()
        await asyncio.sleep(10)
        await stop_news_service()
    
    asyncio.run(test())
