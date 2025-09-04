"""
Telegram scraper for PostgreSQL Telegram News Scraper
Handles all Telegram scraping operations with intelligent decision making
"""

import os
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any, Tuple
from telethon import TelegramClient, events
from telethon.errors import ChannelPrivateError
from telethon.sessions import StringSession

from config import TELEGRAM_API_ID, TELEGRAM_API_HASH, SESSION_FILE, CHANNEL_USERNAMES, INITIAL_SCRAPE_HOURS, REAL_TIME_INTERVAL_MINUTES, MAX_COMPARISON_NEWS
from database import SmartPostgreSQLCache

logger = logging.getLogger("news_scraper")

# ==================== INTELLIGENT SCRAPING DECISION CACHE ====================
class ScrapingDecisionCache:
    """Lightweight cache for scraping decisions"""
    
    def __init__(self, ttl_seconds: int = 300):  # 5 minute cache
        self.ttl = ttl_seconds
        self.cache = {}
        self.last_check_time = None
        self.last_db_state = None
    
    def should_check_database(self) -> bool:
        """Check if we need to query database"""
        now = time.time()
        if self.last_check_time is None:
            return True
        return (now - self.last_check_time) >= self.ttl
    
    def cache_decision(self, should_scrape: bool, next_check_time: datetime, reason: str):
        """Cache scraping decision"""
        self.cache = {
            "should_scrape": should_scrape,
            "next_check_time": next_check_time,
            "reason": reason,
            "cached_at": time.time()
        }
        self.last_check_time = time.time()
    
    def get_cached_decision(self) -> Optional[Dict]:
        """Get cached decision if valid"""
        if not self.cache:
            return None
        
        cached_at = self.cache.get("cached_at", 0)
        if (time.time() - cached_at) < self.ttl:
            return self.cache
        
        return None

class OptimizedTelegramScraper:
    """Telegram scraper with batch processing"""
    
    def __init__(self, batch_processor, pg_manager, bot_integration, session_str: str = None):
        self.processor = batch_processor
        self.pg_manager = pg_manager
        self.bot_integration = bot_integration  # Pass bot integration as parameter
        self.pending_messages = []
        
        # Set up duplicate cache for processor
        self.duplicate_cache = SmartPostgreSQLCache(pg_manager)
        if self.processor:
            self.processor.duplicate_cache = self.duplicate_cache
        
        # Initialize Telegram client
        if session_str:
            self.client = TelegramClient(StringSession(session_str), TELEGRAM_API_ID, TELEGRAM_API_HASH)
        else:
            self.client = TelegramClient(StringSession(), TELEGRAM_API_ID, TELEGRAM_API_HASH)
        
        self.channels = {}
        logger.info("✅ Telegram Scraper initialized")
    
    async def connect(self):
        """Connect to Telegram and channels with robust session handling"""
        try:
            # First, ensure client is connected
            if not self.client.is_connected():
                await self.client.connect()
                logger.info("🔌 Telegram client connected")
            
            # Enhanced authentication with session validation
            if await self.client.is_user_authorized():
                logger.info("✅ Using existing valid session")
            else:
                logger.info("🔐 Session invalid or missing, starting fresh authentication")
                # For production trading backend, we should use bot token authentication
                # But if user auth is needed, it will prompt appropriately
                await self.client.start()
            
            logger.info("✅ Telegram authentication successful")
            
            # Always update session file after successful connection
            session_data = self.client.session.save()
            if session_data:
                with open(SESSION_FILE, "w") as f:
                    f.write(session_data)
                logger.info("🔗 Session updated and saved")
            
            # Connect to channels with enhanced error handling
            connected_channels = 0
            failed_channels = []
            
            for username in CHANNEL_USERNAMES:
                try:
                    # Add @ prefix if not present
                    channel_name = username if username.startswith('@') else f'@{username}'
                    entity = await self.client.get_entity(channel_name)
                    self.channels[username] = entity
                    connected_channels += 1
                    logger.info(f"✅ Connected to {channel_name}")
                except Exception as e:
                    failed_channels.append(username)
                    logger.warning(f"⚠️ Failed to connect to @{username}: {e}")
            
            if failed_channels:
                logger.warning(f"⚠️ Could not connect to {len(failed_channels)} channels: {failed_channels}")
            
            if connected_channels == 0:
                logger.error("💥 Failed to connect to any channels")
                return False
            
            logger.info(f"✅ Connected to {connected_channels}/{len(CHANNEL_USERNAMES)} channels")
            return True
            
        except Exception as e:
            logger.error(f"💥 Telegram connection failed: {e}")
            return False
    
    async def determine_scrape_strategy(self) -> Tuple[str, Optional[datetime], int, bool]:
        """PERFECT SCRAPING LOGIC - as specified by user requirements"""
        has_content = await self.pg_manager.has_content()
        last_scrape_timestamp = await self.pg_manager.get_last_scrape_timestamp()
        
        now = datetime.now(timezone.utc)
        
        # ==================== INITIAL SCRAPE LOGIC ====================
        # Rule: Run only if database is empty
        if not has_content:
            since_time = now - timedelta(hours=INITIAL_SCRAPE_HOURS)
            logger.info(f"🆕 Database is empty - performing initial scrape ({INITIAL_SCRAPE_HOURS}h)")
            return "initial_scrape", since_time, INITIAL_SCRAPE_HOURS, True
        
        # ==================== DATABASE HAS DATA LOGIC ====================
        # Rule: Check time difference between last stored timestamp and current time
        
        if last_scrape_timestamp:
            time_diff = now - last_scrape_timestamp
            minutes_since = time_diff.total_seconds() / 60
            
            # Case 1: Time difference > scraping interval → SCRAPE (fetch missing news)
            if minutes_since >= REAL_TIME_INTERVAL_MINUTES:
                logger.info(f"⏰ Time since last scrape: {minutes_since:.1f}m > {REAL_TIME_INTERVAL_MINUTES}m interval - scraping to maintain data integrity")
                return "real_time_scrape", last_scrape_timestamp, minutes_since / 60, True
            
            # Case 2: Time difference < scraping interval → WAIT (until next scheduled time)
            else:
                next_scrape_time = last_scrape_timestamp + timedelta(minutes=REAL_TIME_INTERVAL_MINUTES)
                wait_minutes = (next_scrape_time - now).total_seconds() / 60
                logger.info(f"⏳ Time since last scrape: {minutes_since:.1f}m < {REAL_TIME_INTERVAL_MINUTES}m interval - waiting {wait_minutes:.1f}m until next scheduled scrape")
                return "within_interval", last_scrape_timestamp, wait_minutes, False
        
        # Edge case: No timestamp but has content - perform catchup scrape
        else:
            since_time = now - timedelta(hours=3)
            logger.info("⚠️ Has content but no timestamp - performing catchup scrape (3h)")
            return "catchup_scrape", since_time, 3, True
    
    async def should_scrape_now(self) -> Tuple[bool, str, Optional[datetime]]:
        """Intelligent scraping decision with caching"""
        
        # Check cache first
        if hasattr(self, 'decision_cache'):
            cached = self.decision_cache.get_cached_decision()
            if cached:
                return cached["should_scrape"], cached["reason"], cached.get("next_check_time")
        
        # Get strategy
        strategy, since_time, duration, should_scrape = await self.determine_scrape_strategy()
        
        # Calculate next check time with perfect logic explanations
        now = datetime.now(timezone.utc)
        if strategy == "within_interval":
            next_check = since_time + timedelta(minutes=REAL_TIME_INTERVAL_MINUTES)
            reason = f"⏳ WAITING: {duration:.1f}m until next scheduled scrape (interval: {REAL_TIME_INTERVAL_MINUTES}m)"
        elif strategy == "initial_scrape":
            next_check = now + timedelta(minutes=REAL_TIME_INTERVAL_MINUTES)
            reason = f"🆕 INITIAL SCRAPE: Database empty - scraping {duration:.0f}h of historical news"
        elif strategy == "real_time_scrape":
            next_check = now + timedelta(minutes=REAL_TIME_INTERVAL_MINUTES)
            reason = f"⏰ REAL-TIME SCRAPE: {duration:.1f}h gap > {REAL_TIME_INTERVAL_MINUTES/60:.1f}h interval - maintaining data integrity"
        elif strategy == "catchup_scrape":
            next_check = now + timedelta(minutes=REAL_TIME_INTERVAL_MINUTES)
            reason = f"⚠️ CATCHUP SCRAPE: Has data but no timestamp - filling {duration:.0f}h gap"
        else:
            next_check = now + timedelta(minutes=5)  # Check again in 5 minutes
            reason = f"❓ UNKNOWN STRATEGY: {strategy}"
        
        # Cache decision
        if hasattr(self, 'decision_cache'):
            self.decision_cache.cache_decision(should_scrape, next_check, reason)
        
        return should_scrape, reason, next_check
    
    async def scrape_messages_optimized(self):
        """Scrape messages from channels"""
        if not self.channels:
            return 0
        
        strategy, since_time, expected_hours, should_scrape = await self.determine_scrape_strategy()
        
        # Enhanced logging based on strategy
        if strategy == "initial_scrape":
            logger.info(f"🆕 INITIAL SCRAPE: Scanning {expected_hours:.0f}h of historical news (database empty)")
        elif strategy == "real_time_scrape":
            logger.info(f"⏰ REAL-TIME SCRAPE: Fetching missing news from {expected_hours:.1f}h gap")
        elif strategy == "catchup_scrape":
            logger.info(f"⚠️ CATCHUP SCRAPE: Filling {expected_hours:.0f}h gap (missing timestamp)")
        else:
            logger.info(f"🔍 Strategy: {strategy} (span: {expected_hours:.1f}h)")
        
        self.pending_messages = []
        
        # Ensure client is connected before scraping
        if not self.client.is_connected():
            logger.info("🔌 Reconnecting Telegram client for scraping")
            await self.client.connect()
        
        for channel_name, entity in self.channels.items():
            message_count = 0
            
            try:
                async for message in self.client.iter_messages(entity, limit=1000):
                    message_utc_time = message.date.astimezone(timezone.utc)
                    
                    if since_time and message_utc_time < since_time:
                        break
                    
                    # Message filtering
                    if message.text and len(message.text.strip()) > 10:
                        self.pending_messages.append((channel_name, message))
                        message_count += 1
            
            except Exception as e:
                logger.error(f"💥 Error scraping @{channel_name}: {e}")
            
            logger.info(f"✅ Found {message_count} messages from @{channel_name}")
        
        total_messages = len(self.pending_messages)
        logger.info(f"🔚 Total messages to process: {total_messages}")
        return total_messages
    
    async def process_all_messages_batch(self):
        """Process all messages using batch processing"""
        if not self.pending_messages:
            return 0
        
        # Get recent news for comparison
        recent_news = await self.pg_manager.get_recent_news(hours=24, limit=MAX_COMPARISON_NEWS)
        logger.info(f"🔄 Using {len(recent_news)} recent news")
        
        start_time = datetime.now()
        
        # Extract message texts
        message_texts = [(msg.text, msg) for channel_name, msg in self.pending_messages]
        
        # Process in batches
        logger.info(f"🚀 Processing {len(message_texts)} messages...")
        processed_results = await self.processor.process_batch_concurrent(message_texts, recent_news)
        
        # Prepare content for storage
        storage_items = []
        valid_results = 0
        
        for i, result in enumerate(processed_results):
            if result is None:
                continue
            
            # Skip duplicates for storage but keep for immediate sending
            if result["paraphrased_text"].startswith("[DUPLICATE]"):
                continue
            
            try:
                channel_name, message = self.pending_messages[i]
                message_utc_time = message.date.astimezone(timezone.utc)
                storage_items.append((channel_name, message.id, result, message_utc_time))
                valid_results += 1
            except:
                continue
        
        # Store results
        stored_count = await self.pg_manager.batch_store_processed_content(storage_items)
        
        # IMMEDIATE SENDING DISABLED: Now using optimized scheduled sending with rate limits
        # if processed_results:
        #     immediate_sent = await self.bot_integration.send_processed_messages_immediately(processed_results)
        #     logger.info(f"🚀 Immediately sent {immediate_sent} messages to Telegram!")
        logger.info(f"📦 Stored {stored_count} messages - will be sent by optimized scheduler with rate limiting")
        
        duration = (datetime.now() - start_time).total_seconds()
        rate = stored_count / duration if duration > 0 else 0
        
        logger.info(f"✅ Processing completed: {stored_count} messages in {duration:.1f}s ({rate:.2f} msg/s)")
        return stored_count
    
    async def scrape_and_process_optimized(self):
        """Main scraping and processing method"""
        start_time = datetime.now()
        
        # Scrape messages
        message_count = await self.scrape_messages_optimized()
        
        if message_count == 0:
            await self.pg_manager.set_last_scrape_timestamp(datetime.now(timezone.utc))
            return 0
        
        # Process messages
        processed_count = await self.process_all_messages_batch()
        
        # Update timestamp
        await self.pg_manager.set_last_scrape_timestamp(datetime.now(timezone.utc))
        
        total_duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"🔚 Scrape completed: {processed_count} messages in {total_duration:.1f}s")
        
        return processed_count
