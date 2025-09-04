"""
Scheduler for PostgreSQL Telegram News Scraper
Handles all scheduling operations with intelligent decision making
"""

import logging
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from tenacity import retry, stop_after_attempt, wait_exponential

from config import BOT_SEND_INTERVAL_MINUTES
from telegram_scraper import ScrapingDecisionCache

logger = logging.getLogger("news_scraper")

class OptimizedContentScheduler:
    """Scheduler with performance monitoring"""
    
    def __init__(self, scraper):
        self.scraper = scraper
        self.scheduler = AsyncIOScheduler()
        self.failure_count = 0
        self.performance_stats = {
            "total_runs": 0,
            "successful_runs": 0,
            "total_processing_time": 0,
            "messages_processed": 0
        }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=60))
    async def intelligent_scraping_task(self):
        """Intelligent scraping task that checks if scraping is needed"""
        try:
            # Check if scraping is needed
            should_scrape, reason, next_check_time = await self.scraper.should_scrape_now()
            
            if not should_scrape:
                logger.info(f"⏭️ Skipping scrape: {reason}")
                return
            
            # Proceed with scraping
            logger.info(f"🚀 Starting scrape: {reason}")
            
            task_start = datetime.now()
            
            if not await self.scraper.connect():
                raise Exception("Failed to connect to Telegram")
            
            processed_count = await self.scraper.scrape_and_process_optimized()
            
            # Update performance stats
            task_duration = (datetime.now() - task_start).total_seconds()
            self.performance_stats["total_runs"] += 1
            self.performance_stats["successful_runs"] += 1
            self.performance_stats["total_processing_time"] += task_duration
            self.performance_stats["messages_processed"] += processed_count
            
            self.failure_count = 0
            logger.info(f"✅ Intelligent scrape completed: {processed_count} messages in {task_duration:.1f}s")
            
            # Clear cache to force fresh decision on next run
            if hasattr(self.scraper, 'decision_cache'):
                self.scraper.decision_cache.cache = {}
            
        except Exception as e:
            self.failure_count += 1
            self.performance_stats["total_runs"] += 1
            logger.error(f"💥 Intelligent scraping failed: {e}")
            
            # Clear cache on failure to ensure retry logic works
            if hasattr(self.scraper, 'decision_cache'):
                self.scraper.decision_cache.cache = {}
            
            raise
    
    async def interval_telegram_send_task(self):
        """Send Telegram messages with 3-second intervals to achieve 20 messages per minute"""
        try:
            logger.info("🔔 Interval send task triggered")
            bot_integration = getattr(self.scraper, 'bot_integration', None)
            if not bot_integration:
                logger.warning("⚠️ No bot integration found")
                return
            
            # Send messages from same scrape cycle with 3-second intervals (20/min)
            sent_count = await bot_integration.send_batch_content_to_channel()
            
            if sent_count > 0:
                total_time = (sent_count - 1) * 3 if sent_count > 1 else 0
                logger.info(f"✅ Interval send completed: {sent_count} messages sent over {total_time}s (20/min)")
            else:
                logger.info("📭 No new messages to send from latest scrape cycle")
                
        except Exception as e:
            logger.error(f"💥 Interval send failed: {e}")
    

    
    async def start_optimized_scheduler(self):
        """Dynamic scheduler based on intelligent scraping decisions"""
        
        # Initialize decision cache in scraper
        self.scraper.decision_cache = ScrapingDecisionCache(ttl_seconds=300)
        
        # Get initial scraping decision
        should_scrape, reason, next_check_time = await self.scraper.should_scrape_now()
        
        logger.info(f"🧠 Intelligent Decision: {reason}")
        
        now = datetime.now(timezone.utc)
        
        if should_scrape:
            # Schedule immediate scrape
            initial_run_time = now + timedelta(seconds=5)
            logger.info("🚀 Scheduling immediate scrape")
        else:
            # Schedule next check based on intelligent decision
            wait_seconds = max(60, (next_check_time - now).total_seconds())
            initial_run_time = now + timedelta(seconds=wait_seconds)
            logger.info(f"⏳ Next check in {wait_seconds/60:.1f} minutes")
        
        # Schedule dynamic scraping job (checks if scraping is needed)
        self.scheduler.add_job(
            self.intelligent_scraping_task,
            "date",
            run_date=initial_run_time,
            id="intelligent_initial_run"
        )
        
        # Schedule periodic intelligent checks (every 5 minutes)
        self.scheduler.add_job(
            self.intelligent_scraping_task,
            "interval",
            minutes=5,  # Check every 5 minutes instead of 30
            id="intelligent_periodic_check"
        )
        
        # INTERVAL SENDING: Sends messages with 3-second intervals (20 messages/minute)
        # Sends stored messages from same scrape cycle to avoid repetition
        self.scheduler.add_job(
            self.interval_telegram_send_task,
            "interval",
            minutes=1,  # Check every 1 minute for new content to send (faster for testing)
            id="interval_telegram_send"
        )
        
        self.scheduler.start()
        logger.info("🧠 Intelligent scheduler started")

# Import the ScrapingDecisionCache class
from telegram_scraper import ScrapingDecisionCache
