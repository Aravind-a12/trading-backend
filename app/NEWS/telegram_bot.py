"""
Telegram bot integration for PostgreSQL Telegram News Scraper
Handles all Telegram bot operations with immediate sending capabilities
"""

import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError, TimedOut, NetworkError

from config import BOT_TOKEN, CHANNEL_USERNAME, BOT_MAX_MESSAGES_PER_MINUTE, INITIAL_SCRAPE_HOURS
from text_utils import optimized_safe_message_for_telegram, PATTERNS
from models import ScraperMetadata

logger = logging.getLogger("news_scraper")

class OptimizedTelegramBot:
    """Telegram bot with PostgreSQL integration and 30-second interval sending"""
    
    def __init__(self, bot_token: str, channel_username: str, pg_manager):
        self.bot = Bot(token=bot_token)
        self.channel_username = channel_username
        self.pg_manager = pg_manager
        
        # Pre-compile markdown escape patterns for performance
        self.markdown_escape_table = str.maketrans({
            char: f'\\{char}' for char in PATTERNS.markdown_chars
        })
        
        logger.info(f"✅ Optimized Telegram Bot initialized for {channel_username} - 20 MESSAGES/MINUTE")
    
    async def verify_bot_permissions(self) -> bool:
        """Verify bot can send messages to channel"""
        try:
            bot_info = await self.bot.get_me()
            await self.bot.get_chat(self.channel_username)
            logger.info(f"✅ Bot verified: @{bot_info.username}")
            return True
        except Exception as e:
            logger.error(f"💥 Bot verification failed: {e}")
            return False
    

    
    async def send_batch_content_to_channel(self) -> int:
        """Send batch content to Telegram channel with 3-second intervals (20 messages/minute)"""
        try:
            unsent_content = await self._get_unsent_content_same_cycle()
            
            if not unsent_content:
                return 0
            
            logger.info(f"📤 Starting to send {len(unsent_content)} messages with 3-second intervals (20/min)")
            
            sent_count = 0
            latest_sent_timestamp = None
            
            for i, content in enumerate(unsent_content):
                # Add 3-second delay between messages (except for the first message)
                # This allows 20 messages per minute (60 seconds / 3 seconds = 20)
                if i > 0:
                    logger.info(f"⏳ Waiting 3 seconds before sending next message ({i+1}/{len(unsent_content)})")
                    await asyncio.sleep(3)  # 3-second interval for 20 messages/minute
                
                # Format and send message
                message = self._format_message_fast(content)
                
                if not message.startswith("💥 Error:"):
                    if await self._send_message_direct(message):
                        sent_count += 1
                        logger.info(f"📤 Sent message {sent_count}/{len(unsent_content)}: {message[:50]}...")
                        
                        try:
                            msg_date = content.get("message_date")
                            if isinstance(msg_date, str):
                                msg_date = datetime.fromisoformat(msg_date)
                            elif isinstance(msg_date, datetime):
                                pass  # Already datetime
                            else:
                                msg_date = datetime.now(timezone.utc)
                                
                            if latest_sent_timestamp is None or msg_date > latest_sent_timestamp:
                                latest_sent_timestamp = msg_date
                        except:
                            pass
                    else:
                        logger.error(f"❌ Failed to send message {i+1}")
                else:
                    logger.warning(f"⚠️ Skipping error message: {message[:50]}...")
            
            # Update timestamp after all messages are sent
            if latest_sent_timestamp:
                await self._update_last_sent_timestamp(latest_sent_timestamp)
            
            if sent_count > 0:
                logger.info(f"✅ Successfully sent {sent_count} messages with 3-second intervals (20/min)")
            
            return sent_count
            
        except Exception as e:
            logger.error(f"💥 Batch send error: {e}")
            return 0
    
    async def _get_unsent_content_same_cycle(self) -> List[Dict]:
        """Get unsent content from the same scrape cycle to avoid repetition"""
        try:
            session = self.pg_manager.get_session()
            last_sent_doc = session.query(ScraperMetadata).filter(ScraperMetadata.key == "last_sent_timestamp").first()
            
            if last_sent_doc and last_sent_doc.timestamp:
                last_sent_time = datetime.fromtimestamp(last_sent_doc.timestamp, timezone.utc)
                content = await self.pg_manager.get_content_after_timestamp(last_sent_time, limit=50)
                logger.info(f"📊 Looking for content after last sent time: {last_sent_time}")
            else:
                # No last sent timestamp - get all recent content
                since_time = datetime.now(timezone.utc) - timedelta(hours=INITIAL_SCRAPE_HOURS)
                content = await self.pg_manager.get_content_since_timestamp(since_time, limit=100)
                logger.info(f"📊 No last sent timestamp found, getting all content since: {since_time}")
            
            session.close()
            
            # Group content by processing timestamp to ensure same scrape cycle
            cycle_groups = {}
            for c in content:
                processed_at = c.get("processed_at", "")
                if processed_at:
                    # Group by hour of processing to ensure same scrape cycle
                    try:
                        proc_time = datetime.fromisoformat(processed_at.replace('Z', '+00:00'))
                        cycle_key = proc_time.strftime("%Y-%m-%d-%H")  # Group by date and hour
                    except:
                        cycle_key = "unknown"
                else:
                    cycle_key = "unknown"
                
                if cycle_key not in cycle_groups:
                    cycle_groups[cycle_key] = []
                cycle_groups[cycle_key].append(c)
            
            # Get the most recent cycle with content
            if cycle_groups:
                # Sort by cycle key (most recent first)
                sorted_cycles = sorted(cycle_groups.keys(), reverse=True)
                latest_cycle = sorted_cycles[0]
                latest_cycle_content = cycle_groups[latest_cycle]
                
                logger.info(f"📊 Found {len(cycle_groups)} scrape cycles, using latest: {latest_cycle}")
                logger.info(f"📦 Latest cycle has {len(latest_cycle_content)} messages")
            else:
                latest_cycle_content = content
            
            # Filter content from the same cycle
            filtered_content = []
            for c in latest_cycle_content:
                paraphrased_text = c.get("paraphrased_text", "")
                if (paraphrased_text 
                    and not paraphrased_text.startswith("💥 Error:")
                    and not paraphrased_text.startswith("[DUPLICATE]")
                    and len(paraphrased_text.strip()) > 10):
                    filtered_content.append(c)
            
            # Sort by message date to maintain chronological order
            filtered_content.sort(key=lambda x: x.get("message_date", datetime.now(timezone.utc)))
            
            if filtered_content:
                logger.info(f"✅ Prepared {len(filtered_content)} messages from same scrape cycle")
            else:
                logger.info("📭 No valid messages found from latest scrape cycle")
                
            return filtered_content
            
        except Exception as e:
            logger.error(f"💥 Error getting same-cycle content: {e}")
            if 'session' in locals():
                session.close()
            return []
    
    def _format_message_fast(self, content_data: Dict) -> str:
        """Format message for Telegram"""
        try:
            text = content_data.get("paraphrased_text", "")
            safe_text = optimized_safe_message_for_telegram(text)
            
            if safe_text.startswith("💥 Error:"):
                return safe_text
            
            # Fast markdown escaping using translation table
            escaped_text = safe_text.translate(self.markdown_escape_table)
            
            return escaped_text if len(escaped_text.strip()) >= 10 else "💥 Error: Message too short"
            
        except Exception as e:
            return f"💥 Error: Message formatting failed - {str(e)}"
    
    async def _send_message_direct(self, message: str) -> bool:
        """Send message to Telegram channel"""
        try:
            await self.bot.send_message(
                chat_id=self.channel_username,
                text=message,
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return True
        except TelegramError as e:
            if "flood control" in str(e).lower():
                logger.warning(f"⚠️ Telegram flood control hit: {e}")
                # Return False so the calling function can handle it
                return False
            else:
                logger.error(f"💥 Telegram API error: {e}")
                return False
        except Exception as e:
            logger.error(f"💥 Send failed: {e}")
            return False
    
    async def _update_last_sent_timestamp(self, latest_timestamp: datetime):
        """Update last sent timestamp in PostgreSQL"""
        try:
            session = self.pg_manager.get_session()
            
            # Remove old entry if exists
            session.query(ScraperMetadata).filter(ScraperMetadata.key == "last_sent_timestamp").delete()
            
            # Add new entry
            metadata = ScraperMetadata(
                key="last_sent_timestamp",
                timestamp=latest_timestamp.timestamp(),
                updated_at=datetime.now(timezone.utc)
            )
            
            session.add(metadata)
            session.commit()
            session.close()
            
        except Exception:
            if 'session' in locals():
                session.rollback()
                session.close()
