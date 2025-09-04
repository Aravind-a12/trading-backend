"""
Database manager for PostgreSQL Telegram News Scraper
Handles all database operations with optimized performance
"""

import logging
import time
import hashlib
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from psycopg2.extras import RealDictCursor
import uuid

from models import Base, ContentItem, ScraperMetadata, DuplicateCache
from config import PG_CONNECTION_STRING, DUPLICATE_CACHE_TTL, INITIAL_SCRAPE_HOURS, MAX_COMPARISON_NEWS
from text_utils import optimized_safe_message_for_telegram

logger = logging.getLogger("news_scraper")

class OptimizedPostgreSQLManager:
    """PostgreSQL manager with enhanced async operations and improved error handling"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.engine = None
        self.SessionLocal = None
        self._connection_pool = None
        
    async def connect(self):
        """Initialize PostgreSQL connection with enhanced error handling"""
        try:
            # Create engine with connection pooling
            self.engine = create_engine(
                self.connection_string,
                pool_size=20,
                max_overflow=30,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            
            # Test connection with better error handling
            with self.engine.connect() as connection:
                result = connection.execute(text("SELECT version();"))
                version = result.fetchone()[0]
                logger.info(f"✅ PostgreSQL connected: {version}")
            
            # Create schema if it doesn't exist
            with self.engine.connect() as connection:
                connection.execute(text("CREATE SCHEMA IF NOT EXISTS blade_news"))
                connection.commit()
                logger.info("✅ Schema 'blade_news' ensured")
            
            # Create all tables
            Base.metadata.create_all(self.engine)
            logger.info("✅ Database tables ensured")
            
            # Create session factory with optimized settings
            self.SessionLocal = sessionmaker(
                autocommit=False, 
                autoflush=False, 
                bind=self.engine,
                expire_on_commit=False
            )
            
            # Create indexes for better performance
            await self._create_indexes()
            
            logger.info("✅ PostgreSQL manager initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"💥 PostgreSQL connection failed: {e}")
            if self.engine:
                self.engine.dispose()
            return False
    
    async def _create_indexes(self):
        """Create database indexes"""
        try:
            with self.engine.connect() as connection:
                # Timeline indexes
                connection.execute(text("CREATE INDEX IF NOT EXISTS idx_content_message_date ON blade_news.content_items(message_date DESC)"))
                connection.execute(text("CREATE INDEX IF NOT EXISTS idx_content_channel_date ON blade_news.content_items(channel, message_date DESC)"))
                connection.execute(text("CREATE INDEX IF NOT EXISTS idx_content_processed_at ON blade_news.content_items(processed_at DESC)"))
                connection.execute(text("CREATE INDEX IF NOT EXISTS idx_content_id ON blade_news.content_items(content_id)"))
                
                # Text search index
                connection.execute(text("CREATE INDEX IF NOT EXISTS idx_content_text_search ON blade_news.content_items USING gin(to_tsvector('english', paraphrased_text))"))
                
                # Metadata indexes
                connection.execute(text("CREATE INDEX IF NOT EXISTS idx_metadata_key ON blade_news.scraper_metadata(key)"))
                
                # Cache indexes  
                connection.execute(text("CREATE INDEX IF NOT EXISTS idx_cache_hash ON blade_news.duplicate_cache(content_hash)"))
                connection.execute(text("CREATE INDEX IF NOT EXISTS idx_cache_created ON blade_news.duplicate_cache(created_at)"))
                
                connection.commit()
                logger.info("✅ Database indexes created")
        except Exception as e:
            logger.error(f"Index creation error: {e}")
    
    def get_session(self) -> Session:
        """Get a database session."""
        if self.SessionLocal is None:
            raise Exception("Database not initialized")
        return self.SessionLocal()
    
    async def get_last_scrape_timestamp(self) -> Optional[datetime]:
        """Get last scrape timestamp from PostgreSQL"""
        try:
            session = self.get_session()
            result = session.query(ScraperMetadata).filter(ScraperMetadata.key == "last_scrape_timestamp").first()
            
            if result and result.timestamp:
                session.close()
                return datetime.fromtimestamp(result.timestamp, timezone.utc)
            
            session.close()
        except Exception:
            if 'session' in locals():
                session.close()
        return None
    
    async def set_last_scrape_timestamp(self, timestamp: datetime):
        """Set last scrape timestamp in PostgreSQL"""
        try:
            session = self.get_session()
            
            # Remove old entry if exists
            session.query(ScraperMetadata).filter(ScraperMetadata.key == "last_scrape_timestamp").delete()
            
            # Add new entry
            metadata = ScraperMetadata(
                key="last_scrape_timestamp",
                timestamp=timestamp.timestamp(),
                updated_at=datetime.now(timezone.utc)
            )
            
            session.add(metadata)
            session.commit()
            session.close()
            
        except Exception:
            if 'session' in locals():
                session.rollback()
                session.close()
    
    async def has_content(self) -> bool:
        """Check if database has any content"""
        try:
            session = self.get_session()
            count = session.query(ContentItem).count()
            session.close()
            return count > 0
        except Exception:
            if 'session' in locals():
                session.close()
            return False
    
    async def get_content_count(self) -> int:
        """Get total content count"""
        try:
            session = self.get_session()
            count = session.query(ContentItem).count()
            session.close()
            return count
        except Exception:
            if 'session' in locals():
                session.close()
            return 0
    
    async def batch_store_processed_content(self, content_items: List[Tuple[str, int, Dict, datetime]]) -> int:
        """Optimized batch store with better error handling"""
        if not content_items:
            return 0
        
        session = None
        try:
            session = self.get_session()
            stored_count = 0
            batch_items = []
            
            # Pre-validate and prepare items
            for channel, message_id, content_data, message_date in content_items:
                if not self._fast_validate_content(content_data):
                    continue
                
                # Create unique content ID
                content_id = f"{int(message_date.timestamp())}_{channel}_{message_id}"
                
                content_item = ContentItem(
                    content_id=content_id,
                    channel=channel,
                    message_id=str(message_id),
                    original_text=content_data.get("original_text", ""),
                    paraphrased_text=content_data.get("paraphrased_text", ""),
                    processed_at=content_data.get("processed_at", ""),
                    message_date=message_date,
                    processor=content_data.get("processor", "unknown"),
                    model_used=content_data.get("model_used", ""),
                    duplicate_decision=content_data.get("duplicate_decision", ""),
                    duplicate_confidence=content_data.get("duplicate_confidence", 0.0),
                    generation_time=content_data.get("generation_time", 0.0),
                    model_priority=content_data.get("model_priority", 999)
                )
                batch_items.append((content_id, content_item))
            
            # Batch check for existing items
            if batch_items:
                existing_ids = set()
                content_ids = [item[0] for item in batch_items]
                
                # Efficient batch query for existing items
                existing_items = session.query(ContentItem.content_id).filter(
                    ContentItem.content_id.in_(content_ids)
                ).all()
                existing_ids = {item.content_id for item in existing_items}
                
                # Add only new items
                for content_id, content_item in batch_items:
                    if content_id not in existing_ids:
                        session.add(content_item)
                        stored_count += 1
                
                # Batch commit
                if stored_count > 0:
                    session.commit()
                    logger.info(f"✅ Batch stored {stored_count} items")
            
            return stored_count
            
        except Exception as e:
            logger.error(f"💥 PostgreSQL batch store error: {e}")
            if session:
                try:
                    session.rollback()
                except:
                    pass
            return 0
        finally:
            if session:
                try:
                    session.close()
                except:
                    pass
    
    async def get_content_since_timestamp(self, since_timestamp: datetime, limit: int = 1000) -> List[Dict]:
        """Get content items since timestamp with enhanced error handling"""
        session = None
        try:
            session = self.get_session()
            items = session.query(ContentItem).filter(
                ContentItem.message_date >= since_timestamp
            ).order_by(ContentItem.message_date.desc()).limit(limit).all()
            
            result = []
            for item in items:
                result.append({
                    "_id": item.content_id,  # MongoDB compatibility
                    "content_id": item.content_id,
                    "channel": item.channel,
                    "message_id": item.message_id,
                    "original_text": item.original_text,
                    "paraphrased_text": item.paraphrased_text,
                    "processed_at": item.processed_at,
                    "message_date": item.message_date,
                    "processor": item.processor,
                    "model_used": item.model_used,
                    "duplicate_decision": item.duplicate_decision,
                    "created_at": item.created_at
                })
            
            return result
            
        except Exception as e:
            logger.error(f"💥 Error getting content since timestamp: {e}")
            return []
        finally:
            if session:
                try:
                    session.close()
                except:
                    pass
    
    async def get_content_after_timestamp(self, after_timestamp: datetime, limit: int = 1000) -> List[Dict]:
        """Get content items after timestamp with enhanced error handling"""
        session = None
        try:
            session = self.get_session()
            items = session.query(ContentItem).filter(
                ContentItem.message_date > after_timestamp
            ).order_by(ContentItem.message_date.desc()).limit(limit).all()
            
            result = []
            for item in items:
                result.append({
                    "_id": item.content_id,  # MongoDB compatibility
                    "content_id": item.content_id,
                    "channel": item.channel,
                    "message_id": item.message_id,
                    "original_text": item.original_text,
                    "paraphrased_text": item.paraphrased_text,
                    "processed_at": item.processed_at,
                    "message_date": item.message_date,
                    "processor": item.processor,
                    "model_used": item.model_used,
                    "duplicate_decision": item.duplicate_decision,
                    "created_at": item.created_at
                })
            
            return result
            
        except Exception as e:
            logger.error(f"💥 Error getting content after timestamp: {e}")
            return []
        finally:
            if session:
                try:
                    session.close()
                except:
                    pass
    
    async def get_recent_content(self, limit: int = 50) -> List[Dict]:
        """Get recent content items with enhanced error handling"""
        session = None
        try:
            session = self.get_session()
            items = session.query(ContentItem).order_by(ContentItem.message_date.desc()).limit(limit).all()
            
            result = []
            for item in items:
                result.append({
                    "_id": item.content_id,  # MongoDB compatibility
                    "content_id": item.content_id,
                    "channel": item.channel,
                    "message_id": item.message_id,
                    "original_text": item.original_text,
                    "paraphrased_text": item.paraphrased_text,
                    "processed_at": item.processed_at,
                    "message_date": item.message_date,
                    "processor": item.processor,
                    "model_used": item.model_used,
                    "duplicate_decision": item.duplicate_decision,
                    "created_at": item.created_at
                })
            
            return result
            
        except Exception as e:
            logger.error(f"💥 Error getting recent content: {e}")
            return []
        finally:
            if session:
                try:
                    session.close()
                except:
                    pass
    
    async def get_recent_news(self, hours: int = 24, limit: int = 50) -> List[Dict]:
        """Get recent news from last specified hours"""
        since_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        return await self.get_content_since_timestamp(since_time, limit)
    
    def _fast_validate_content(self, content_data: Dict[str, Any]) -> bool:
        """Validate content data"""
        paraphrased_text = content_data.get("paraphrased_text")
        if not paraphrased_text or not isinstance(paraphrased_text, str):
            return False
        
        safe_text = optimized_safe_message_for_telegram(paraphrased_text)
        if safe_text.startswith("💥 Error:"):
            return False
        
        original_text = content_data.get("original_text")
        if not original_text or not isinstance(original_text, str):
            return False
        
        return True
    
    async def close(self):
        """Close PostgreSQL connection with enhanced cleanup"""
        try:
            if self.engine:
                # Close all sessions first
                if self.SessionLocal:
                    self.SessionLocal.close_all()
                
                # Dispose of engine
                self.engine.dispose()
                logger.info("✅ PostgreSQL connections closed gracefully")
        except Exception as e:
            logger.error(f"💥 Error closing PostgreSQL connections: {e}")
        finally:
            self.engine = None
            self.SessionLocal = None

class SmartPostgreSQLCache:
    """PostgreSQL-based caching for duplicate detection"""
    
    def __init__(self, pg_manager, ttl: int = DUPLICATE_CACHE_TTL):
        self.pg_manager = pg_manager
        self.ttl = ttl
        self.local_cache = {}
        self.cache_timestamps = {}
        # Import patterns here to avoid circular import
        from text_utils import PATTERNS
        self.patterns = PATTERNS
    
    def _generate_content_hash(self, text: str) -> str:
        """Generate hash for content"""
        normalized = self.patterns.whitespace_pattern.sub(' ', text.lower().strip())
        return hashlib.md5(normalized.encode()).hexdigest()
    
    async def get_cached_duplicate_result(self, text: str) -> Optional[Tuple[str, float]]:
        """Get cached duplicate result with enhanced error handling"""
        content_hash = self._generate_content_hash(text)
        
        # Check local cache first
        if content_hash in self.local_cache:
            timestamp = self.cache_timestamps.get(content_hash, 0)
            if time.time() - timestamp < self.ttl:
                return self.local_cache[content_hash]
            else:
                del self.local_cache[content_hash]
                del self.cache_timestamps[content_hash]
        
        # Check PostgreSQL cache with enhanced error handling
        session = None
        try:
            session = self.pg_manager.get_session()
            cached_item = session.query(DuplicateCache).filter(
                DuplicateCache.content_hash == content_hash,
                DuplicateCache.created_at > (datetime.now(timezone.utc) - timedelta(seconds=self.ttl))
            ).first()
            
            if cached_item:
                decision = cached_item.decision
                confidence = cached_item.confidence
                
                # Update local cache
                self.local_cache[content_hash] = (decision, confidence)
                self.cache_timestamps[content_hash] = time.time()
                
                return (decision, confidence)
            
        except Exception as e:
            logger.error(f"💥 Error getting cached duplicate result: {e}")
        finally:
            if session:
                try:
                    session.close()
                except:
                    pass
        
        return None
    
    async def cache_duplicate_result(self, text: str, decision: str, confidence: float):
        """Cache duplicate result in PostgreSQL with enhanced error handling"""
        content_hash = self._generate_content_hash(text)
        
        session = None
        try:
            session = self.pg_manager.get_session()
            
            # Remove old entry if exists
            session.query(DuplicateCache).filter(DuplicateCache.content_hash == content_hash).delete()
            
            # Add new entry
            cache_item = DuplicateCache(
                content_hash=content_hash,
                decision=decision,
                confidence=confidence,
                created_at=datetime.now(timezone.utc)
            )
            
            session.add(cache_item)
            session.commit()
            
            # Store in local cache
            self.local_cache[content_hash] = (decision, confidence)
            self.cache_timestamps[content_hash] = time.time()
            
        except Exception as e:
            logger.error(f"💥 Error caching duplicate result: {e}")
            if session:
                try:
                    session.rollback()
                except:
                    pass
        finally:
            if session:
                try:
                    session.close()
                except:
                    pass
