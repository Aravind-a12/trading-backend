"""
Database models for PostgreSQL Telegram News Scraper
SQLAlchemy models for all database tables
"""

from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, Float, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ContentItem(Base):
    """Content items table - equivalent to MongoDB content_items collection"""
    __tablename__ = "content_items"
    __table_args__ = {'schema': 'blade_news'}
    
    id = Column(Integer, primary_key=True)
    content_id = Column(String(255), unique=True, nullable=False)  # MongoDB _id equivalent
    channel = Column(String(100), nullable=False)
    message_id = Column(String(50), nullable=False)
    original_text = Column(Text, nullable=False)
    paraphrased_text = Column(Text, nullable=False)
    processed_at = Column(String(50), nullable=False)  # ISO format
    message_date = Column(DateTime(timezone=True), nullable=False)
    processor = Column(String(50))
    model_used = Column(String(50))
    duplicate_decision = Column(String(20))
    duplicate_confidence = Column(Float)
    generation_time = Column(Float)
    model_priority = Column(Integer)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

class ScraperMetadata(Base):
    """Scraper metadata table - equivalent to MongoDB scraper_metadata collection"""
    __tablename__ = "scraper_metadata"
    __table_args__ = {'schema': 'blade_news'}
    
    id = Column(Integer, primary_key=True)
    key = Column(String(100), unique=True, nullable=False)
    timestamp = Column(Float)  # Unix timestamp
    updated_at = Column(DateTime(timezone=True), nullable=False)
    data = Column(JSON)  # For additional metadata

class DuplicateCache(Base):
    """Duplicate cache table - equivalent to MongoDB duplicate_cache collection"""
    __tablename__ = "duplicate_cache"
    __table_args__ = {'schema': 'blade_news'}
    
    id = Column(Integer, primary_key=True)
    content_hash = Column(String(64), unique=True, nullable=False)
    decision = Column(String(20), nullable=False)
    confidence = Column(Float, nullable=False)
    reference_id = Column(String(64))
    created_at = Column(DateTime(timezone=True), nullable=False)
