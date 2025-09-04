#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
News API Router for Trading Backend
Provides endpoints for managing and monitoring the NEWS service
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Optional
import os
import asyncio
from datetime import datetime, timezone, timedelta
import logging

router = APIRouter()
logger = logging.getLogger("news_router")

# Service status tracking
_news_service_status = {
    "enabled": False,
    "running": False,
    "last_startup": None,
    "last_error": None,
    "stats": {
        "total_messages_processed": 0,
        "total_messages_sent": 0,
        "last_scrape_time": None,
        "last_send_time": None
    }
}

@router.get("/status")
async def get_news_status():
    """Get comprehensive news service status"""
    try:
        # Import here to avoid circular imports
        from app.NEWS.simple_news import get_service_status
        
        # Get current service status
        service_status = await get_service_status()
        
        # Check environment variable
        news_enabled = os.getenv("ENABLE_NEWS", "true").lower() in {"1", "true", "yes"}
        
        return {
            "service_enabled": news_enabled,
            "service_running": service_status.get("running", False),
            "last_startup": service_status.get("last_startup"),
            "last_error": service_status.get("last_error"),
            "configuration": {
                "scrape_interval_minutes": service_status.get("scrape_interval_minutes", 30),
                "send_interval_minutes": service_status.get("send_interval_minutes", 5),
                "channels": service_status.get("channels", []),
                "database_connected": service_status.get("database_connected", False)
            },
            "statistics": service_status.get("stats", {}),
            "version": "v7.0.0"
        }
    except Exception as e:
        logger.error(f"Error getting news status: {e}")
        return {
            "service_enabled": False,
            "service_running": False,
            "error": str(e),
            "version": "v7.0.0"
        }

@router.get("/health")
async def health_check():
    """Health check endpoint for the news service"""
    try:
        from app.NEWS.simple_news import health_check as news_health_check
        
        health_status = await news_health_check()
        
        if health_status.get("healthy", False):
            return {
                "status": "healthy",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "details": health_status
            }
        else:
            raise HTTPException(status_code=503, detail={
                "status": "unhealthy",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "details": health_status
            })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail={
            "status": "unhealthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e)
        })

@router.get("/recent")
async def get_recent_news(limit: int = 20):
    """Get recent news items"""
    try:
        from app.NEWS.simple_news import get_recent_news_items
        
        if limit < 1 or limit > 100:
            raise HTTPException(status_code=400, detail="Limit must be between 1 and 100")
        
        news_items = await get_recent_news_items(limit)
        
        return {
            "count": len(news_items),
            "items": news_items,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting recent news: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/statistics")
async def get_statistics():
    """Get detailed statistics about the news service"""
    try:
        from app.NEWS.simple_news import get_detailed_statistics
        
        stats = await get_detailed_statistics()
        
        return {
            "statistics": stats,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/control/start")
async def start_news_service():
    """Start the news service manually"""
    try:
        from app.NEWS.simple_news import start_news_service as start_service
        
        result = await start_service()
        
        if result:
            return {
                "status": "success",
                "message": "News service started successfully",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to start news service")
    except Exception as e:
        logger.error(f"Error starting news service: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/control/stop")
async def stop_news_service():
    """Stop the news service manually"""
    try:
        from app.NEWS.simple_news import stop_news_service as stop_service
        
        result = await stop_service()
        
        if result:
            return {
                "status": "success",
                "message": "News service stopped successfully",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to stop news service")
    except Exception as e:
        logger.error(f"Error stopping news service: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/control/restart")
async def restart_news_service():
    """Restart the news service"""
    try:
        from app.NEWS.simple_news import restart_news_service as restart_service
        
        result = await restart_service()
        
        if result:
            return {
                "status": "success",
                "message": "News service restarted successfully",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to restart news service")
    except Exception as e:
        logger.error(f"Error restarting news service: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/channels")
async def get_channel_info():
    """Get information about configured channels"""
    try:
        from app.NEWS.simple_news import get_channel_information
        
        channel_info = await get_channel_information()
        
        return {
            "channels": channel_info,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting channel info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/database/status")
async def get_database_status():
    """Get database connection status and statistics"""
    try:
        from app.NEWS.simple_news import get_database_status
        
        db_status = await get_database_status()
        
        return {
            "database": db_status,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting database status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/database/test")
async def test_database_connection():
    """Test database connection"""
    try:
        from app.NEWS.simple_news import test_database_connection
        
        test_result = await test_database_connection()
        
        if test_result.get("success", False):
            return {
                "status": "success",
                "result": test_result,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail=test_result)
    except Exception as e:
        logger.error(f"Error testing database: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/logs")
async def get_recent_logs(lines: int = 50):
    """Get recent log entries"""
    try:
        if lines < 1 or lines > 1000:
            raise HTTPException(status_code=400, detail="Lines must be between 1 and 1000")
        
        from app.NEWS.simple_news import get_recent_logs
        
        logs = await get_recent_logs(lines)
        
        return {
            "logs": logs,
            "line_count": len(logs),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))
