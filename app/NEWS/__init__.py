#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NEWS System Package
Provides automated Telegram news scraping and processing functionality
"""

from .simple_news import (
    start_news_service,
    stop_news_service,
    restart_news_service,
    get_service_status,
    health_check,
    get_recent_news_items,
    get_detailed_statistics,
    get_channel_information,
    get_database_status,
    test_database_connection,
    get_recent_logs
)

__version__ = "7.0.0"
__all__ = [
    "start_news_service",
    "stop_news_service", 
    "restart_news_service",
    "get_service_status",
    "health_check",
    "get_recent_news_items",
    "get_detailed_statistics",
    "get_channel_information",
    "get_database_status",
    "test_database_connection",
    "get_recent_logs"
]
