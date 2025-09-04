"""
Patch for telegram library in the News System
Fixes issues with AsyncClient and proxies parameter
"""
import logging
from typing import Optional, Dict, Any
from pathlib import Path
import sys

logger = logging.getLogger("telegram_patch")

def apply_patches():
    """Apply patches to telegram libraries"""
    try:
        # Patch python-telegram-bot
        from telegram import Bot
        from telegram.request import HTTPXRequest
        
        # Store the original Bot.__init__
        original_bot_init = Bot.__init__
        
        # Define a patched Bot.__init__
        def patched_bot_init(self, token: str, **kwargs):
            # Remove 'proxies' if it exists
            if 'proxies' in kwargs:
                logger.info("Removing 'proxies' parameter from Bot initialization")
                del kwargs['proxies']
            
            # Remove 'request' if it exists
            if 'request' in kwargs:
                logger.info("Removing 'request' parameter from Bot initialization")
                del kwargs['request']
            
            # Create a custom request object without proxies
            request = HTTPXRequest(
                connection_pool_size=kwargs.pop('connection_pool_size', 1),
                read_timeout=kwargs.pop('read_timeout', 5.0),
                write_timeout=kwargs.pop('write_timeout', 5.0),
                connect_timeout=kwargs.pop('connect_timeout', 5.0),
            )
            
            # Call the original __init__ with our modified arguments
            kwargs['request'] = request
            original_bot_init(self, token=token, **kwargs)
        
        # Apply the patch
        Bot.__init__ = patched_bot_init
        logger.info("✅ Successfully patched python-telegram-bot.Bot")
        
        # Patch telethon if needed
        import telethon
        from telethon import TelegramClient
        
        # Store the original TelegramClient.__init__
        original_client_init = TelegramClient.__init__
        
        # Define a patched TelegramClient.__init__
        def patched_client_init(self, session, api_id, api_hash, **kwargs):
            # Remove 'proxies' if it exists
            if 'proxy' in kwargs:
                logger.info("Removing 'proxy' parameter from TelegramClient initialization")
                del kwargs['proxy']
            
            # Call the original __init__ with our modified arguments
            original_client_init(self, session, api_id, api_hash, **kwargs)
        
        # Apply the patch
        TelegramClient.__init__ = patched_client_init
        logger.info("✅ Successfully patched telethon.TelegramClient")
        
        # Patch aiohttp if needed
        import aiohttp
        
        # Store the original ClientSession.__init__
        original_session_init = aiohttp.ClientSession.__init__
        
        # Define a patched ClientSession.__init__
        def patched_session_init(self, *args, **kwargs):
            # Remove 'proxies' if it exists
            if 'proxies' in kwargs:
                logger.info("Removing 'proxies' parameter from ClientSession initialization")
                del kwargs['proxies']
            
            # Call the original __init__ with our modified arguments
            original_session_init(self, *args, **kwargs)
        
        # Apply the patch
        aiohttp.ClientSession.__init__ = patched_session_init
        logger.info("✅ Successfully patched aiohttp.ClientSession")
        
        # Patch httpx.AsyncClient if needed
        import httpx
        
        # Store the original AsyncClient.__init__
        original_async_client_init = httpx.AsyncClient.__init__
        
        # Define a patched AsyncClient.__init__
        def patched_async_client_init(self, *args, **kwargs):
            # Remove 'proxies' if it exists
            if 'proxies' in kwargs:
                logger.info("Removing 'proxies' parameter from httpx.AsyncClient initialization")
                del kwargs['proxies']
            
            # Call the original __init__ with our modified arguments
            original_async_client_init(self, *args, **kwargs)
        
        # Apply the patch
        httpx.AsyncClient.__init__ = patched_async_client_init
        logger.info("✅ Successfully patched httpx.AsyncClient")
        
        # Patch HTTPXRequest if it exists
        try:
            from telegram.request import HTTPXRequest
            
            # Store the original HTTPXRequest.__init__
            original_httpx_request_init = HTTPXRequest.__init__
            
            # Define a patched HTTPXRequest.__init__
            def patched_httpx_request_init(self, *args, **kwargs):
                # Remove proxy-related parameters
                proxy_params = ['proxies', 'proxy', 'trust_env']
                for param in proxy_params:
                    if param in kwargs:
                        logger.info(f"Removing '{param}' parameter from HTTPXRequest initialization")
                        del kwargs[param]
                
                # Call the original __init__ with our modified arguments
                original_httpx_request_init(self, *args, **kwargs)
            
            # Apply the patch
            HTTPXRequest.__init__ = patched_httpx_request_init
            logger.info("✅ Successfully patched telegram.request.HTTPXRequest")
            
        except Exception as e:
            logger.warning(f"⚠️ Could not patch HTTPXRequest: {e}")
        
        return True
    except Exception as e:
        logger.error(f"❌ Failed to apply telegram patches: {str(e)}")
        return False
