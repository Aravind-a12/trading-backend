"""
Gemini API client for PostgreSQL Telegram News Scraper
Handles all Gemini API interactions with rate limiting and caching
"""

import time
import logging
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any, Tuple
from collections import defaultdict
import aiohttp
import google.generativeai as genai

from config import GEMINI_API_KEY, GEMINI_MODELS, MODEL_CONFIGS, CONCURRENT_API_CALLS, TIMEOUT_SECONDS, DUPLICATE_THRESHOLD, MAX_COMPARISON_NEWS, RATE_LIMIT_CACHE_TTL
from text_utils import optimized_extract_text_from_response, fast_text_cleanup, fast_clean_response, fast_validate_paraphrase, fast_fallback_paraphrase

logger = logging.getLogger("news_scraper")

# ==================== IN-MEMORY RATE LIMIT CACHE ====================
class RateLimitCache:
    """In-memory cache for rate limit status"""
    
    def __init__(self, ttl: int = RATE_LIMIT_CACHE_TTL):
        self.ttl = ttl
        self.cache = {}
        self.timestamps = {}
    
    def get_cached_rate_status(self, model_name: str) -> Optional[Tuple[bool, Dict]]:
        """Get cached rate limit status"""
        if model_name in self.cache:
            timestamp = self.timestamps.get(model_name, 0)
            if time.time() - timestamp < self.ttl:
                return self.cache[model_name]
            else:
                # Expired
                del self.cache[model_name]
                del self.timestamps[model_name]
        return None
    
    def cache_rate_status(self, model_name: str, can_use: bool, status: Dict):
        """Cache rate limit status"""
        self.cache[model_name] = (can_use, status)
        self.timestamps[model_name] = time.time()
    
    def clear_expired(self):
        """Clear expired cache entries"""
        current_time = time.time()
        expired_keys = [
            key for key, timestamp in self.timestamps.items() 
            if current_time - timestamp >= self.ttl
        ]
        for key in expired_keys:
            self.cache.pop(key, None)
            self.timestamps.pop(key, None)

# HTTP connection pooling
class OptimizedHTTPSession:
    """HTTP connection pooling for faster API calls"""
    
    def __init__(self):
        self.connector = None
        self.session = None
    
    async def __aenter__(self):
        self.connector = aiohttp.TCPConnector(
            limit=50,  # Total connection limit
            limit_per_host=10,  # Per-host connection limit
            ttl_dns_cache=300,  # DNS cache TTL
            use_dns_cache=True,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
        self.session = aiohttp.ClientSession(
            connector=self.connector,
            timeout=timeout,
            headers={'User-Agent': 'OptimizedTelegramScraper/7.0'}
        )
        return self.session
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
        if self.connector:
            await self.connector.close()

# Gemini rate limiter
class OptimizedGeminiRateLimiter:
    """Rate limiter with in-memory tracking"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        genai.configure(api_key=api_key)
        
        # Use in-memory rate tracking only
        self.rate_tracking = defaultdict(lambda: defaultdict(int))
        self.rate_cache = RateLimitCache()
        
        # Initialize models
        self.models = sorted(GEMINI_MODELS, key=lambda x: x.priority)
        self.model_instances = {}
        
        for model_config in self.models:
            try:
                generation_config = {
                    "max_output_tokens": 2048,
                    "temperature": 0.3,
                    "top_p": 0.8,
                }
                self.model_instances[model_config.name] = genai.GenerativeModel(
                    model_config.name,
                    generation_config=generation_config
                )
                logger.info(f"✅ Initialized model: {model_config.name}")
            except Exception as e:
                logger.error(f"💥 Failed to initialize {model_config.name}: {e}")
        
        self.current_model = self.models[0].name if self.models else None
    
    def _get_current_usage(self, model_name: str) -> Dict[str, int]:
        """Get current usage from in-memory tracking"""
        now = datetime.now(timezone.utc)
        date_key = now.strftime("%Y-%m-%d")
        minute_key = now.strftime("%Y-%m-%d-%H-%M")
        
        return {
            "daily_requests": self.rate_tracking[model_name][f"daily_{date_key}"],
            "minute_requests": self.rate_tracking[model_name][f"minute_{minute_key}"]
        }
    
    def _check_rate_limits_cached(self, model_name: str) -> Tuple[bool, Dict[str, Any]]:
        """Check rate limits with caching"""
        # Check cache first
        cached_result = self.rate_cache.get_cached_rate_status(model_name)
        if cached_result:
            return cached_result
        
        # Not cached - check in-memory tracking
        if model_name not in MODEL_CONFIGS:
            return False, {"error": f"Unknown model: {model_name}"}
        
        config = MODEL_CONFIGS[model_name]
        usage = self._get_current_usage(model_name)
        
        # Check all rate limits
        limits_status = {
            "daily_requests": {
                "current": usage["daily_requests"],
                "limit": config.safe_rpd,
                "exceeded": usage["daily_requests"] >= config.safe_rpd
            },
            "minute_requests": {
                "current": usage["minute_requests"],
                "limit": config.safe_rpm,
                "exceeded": usage["minute_requests"] >= config.safe_rpm
            },
        }
        
        any_exceeded = any(status["exceeded"] for status in limits_status.values())
        can_use = not any_exceeded
        
        status = {
            "model": model_name,
            "can_use": can_use,
            "limits": limits_status,
            "priority": config.priority
        }
        
        # Cache the result
        self.rate_cache.cache_rate_status(model_name, can_use, status)
        
        return can_use, status
    
    def _find_available_model(self) -> Optional[Tuple[str, Dict]]:
        """Find the best available model that's within rate limits"""
        for model_config in self.models:
            if model_config.name not in self.model_instances:
                continue
                
            can_use, status = self._check_rate_limits_cached(model_config.name)
            if can_use:
                return model_config.name, status
        
        return None
    
    def _increment_usage(self, model_name: str):
        """Increment in-memory usage counters"""
        now = datetime.now(timezone.utc)
        date_key = now.strftime("%Y-%m-%d")
        minute_key = now.strftime("%Y-%m-%d-%H-%M")
        
        self.rate_tracking[model_name][f"daily_{date_key}"] += 1
        self.rate_tracking[model_name][f"minute_{minute_key}"] += 1
        
        # Clean up old entries periodically
        self._cleanup_old_entries()
        
        # Invalidate cache for this model
        self.rate_cache.cache.pop(model_name, None)
        self.rate_cache.timestamps.pop(model_name, None)
    
    def _cleanup_old_entries(self):
        """Clean up old rate tracking entries"""
        # This is a simple cleanup - in production you might want more sophisticated cleanup
        pass
    
    async def generate_content_optimized(
        self,
        prompt: str,
        preferred_model: Optional[str] = None,
        max_wait_seconds: int = 180,
        context: str = "general"
    ) -> Dict[str, Any]:
        """Optimized content generation with caching and connection pooling"""
        start_time = time.time()
        
        # Find available model
        model_result = self._find_available_model()
        if not model_result:
            # Clear expired cache entries and try again
            self.rate_cache.clear_expired()
            model_result = self._find_available_model()
            if not model_result:
                raise Exception("No models available")
        
        selected_model, model_status = model_result
        
        # Generate content with selected model
        try:
            model_instance = self.model_instances[selected_model]
            response = model_instance.generate_content(prompt)
            
            # Use optimized text extraction
            response_text = optimized_extract_text_from_response(response)
            
            if response_text is None:
                raise ValueError(f"Text extraction failed for {selected_model}")
            
            # Context-specific validation
            if context == "paraphrase" and len(response_text) < 20:
                raise ValueError(f"Paraphrase response too short: {len(response_text)} chars")
            
            # Success - increment usage counters
            self._increment_usage(selected_model)
            self.current_model = selected_model
            
            generation_time = time.time() - start_time
            
            return {
                "success": True,
                "text": response_text,
                "model_used": selected_model,
                "model_priority": MODEL_CONFIGS[selected_model].priority,
                "generation_time_seconds": generation_time,
                "context": context,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            # Try one fallback model
            remaining_models = [m for m in self.models 
                              if m.name != selected_model and m.name in self.model_instances]
            
            for fallback_model in remaining_models[:1]:  # Try one fallback
                can_use, status = self._check_rate_limits_cached(fallback_model.name)
                if can_use:
                    try:
                        fallback_instance = self.model_instances[fallback_model.name]
                        response = fallback_instance.generate_content(prompt)
                        response_text = optimized_extract_text_from_response(response)
                        
                        if response_text is None:
                            continue
                        
                        self._increment_usage(fallback_model.name)
                        generation_time = time.time() - start_time
                        
                        return {
                            "success": True,
                            "text": response_text,
                            "model_used": fallback_model.name,
                            "model_priority": fallback_model.priority,
                            "generation_time_seconds": generation_time,
                            "fallback_reason": f"Primary model {selected_model} failed",
                            "context": context,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        }
                    except Exception:
                        continue
            
            raise Exception(f"All models failed. Primary error: {str(e)}")

# Batch content processor
class BatchContentProcessor:
    """Batch processing engine for high throughput"""
    
    def __init__(self, api_key: str):
        self.rate_limiter = OptimizedGeminiRateLimiter(api_key)
        self.duplicate_cache = None  # Will be set by main class
        
        # Optimized prompts
        self.paraphrase_prompt = """
You are a professional financial news editor. Transform the text into professional financial news format.

REQUIREMENTS:
1. Remove URLs, emojis, hashtags, informal language
2. Preserve ALL factual information, numbers, dates, technical terms
3. Use professional, clear language
4. Maintain original meaning and context
5. Return ONLY the paraphrased text - no commentary
6. 20-2000 characters

INPUT: "{text}"
OUTPUT:"""

        self.duplicate_check_prompt = """
Compare NEW MESSAGE with RECENT NEWS. Determine if NEW contains substantially new information.

NEW MESSAGE: {new_message}
RECENT NEWS: {recent_news}

RESPONSE: NEW, DUPLICATE, or UPDATE"""
    
    async def process_single_message_optimized(self, text: str, recent_news: List[Dict]) -> Optional[Dict[str, Any]]:
        """Process single message"""
        try:
            # Fast text cleanup with pre-compiled patterns
            cleaned_text = fast_text_cleanup(text)
            if len(cleaned_text) < 20:
                return None
            
            # Check duplicate cache first
            cached_duplicate = await self.duplicate_cache.get_cached_duplicate_result(cleaned_text) if self.duplicate_cache else None
            
            if cached_duplicate:
                decision, confidence = cached_duplicate
            else:
                # Check for duplicates with API
                decision, confidence = await self._check_for_duplicate_fast(cleaned_text, recent_news)
                if self.duplicate_cache:
                    await self.duplicate_cache.cache_duplicate_result(cleaned_text, decision, confidence)
            
            # Enhanced duplicate detection with multiple checks
            is_duplicate = False
            duplicate_reason = ""
            
            # Check 1: High confidence API decision
            if decision == "DUPLICATE" and confidence >= DUPLICATE_THRESHOLD:
                is_duplicate = True
                duplicate_reason = f"API_DUPLICATE_HIGH_CONF_{confidence}"
            
            # Check 2: Exact text match (more strict)
            elif recent_news and self._is_exact_duplicate(cleaned_text, recent_news):
                is_duplicate = True
                duplicate_reason = "EXACT_TEXT_MATCH"
                confidence = 0.95
            
            # Check 3: Very similar text (similarity > 90%)
            elif recent_news and self._is_very_similar(cleaned_text, recent_news):
                is_duplicate = True
                duplicate_reason = "HIGH_SIMILARITY_MATCH"
                confidence = 0.90
            
            if is_duplicate:
                return {
                    "original_text": text,
                    "paraphrased_text": f"[DUPLICATE] {duplicate_reason} - " + cleaned_text,
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                    "processor": "batch_optimized",
                    "duplicate_decision": decision,
                    "duplicate_confidence": confidence
                }
            
            # Process with Gemini
            prompt = self.paraphrase_prompt.format(text=cleaned_text)
            
            result = await self.rate_limiter.generate_content_optimized(
                prompt=prompt,
                max_wait_seconds=120,
                context="paraphrase"
            )
            
            if not result["success"]:
                return None
            
            # Fast response cleaning
            raw_paraphrased = result["text"]
            cleaned_paraphrased = fast_clean_response(raw_paraphrased)
            is_valid, validated_paraphrased = fast_validate_paraphrase(cleaned_text, cleaned_paraphrased)
            
            if not is_valid:
                validated_paraphrased = fast_fallback_paraphrase(cleaned_text)
            
            return {
                "original_text": text,
                "paraphrased_text": validated_paraphrased,
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "processor": "batch_optimized",
                "model_used": result["model_used"],
                "model_priority": result["model_priority"],
                "generation_time": result["generation_time_seconds"],
                "duplicate_decision": decision,
                "duplicate_confidence": confidence
            }
            
        except Exception as e:
            logger.error(f"💥 Batch processing error: {e}")
            return None
    
    async def process_batch_concurrent(self, messages: List[Tuple[str, Any]], recent_news: List[Dict]) -> List[Dict]:
        """Process batch of messages concurrently"""
        if not messages:
            return []
        
        # Create semaphore to limit concurrent API calls
        import asyncio
        semaphore = asyncio.Semaphore(CONCURRENT_API_CALLS)
        
        async def process_with_semaphore(msg_data):
            async with semaphore:
                text, _ = msg_data
                return await self.process_single_message_optimized(text, recent_news)
        
        # Process all messages concurrently in batches
        results = []
        from config import BATCH_SIZE
        for i in range(0, len(messages), BATCH_SIZE):
            batch = messages[i:i + BATCH_SIZE]
            batch_results = await asyncio.gather(*[process_with_semaphore(msg) for msg in batch], return_exceptions=True)
            
            # Filter out exceptions and None results
            valid_results = [r for r in batch_results if r is not None and not isinstance(r, Exception)]
            results.extend(valid_results)
            
            # Small delay between batches to be respectful to API
            if i + BATCH_SIZE < len(messages):
                await asyncio.sleep(0.5)
        
        return results
    
    async def _check_for_duplicate_fast(self, new_text: str, recent_news: List[Dict]) -> Tuple[str, float]:
        """Fast duplicate checking"""
        if not recent_news:
            return "NEW", 1.0
        
        try:
            # Limit recent news
            recent_news_formatted = []
            for i, news in enumerate(recent_news[:min(50, MAX_COMPARISON_NEWS)], 1):
                news_text = news.get("paraphrased_text", news.get("original_text", ""))
                if news_text:
                    recent_news_formatted.append(f"{i}. {news_text}")
            
            if not recent_news_formatted:
                return "NEW", 1.0
            
            recent_news_str = "\n".join(recent_news_formatted)
            prompt = self.duplicate_check_prompt.format(
                new_message=new_text,
                recent_news=recent_news_str
            )
            
            result = await self.rate_limiter.generate_content_optimized(
                prompt=prompt,
                max_wait_seconds=30,
                context="duplicate_check"
            )
            
            if result["success"]:
                decision = result["text"].strip().upper()
                if "NEW" in decision:
                    return "NEW", 0.9
                elif "DUPLICATE" in decision:
                    return "DUPLICATE", 0.8
                elif "UPDATE" in decision:
                    return "UPDATE", 0.7
            
            return "NEW", 0.5
            
        except Exception:
            return "NEW", 0.5
    
    def _is_exact_duplicate(self, new_text: str, recent_news: List[Dict]) -> bool:
        """Check for exact text duplicates"""
        new_text_normalized = new_text.lower().strip()
        
        for news in recent_news[:50]:  # Check recent 50 items
            existing_text = news.get("paraphrased_text", news.get("original_text", ""))
            if existing_text:
                existing_normalized = existing_text.lower().strip()
                
                # Remove [DUPLICATE] prefix if present
                if existing_normalized.startswith("[duplicate]"):
                    existing_normalized = existing_normalized[11:].strip()
                
                # Exact match or very close match
                if (existing_normalized == new_text_normalized or 
                    abs(len(existing_normalized) - len(new_text_normalized)) < 5 and
                    existing_normalized in new_text_normalized or new_text_normalized in existing_normalized):
                    return True
        return False
    
    def _is_very_similar(self, new_text: str, recent_news: List[Dict]) -> bool:
        """Check for very similar text using simple similarity"""
        new_words = set(new_text.lower().split())
        
        for news in recent_news[:30]:  # Check recent 30 items  
            existing_text = news.get("paraphrased_text", news.get("original_text", ""))
            if existing_text:
                # Remove [DUPLICATE] prefix if present
                if existing_text.lower().startswith("[duplicate]"):
                    existing_text = existing_text[11:].strip()
                
                existing_words = set(existing_text.lower().split())
                
                # Skip if one text is too short
                if len(new_words) < 5 or len(existing_words) < 5:
                    continue
                
                # Calculate Jaccard similarity
                intersection = len(new_words.intersection(existing_words))
                union = len(new_words.union(existing_words))
                
                if union > 0:
                    similarity = intersection / union
                    if similarity > 0.85:  # 85% similarity
                        return True
        return False
