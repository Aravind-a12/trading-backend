"""
Text processing utilities for PostgreSQL Telegram News Scraper
Optimized text processing with pre-compiled regex patterns
"""

import re
import hashlib
import html
import json
import gc
import time
from typing import Tuple, Optional, Any
from functools import lru_cache

# Pre-compiled regex patterns
class OptimizedPatterns:
    """Pre-compiled regex patterns for fast text processing"""
    
    def __init__(self):
        # URLs
        self.url_pattern = re.compile(r'https?://[^\s]+')
        
        # Markdown links
        self.markdown_link_pattern = re.compile(r'\[([^\]]+)\]\([^)]+\)')
        
        # Markdown formatting patterns (** for bold, * for italic, __ for underline)
        self.markdown_bold_pattern = re.compile(r'\*\*([^*]+)\*\*')
        self.markdown_italic_pattern = re.compile(r'\*([^*]+)\*')
        self.markdown_underline_pattern = re.compile(r'__([^_]+)__')
        self.markdown_code_pattern = re.compile(r'`([^`]+)`')
        
        # Emojis
        self.emoji_pattern = re.compile(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF]+')
        
        # Whitespace
        self.whitespace_pattern = re.compile(r'\s+')
        
        # Object references
        self.object_patterns = [
            re.compile(r'<.*object at 0x[0-9a-f]+>', re.IGNORECASE),
            re.compile(r'<google\.generativeai\.types\.generation_types\.GenerateContentResponse', re.IGNORECASE),
            re.compile(r'<google\.generativeai\..*>', re.IGNORECASE),
            re.compile(r'<generativeai\..*>', re.IGNORECASE),
            re.compile(r'GenerateContentResponse object', re.IGNORECASE),
            re.compile(r'response object', re.IGNORECASE),
        ]
        
        # Informal patterns
        self.informal_patterns = [
            re.compile(r'\b(breaking|BREAKING)\b', re.IGNORECASE),
            re.compile(r'\b(just|just now)\b', re.IGNORECASE), 
            re.compile(r'\b(huge|big|massive)\b', re.IGNORECASE),
            re.compile(r'\b(check out|look at)\b', re.IGNORECASE),
            re.compile(r'\b(this is|that is)\b', re.IGNORECASE),
            re.compile(r'\b(🚀|📈|💥|🔥|⚡)', re.IGNORECASE),
            re.compile(r'\b(btw|fyi|imo|tbh)\b', re.IGNORECASE),
            re.compile(r'\b(omg|wow|amazing|incredible)\b', re.IGNORECASE)
        ]
        
        # Contractions
        self.contractions = {
            re.compile(r"\bdon't\b", re.IGNORECASE): "do not",
            re.compile(r"\bcan't\b", re.IGNORECASE): "cannot", 
            re.compile(r"\bwon't\b", re.IGNORECASE): "will not",
            re.compile(r"\bhasn't\b", re.IGNORECASE): "has not",
            re.compile(r"\bhaven't\b", re.IGNORECASE): "have not",
            re.compile(r"\bdoesn't\b", re.IGNORECASE): "does not"
        }
        
        # Markdown escape characters
        self.markdown_chars = ['\\', '_', '*', '[', ']', '(', ')', '~', '`', 
                              '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']

# Global instance for reuse
PATTERNS = OptimizedPatterns()

# Text extraction and validation
@lru_cache(maxsize=1000)
def fast_validate_text_content(text_hash: str, text: str) -> Tuple[bool, str]:
    """
    Cached text validation
    Returns: (is_valid, validated_text_or_error_message)
    """
    if not isinstance(text, str):
        return False, f"Content is not string: {type(text)}"
    
    # Check for object reference strings using pre-compiled patterns
    for pattern in PATTERNS.object_patterns:
        if pattern.search(text):
            return False, f"Object reference detected: {pattern.pattern}"
    
    # Check minimum length
    if len(text.strip()) < 5:
        return False, f"Text too short: {len(text.strip())} chars"
    
    # Check for suspicious API-related content
    suspicious_phrases = ["generate_content", "api response", "model response", 
                         "text property", "candidates[0]", "parts[0]"]
    
    lower_text = text.lower()
    for phrase in suspicious_phrases:
        if phrase in lower_text and len(text) < 100:
            return False, f"Suspicious API content: {phrase}"
    
    return True, text.strip()

def optimized_extract_text_from_response(response) -> Optional[str]:
    """
    Extract text from API response
    """
    if response is None:
        return None
    
    # Check if response is already a string
    if isinstance(response, str):
        text_hash = hashlib.md5(response.encode()).hexdigest()
        is_valid, result = fast_validate_text_content(text_hash, response)
        return result if is_valid else None
    
    # Validate response object has expected structure
    if not hasattr(response, '__dict__'):
        return None
    
    # METHOD 1: Direct .text property (most effective method)
    try:
        if hasattr(response, 'text') and response.text is not None:
            if isinstance(response.text, str) and len(response.text.strip()) > 0:
                extracted_text = response.text.strip()
                text_hash = hashlib.md5(extracted_text.encode()).hexdigest()
                is_valid, validated_text = fast_validate_text_content(text_hash, extracted_text)
                return validated_text if is_valid else None
    except Exception:
        pass
    
    return None

def optimized_safe_message_for_telegram(text: Any) -> str:
    """
    Sanitize text for Telegram
    """
    if text is None:
        return "💥 Error: No content available"
    
    text_str = str(text) if not isinstance(text, str) else text
    text_hash = hashlib.md5(text_str.encode()).hexdigest()
    
    # Use cached validation
    is_valid, result = fast_validate_text_content(text_hash, text_str)
    if not is_valid:
        return f"💥 Error: Content validation failed - {result}"
    
    # Fast cleaning with pre-compiled patterns
    clean_text = PATTERNS.url_pattern.sub('', result)
    clean_text = PATTERNS.markdown_bold_pattern.sub(r'\1', clean_text)  # **text** -> text
    clean_text = PATTERNS.markdown_italic_pattern.sub(r'\1', clean_text)  # *text* -> text  
    clean_text = PATTERNS.markdown_underline_pattern.sub(r'\1', clean_text)  # __text__ -> text
    clean_text = PATTERNS.markdown_code_pattern.sub(r'\1', clean_text)  # `text` -> text
    clean_text = PATTERNS.whitespace_pattern.sub(' ', clean_text).strip()
    
    # Truncate if too long
    if len(clean_text) > 2500:
        clean_text = clean_text[:2497] + "..."
    
    return clean_text if len(clean_text.strip()) >= 10 else "💥 Error: Content too short after cleaning"

# These are the private functions used internally - make them public
def _fast_text_cleanup(text: str) -> str:
    """Fast text cleanup using pre-compiled patterns"""
    # Use pre-compiled patterns
    text = PATTERNS.url_pattern.sub('', text)
    text = PATTERNS.markdown_link_pattern.sub(r'\1', text)
    
    # Clean markdown formatting patterns
    text = PATTERNS.markdown_bold_pattern.sub(r'\1', text)  # **text** -> text
    text = PATTERNS.markdown_italic_pattern.sub(r'\1', text)  # *text* -> text
    text = PATTERNS.markdown_underline_pattern.sub(r'\1', text)  # __text__ -> text
    text = PATTERNS.markdown_code_pattern.sub(r'\1', text)  # `text` -> text
    
    text = PATTERNS.emoji_pattern.sub(' ', text)
    text = PATTERNS.whitespace_pattern.sub(' ', text).strip()
    return text

def _fast_clean_response(text: str) -> str:
    """Fast response cleaning"""
    text = re.sub(r'^["\']|["\']$', '', text.strip())
    return text.strip()

def _fast_validate_paraphrase(original: str, paraphrased: str) -> Tuple[bool, str]:
    """Fast paraphrase validation"""
    # Remove common prefixes quickly
    unwanted_prefixes = [
        "Here's a professional paraphrase:",
        "Professional paraphrase:",
        "Paraphrased text:",
    ]
    
    cleaned = paraphrased
    for prefix in unwanted_prefixes:
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
            break
    
    # Quick checks
    if len(cleaned) < 20:
        return False, "Too short"
    
    if cleaned.lower().strip() == original.lower().strip():
        return False, "Identical to original"
    
    return True, cleaned

def _fast_fallback_paraphrase(text: str) -> str:
    """Fast rule-based fallback paraphrasing"""
    cleaned = text
    
    # Remove informal patterns using pre-compiled regex
    for pattern in PATTERNS.informal_patterns:
        cleaned = pattern.sub('', cleaned)
    
    # Replace contractions
    for pattern, replacement in PATTERNS.contractions.items():
        cleaned = pattern.sub(replacement, cleaned)
    
    # Clean whitespace and capitalize
    cleaned = PATTERNS.whitespace_pattern.sub(' ', cleaned).strip()
    if cleaned and not cleaned[0].isupper():
        cleaned = cleaned[0].upper() + cleaned[1:]
    
    return cleaned if len(cleaned) > 20 else text

# Public aliases for backward compatibility
fast_text_cleanup = _fast_text_cleanup
fast_clean_response = _fast_clean_response
fast_validate_paraphrase = _fast_validate_paraphrase
fast_fallback_paraphrase = _fast_fallback_paraphrase
