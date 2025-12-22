"""
ActivityWatch Client - Communicates with local ActivityWatch server

ActivityWatch runs locally on port 5600 and provides a REST API to access
tracked activity data including:
- Window focus events (which app/window is active)
- AFK status (keyboard/mouse activity)
- Browser tab tracking (via browser extension)

This client is designed to be used by the sync service to pull data
from the local ActivityWatch instance and upload to Supabase.
"""

import httpx
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Default ActivityWatch server URL (local)
ACTIVITYWATCH_BASE_URL = "http://localhost:5600"


class ActivityWatchClient:
    """Client for ActivityWatch REST API."""
    
    def __init__(self, base_url: str = ACTIVITYWATCH_BASE_URL):
        self.base_url = base_url.rstrip('/')
        
    async def is_available(self) -> bool:
        """Check if ActivityWatch server is running."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.base_url}/api/0/info")
                return response.status_code == 200
        except Exception:
            return False
    
    async def get_buckets(self) -> Dict[str, Any]:
        """Get all available buckets from ActivityWatch."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(f"{self.base_url}/api/0/buckets/")
            response.raise_for_status()
            return response.json()
    
    async def get_events(
        self,
        bucket_id: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Get events from a bucket within a time range.
        
        Args:
            bucket_id: The bucket ID to query
            start: Start of time range (default: 24 hours ago)
            end: End of time range (default: now)
            limit: Maximum number of events to return
            
        Returns:
            List of event dictionaries
        """
        if end is None:
            end = datetime.now(timezone.utc)
        if start is None:
            start = end - timedelta(hours=24)
            
        params = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": limit
        }
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                f"{self.base_url}/api/0/buckets/{bucket_id}/events",
                params=params
            )
            response.raise_for_status()
            return response.json()
    
    async def get_bucket_info(self, bucket_id: str) -> Dict[str, Any]:
        """Get metadata for a specific bucket."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(f"{self.base_url}/api/0/buckets/{bucket_id}")
            response.raise_for_status()
            return response.json()


def parse_window_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Parse a window watcher event into normalized format."""
    data = event.get("data", {})
    return {
        "app_name": data.get("app"),
        "window_title": data.get("title"),
        "timestamp": event.get("timestamp"),
        "duration": event.get("duration", 0),
        "event_id": event.get("id"),
    }


def parse_afk_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Parse an AFK watcher event into normalized format."""
    data = event.get("data", {})
    return {
        "afk_status": data.get("status"),  # 'afk' or 'not-afk'
        "timestamp": event.get("timestamp"),
        "duration": event.get("duration", 0),
        "event_id": event.get("id"),
    }


def parse_web_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Parse a web watcher event into normalized format."""
    data = event.get("data", {})
    url = data.get("url", "")
    
    # Extract domain from URL
    domain = None
    if url:
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            # Remove 'www.' prefix
            if domain.startswith("www."):
                domain = domain[4:]
        except Exception:
            pass
    
    return {
        "url": url,
        "site_domain": domain,
        "tab_title": data.get("title"),
        "timestamp": event.get("timestamp"),
        "duration": event.get("duration", 0),
        "event_id": event.get("id"),
        "audible": data.get("audible", False),
        "incognito": data.get("incognito", False),
    }


def categorize_app(app_name: str) -> str:
    """
    Categorize an application as productive, neutral, or distracting.
    
    Returns: 'productive', 'neutral', or 'distracting'
    """
    if not app_name:
        return "neutral"
    
    app_lower = app_name.lower()
    
    # Productive apps
    productive_apps = [
        "code", "visual studio", "pycharm", "intellij", "webstorm",  # IDEs
        "terminal", "powershell", "cmd", "iterm", "warp",  # Terminals
        "notion", "obsidian", "roam", "logseq",  # Note-taking
        "slack", "teams", "zoom", "meet",  # Work communication
        "figma", "sketch", "photoshop", "illustrator",  # Design
        "excel", "sheets", "word", "docs", "powerpoint",  # Office
        "calendar", "outlook",  # Planning
        "github", "gitlab", "bitbucket",  # Version control
        "postman", "insomnia",  # API tools
        "datagrip", "dbeaver", "tableplus",  # Database
    ]
    
    # Distracting apps/sites
    distracting_apps = [
        "youtube", "netflix", "twitch", "disney",  # Streaming
        "twitter", "x", "facebook", "instagram", "tiktok", "reddit",  # Social media
        "whatsapp", "telegram", "messenger", "discord",  # Personal chat (context-dependent)
        "game", "steam", "epic games",  # Gaming
    ]
    
    for prod in productive_apps:
        if prod in app_lower:
            return "productive"
    
    for dist in distracting_apps:
        if dist in app_lower:
            return "distracting"
    
    return "neutral"


def categorize_website(domain: str) -> str:
    """
    Categorize a website domain as productive, neutral, or distracting.
    
    Returns: 'productive', 'neutral', or 'distracting'
    """
    if not domain:
        return "neutral"
    
    domain_lower = domain.lower()
    
    # Productive domains
    productive_domains = [
        "github.com", "gitlab.com", "bitbucket.org",
        "stackoverflow.com", "stackexchange.com",
        "docs.google.com", "notion.so", "linear.app",
        "figma.com", "canva.com",
        "claude.ai", "chat.openai.com", "chatgpt.com",
        "cloud.google.com", "console.aws.amazon.com", "portal.azure.com",
        "supabase.com", "vercel.com", "netlify.com",
        "medium.com", "dev.to", "hashnode.com",
        "udemy.com", "coursera.org", "linkedin.com/learning",
    ]
    
    # Distracting domains
    distracting_domains = [
        "youtube.com", "netflix.com", "twitch.tv", "disneyplus.com",
        "twitter.com", "x.com", "facebook.com", "instagram.com", "tiktok.com",
        "reddit.com", "9gag.com", "imgur.com",
        "amazon.com", "ebay.com",  # Shopping (usually distracting)
        "news.ycombinator.com",  # HN can be a time sink
    ]
    
    for prod in productive_domains:
        if domain_lower == prod or domain_lower.endswith("." + prod):
            return "productive"
    
    for dist in distracting_domains:
        if domain_lower == dist or domain_lower.endswith("." + dist):
            return "distracting"
    
    return "neutral"
