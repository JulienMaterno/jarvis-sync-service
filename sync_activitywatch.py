"""
ActivityWatch Sync Service

Syncs activity data from local ActivityWatch instance to Supabase.
Designed to run periodically to keep cloud data up to date with local tracking.

Note: ActivityWatch runs locally and cannot be accessed from Cloud Run.
This sync must be triggered from a local machine or use a tunnel.
"""

import asyncio
import logging
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from collections import defaultdict

from lib.activitywatch_client import (
    ActivityWatchClient,
    parse_window_event,
    parse_afk_event,
    parse_web_event,
    categorize_app,
    categorize_website,
)
from lib.supabase_client import supabase
from lib.logging_service import log_sync_event

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ActivityWatchSync:
    """Syncs ActivityWatch data to Supabase."""
    
    def __init__(self, aw_url: str = "http://localhost:5600"):
        self.client = ActivityWatchClient(aw_url)
        
    async def get_last_sync_times(self) -> Dict[str, datetime]:
        """Get the last sync timestamp for each bucket."""
        try:
            result = supabase.table("sync_state").select("value").eq(
                "key", "activitywatch_last_sync"
            ).execute()
            
            if result.data and result.data[0].get("value"):
                data = json.loads(result.data[0]["value"])
                # Convert ISO strings back to datetime
                return {
                    k: datetime.fromisoformat(v.replace("Z", "+00:00"))
                    for k, v in data.items()
                }
        except Exception as e:
            logger.warning(f"Failed to get last sync times: {e}")
        
        return {}
    
    async def save_last_sync_times(self, sync_times: Dict[str, datetime]):
        """Save the last sync timestamp for each bucket."""
        try:
            # Convert datetime to ISO strings
            data = {k: v.isoformat() for k, v in sync_times.items()}
            
            supabase.table("sync_state").upsert({
                "key": "activitywatch_last_sync",
                "value": json.dumps(data),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }).execute()
        except Exception as e:
            logger.error(f"Failed to save sync times: {e}")
    
    async def sync_events(
        self,
        hours: int = 24,
        full_sync: bool = False
    ) -> Dict[str, Any]:
        """
        Sync events from ActivityWatch to Supabase.
        
        Args:
            hours: Number of hours to sync (default 24)
            full_sync: If True, ignore last sync time and sync all events
            
        Returns:
            Summary of sync operation
        """
        try:
            # Check if ActivityWatch is available
            if not await self.client.is_available():
                logger.warning("ActivityWatch is not running")
                await log_sync_event(
                    "activitywatch_sync",
                    "warning",
                    "ActivityWatch server not available"
                )
                return {"status": "unavailable", "message": "ActivityWatch not running"}
            
            logger.info("Starting ActivityWatch sync...")
            
            # Get buckets
            buckets = await self.client.get_buckets()
            logger.info(f"Found {len(buckets)} buckets")
            
            # Get last sync times
            last_sync_times = {} if full_sync else await self.get_last_sync_times()
            new_sync_times = {}
            
            total_events = 0
            events_by_bucket = {}
            
            now = datetime.now(timezone.utc)
            default_start = now - timedelta(hours=hours)
            
            for bucket_id, bucket_info in buckets.items():
                bucket_type = bucket_info.get("type", "unknown")
                hostname = bucket_info.get("hostname", "unknown")
                
                # Determine start time for this bucket
                start_time = last_sync_times.get(bucket_id, default_start)
                if full_sync:
                    start_time = default_start
                
                logger.info(f"Syncing bucket {bucket_id} from {start_time}")
                
                try:
                    events = await self.client.get_events(
                        bucket_id,
                        start=start_time,
                        end=now,
                        limit=50000
                    )
                    
                    if not events:
                        logger.info(f"No new events in {bucket_id}")
                        new_sync_times[bucket_id] = now
                        continue
                    
                    logger.info(f"Found {len(events)} events in {bucket_id}")
                    
                    # Process events based on bucket type
                    records = []
                    for event in events:
                        record = self._process_event(event, bucket_id, bucket_type, hostname)
                        if record:
                            records.append(record)
                    
                    # Batch upsert to Supabase
                    if records:
                        await self._upsert_events(records)
                        total_events += len(records)
                        events_by_bucket[bucket_id] = len(records)
                    
                    new_sync_times[bucket_id] = now
                    
                except Exception as e:
                    logger.error(f"Error syncing bucket {bucket_id}: {e}")
                    events_by_bucket[bucket_id] = f"error: {str(e)}"
            
            # Save sync times
            await self.save_last_sync_times(new_sync_times)
            
            # Generate daily summary
            await self._update_daily_summary(now.date())
            
            await log_sync_event(
                "activitywatch_sync",
                "success",
                f"Synced {total_events} events from {len(buckets)} buckets",
                {"events_by_bucket": events_by_bucket}
            )
            
            return {
                "status": "success",
                "total_events": total_events,
                "buckets_synced": len(buckets),
                "details": events_by_bucket
            }
            
        except Exception as e:
            logger.error(f"ActivityWatch sync failed: {e}")
            await log_sync_event("activitywatch_sync", "error", str(e))
            raise
    
    def _process_event(
        self,
        event: Dict[str, Any],
        bucket_id: str,
        bucket_type: str,
        hostname: str
    ) -> Optional[Dict[str, Any]]:
        """Process a single event into database format."""
        try:
            base_record = {
                "bucket_id": bucket_id,
                "bucket_type": bucket_type,
                "hostname": hostname,
                "event_id": event.get("id"),
                "timestamp": event.get("timestamp"),
                "duration": event.get("duration", 0),
                "raw_data": event.get("data", {}),
            }
            
            if bucket_type == "currentwindow":
                parsed = parse_window_event(event)
                base_record.update({
                    "app_name": parsed.get("app_name"),
                    "window_title": parsed.get("window_title"),
                })
                
            elif bucket_type == "afkstatus":
                parsed = parse_afk_event(event)
                base_record.update({
                    "afk_status": parsed.get("afk_status"),
                })
                
            elif bucket_type == "web.tab.current":
                parsed = parse_web_event(event)
                base_record.update({
                    "url": parsed.get("url"),
                    "site_domain": parsed.get("site_domain"),
                    "tab_title": parsed.get("tab_title"),
                })
            
            return base_record
            
        except Exception as e:
            logger.warning(f"Failed to process event: {e}")
            return None
    
    async def _upsert_events(self, records: List[Dict[str, Any]]):
        """Batch upsert events to Supabase."""
        batch_size = 500
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                supabase.table("activity_events").upsert(
                    batch,
                    on_conflict="bucket_id,event_id"
                ).execute()
            except Exception as e:
                logger.error(f"Failed to upsert batch {i}: {e}")
    
    async def _update_daily_summary(self, date: datetime.date):
        """Update or create daily summary for the given date."""
        try:
            # Query events for the date
            start = datetime.combine(date, datetime.min.time(), tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            
            result = supabase.table("activity_events").select("*").gte(
                "timestamp", start.isoformat()
            ).lt(
                "timestamp", end.isoformat()
            ).execute()
            
            if not result.data:
                return
            
            events = result.data
            
            # Get unique hostnames
            hostnames = set(e.get("hostname") for e in events if e.get("hostname"))
            
            for hostname in hostnames:
                host_events = [e for e in events if e.get("hostname") == hostname]
                summary = self._calculate_summary(host_events, date, hostname)
                
                if summary:
                    supabase.table("activity_summaries").upsert(
                        summary,
                        on_conflict="date,hostname"
                    ).execute()
                    
        except Exception as e:
            logger.error(f"Failed to update daily summary: {e}")
    
    def _calculate_summary(
        self,
        events: List[Dict[str, Any]],
        date: datetime.date,
        hostname: str
    ) -> Optional[Dict[str, Any]]:
        """Calculate summary statistics from events."""
        try:
            # Separate by type
            window_events = [e for e in events if e.get("bucket_type") == "currentwindow"]
            afk_events = [e for e in events if e.get("bucket_type") == "afkstatus"]
            web_events = [e for e in events if e.get("bucket_type") == "web.tab.current"]
            
            # Calculate AFK time
            total_afk = sum(
                e.get("duration", 0) 
                for e in afk_events 
                if e.get("afk_status") == "afk"
            )
            total_active_from_afk = sum(
                e.get("duration", 0)
                for e in afk_events
                if e.get("afk_status") == "not-afk"
            )
            
            # Calculate app usage
            app_time = defaultdict(float)
            for e in window_events:
                app = e.get("app_name") or "Unknown"
                app_time[app] += e.get("duration", 0)
            
            top_apps = sorted(
                [{"app": k, "duration": v} for k, v in app_time.items()],
                key=lambda x: x["duration"],
                reverse=True
            )[:10]
            
            total_app_time = sum(app_time.values())
            for app in top_apps:
                app["percentage"] = round(
                    (app["duration"] / total_app_time * 100) if total_app_time > 0 else 0,
                    1
                )
            
            # Calculate website usage
            site_time = defaultdict(float)
            for e in web_events:
                domain = e.get("site_domain") or "Unknown"
                site_time[domain] += e.get("duration", 0)
            
            top_sites = sorted(
                [{"domain": k, "duration": v} for k, v in site_time.items()],
                key=lambda x: x["duration"],
                reverse=True
            )[:10]
            
            total_site_time = sum(site_time.values())
            for site in top_sites:
                site["percentage"] = round(
                    (site["duration"] / total_site_time * 100) if total_site_time > 0 else 0,
                    1
                )
            
            # Calculate productivity breakdown
            productive_time = 0
            distracting_time = 0
            neutral_time = 0
            
            for app, duration in app_time.items():
                category = categorize_app(app)
                if category == "productive":
                    productive_time += duration
                elif category == "distracting":
                    distracting_time += duration
                else:
                    neutral_time += duration
            
            for domain, duration in site_time.items():
                category = categorize_website(domain)
                if category == "productive":
                    productive_time += duration
                elif category == "distracting":
                    distracting_time += duration
                # Note: neutral web time not added to avoid double-counting
            
            # Calculate hourly breakdown
            hourly = defaultdict(lambda: {"active": 0, "afk": 0})
            for e in afk_events:
                ts = datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00"))
                hour = ts.hour
                if e.get("afk_status") == "afk":
                    hourly[hour]["afk"] += e.get("duration", 0)
                else:
                    hourly[hour]["active"] += e.get("duration", 0)
            
            hourly_breakdown = [
                {"hour": h, **data} 
                for h, data in sorted(hourly.items())
            ]
            
            return {
                "date": str(date),
                "hostname": hostname,
                "total_active_time": total_active_from_afk,
                "total_afk_time": total_afk,
                "top_apps": top_apps,
                "top_sites": top_sites,
                "productive_time": productive_time,
                "neutral_time": neutral_time,
                "distracting_time": distracting_time,
                "hourly_breakdown": hourly_breakdown,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate summary: {e}")
            return None
    
    async def get_today_summary(self) -> Optional[Dict[str, Any]]:
        """Get or generate summary for today."""
        today = datetime.now(timezone.utc).date()
        
        # Try to get existing summary
        result = supabase.table("activity_summaries").select("*").eq(
            "date", str(today)
        ).execute()
        
        if result.data:
            return result.data[0]
        
        # Generate summary from events
        await self._update_daily_summary(today)
        
        result = supabase.table("activity_summaries").select("*").eq(
            "date", str(today)
        ).execute()
        
        return result.data[0] if result.data else None


async def run_activitywatch_sync(hours: int = 24, full: bool = False) -> Dict[str, Any]:
    """Run ActivityWatch sync."""
    sync = ActivityWatchSync()
    return await sync.sync_events(hours=hours, full_sync=full)


def format_activity_summary_for_journal(summary: Dict[str, Any]) -> str:
    """Format activity summary for inclusion in evening journal prompt."""
    if not summary:
        return "No activity data available for today."
    
    lines = []
    
    # Time summary
    active_hours = summary.get("total_active_time", 0) / 3600
    afk_hours = summary.get("total_afk_time", 0) / 3600
    lines.append(f"**Screen Time Today**: {active_hours:.1f}h active, {afk_hours:.1f}h away")
    
    # Productivity breakdown
    prod = summary.get("productive_time", 0) / 3600
    dist = summary.get("distracting_time", 0) / 3600
    neut = summary.get("neutral_time", 0) / 3600
    
    if prod + dist + neut > 0:
        lines.append(f"**Productivity**: {prod:.1f}h productive, {dist:.1f}h distracting, {neut:.1f}h neutral")
    
    # Top apps
    top_apps = summary.get("top_apps", [])[:5]
    if top_apps:
        app_lines = []
        for app in top_apps:
            hours = app["duration"] / 3600
            if hours >= 0.1:  # Only show if >= 6 minutes
                app_lines.append(f"  • {app['app']}: {hours:.1f}h ({app['percentage']}%)")
        if app_lines:
            lines.append("**Top Apps**:")
            lines.extend(app_lines)
    
    # Top sites
    top_sites = summary.get("top_sites", [])[:5]
    if top_sites:
        site_lines = []
        for site in top_sites:
            hours = site["duration"] / 3600
            if hours >= 0.1:
                site_lines.append(f"  • {site['domain']}: {hours:.1f}h ({site['percentage']}%)")
        if site_lines:
            lines.append("**Top Websites**:")
            lines.extend(site_lines)
    
    return "\n".join(lines)


# CLI for testing
if __name__ == "__main__":
    import sys
    
    async def main():
        sync = ActivityWatchSync()
        
        if not await sync.client.is_available():
            print("ActivityWatch is not running!")
            sys.exit(1)
        
        print("ActivityWatch is available. Starting sync...")
        result = await sync.sync_events(hours=24, full_sync=True)
        print(f"\nSync result: {json.dumps(result, indent=2)}")
        
        print("\n--- Today's Summary ---")
        summary = await sync.get_today_summary()
        if summary:
            print(format_activity_summary_for_journal(summary))
        else:
            print("No summary available")
    
    asyncio.run(main())
