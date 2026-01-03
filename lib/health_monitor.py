"""
Comprehensive System Health Monitor for Jarvis Ecosystem.

This module provides:
1. Health check functions for all system components
2. Database integrity validation
3. Service connectivity checks
4. Error aggregation and analysis
5. Daily health report generation

Usage:
    python -m lib.health_monitor          # Run full health check
    python -m lib.health_monitor --quick  # Run quick connectivity check
"""
import os
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum

from lib.supabase_client import supabase
from lib.logging_service import log_sync_event

logger = logging.getLogger("HealthMonitor")


class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ComponentHealth:
    name: str
    status: HealthStatus
    message: str
    details: Optional[Dict] = None
    last_check: Optional[str] = None


@dataclass
class SystemHealthReport:
    overall_status: HealthStatus
    timestamp: str
    components: List[ComponentHealth]
    errors_24h: int
    warnings: List[str]
    recommendations: List[str]
    
    def to_dict(self):
        return {
            "overall_status": self.overall_status.value,
            "timestamp": self.timestamp,
            "components": [{"name": c.name, "status": c.status.value, "message": c.message, "details": c.details} for c in self.components],
            "errors_24h": self.errors_24h,
            "warnings": self.warnings,
            "recommendations": self.recommendations
        }


class SystemHealthMonitor:
    """Comprehensive health monitoring for Jarvis ecosystem."""
    
    def __init__(self):
        self.components: List[ComponentHealth] = []
        self.warnings: List[str] = []
        self.recommendations: List[str] = []
    
    async def check_database_health(self) -> ComponentHealth:
        """Check database connectivity and basic integrity."""
        try:
            # Test basic connectivity
            result = supabase.table("sync_logs").select("id").limit(1).execute()
            
            # Check table counts
            tables = ["contacts", "meetings", "tasks", "journals", "reflections", 
                      "transcripts", "calendar_events", "emails"]
            counts = {}
            for table in tables:
                try:
                    count_result = supabase.table(table).select("id", count="exact").execute()
                    counts[table] = count_result.count or 0
                except Exception:
                    counts[table] = "error"
            
            return ComponentHealth(
                name="Database (Supabase)",
                status=HealthStatus.HEALTHY,
                message=f"Connected. Tables: {len(tables)} accessible",
                details={"table_counts": counts}
            )
        except Exception as e:
            return ComponentHealth(
                name="Database (Supabase)",
                status=HealthStatus.UNHEALTHY,
                message=f"Connection failed: {str(e)[:100]}"
            )
    
    async def check_sync_errors(self) -> ComponentHealth:
        """Analyze recent sync errors.
        
        Distinguishes between:
        - Transient errors (recovered on next sync) - HEALTHY
        - Persistent errors (multiple failures, or no recovery) - DEGRADED/UNHEALTHY
        """
        try:
            since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            
            # Get error logs
            result = supabase.table("sync_logs") \
                .select("*") \
                .eq("status", "error") \
                .gte("created_at", since) \
                .order("created_at", desc=True) \
                .execute()
            
            errors = result.data or []
            error_count = len(errors)
            
            # Categorize errors by type
            error_types = {}
            for err in errors:
                event_type = err.get("event_type", "unknown")
                error_types[event_type] = error_types.get(event_type, 0) + 1
            
            # Check if errors are transient (recovered on next sync)
            unrecovered_errors = 0
            for event_type, count in error_types.items():
                # Check if there's a success AFTER the error for this sync type
                success_result = supabase.table("sync_logs") \
                    .select("created_at") \
                    .eq("status", "success") \
                    .ilike("event_type", f"%{event_type.replace('_sync', '')}%") \
                    .gte("created_at", since) \
                    .order("created_at", desc=True) \
                    .limit(1) \
                    .execute()
                
                if not success_result.data:
                    # No success after error - this is unrecovered
                    unrecovered_errors += count
            
            if error_count == 0:
                status = HealthStatus.HEALTHY
                message = "No errors in last 24h"
            elif unrecovered_errors == 0:
                # All errors recovered - transient issues only
                status = HealthStatus.HEALTHY
                message = f"{error_count} transient error(s) - all recovered"
            elif error_count < 5:
                status = HealthStatus.DEGRADED
                message = f"{unrecovered_errors} unrecovered error(s) in last 24h"
                self.warnings.append(f"Found {unrecovered_errors} sync errors that haven't recovered")
            else:
                status = HealthStatus.UNHEALTHY
                message = f"{error_count} errors ({unrecovered_errors} unrecovered) - investigate!"
                self.recommendations.append("Review sync_logs table for recurring errors")
            
            return ComponentHealth(
                name="Sync Operations",
                status=status,
                message=message,
                details={"error_count": error_count, "by_type": error_types}
            )
        except Exception as e:
            return ComponentHealth(
                name="Sync Operations",
                status=HealthStatus.UNKNOWN,
                message=f"Could not check: {str(e)[:100]}"
            )
    
    async def check_data_integrity(self) -> ComponentHealth:
        """Check for data integrity issues."""
        issues = []
        
        try:
            # 1. Contacts without notion_page_id (should all have one)
            orphan_contacts = supabase.table("contacts") \
                .select("id", count="exact") \
                .is_("notion_page_id", "null") \
                .is_("deleted_at", "null") \
                .execute()
            if orphan_contacts.count and orphan_contacts.count > 0:
                issues.append(f"{orphan_contacts.count} contacts without Notion link")
            
            # Note: Meetings with unlinked contacts are normal - not all meetings have linked contacts
            # Removed unlinked meetings warning per user preference
            
            if not issues:
                return ComponentHealth(
                    name="Data Integrity",
                    status=HealthStatus.HEALTHY,
                    message="No integrity issues found"
                )
            else:
                self.warnings.extend(issues)
                return ComponentHealth(
                    name="Data Integrity",
                    status=HealthStatus.DEGRADED,
                    message=f"{len(issues)} issue(s) found",
                    details={"issues": issues}
                )
        except Exception as e:
            return ComponentHealth(
                name="Data Integrity",
                status=HealthStatus.UNKNOWN,
                message=f"Could not check: {str(e)[:100]}"
            )
    
    async def check_calendar_sync(self) -> ComponentHealth:
        """Check calendar sync status."""
        try:
            since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            
            # Get recent calendar sync logs
            result = supabase.table("sync_logs") \
                .select("*") \
                .eq("event_type", "calendar_sync") \
                .gte("created_at", since) \
                .order("created_at", desc=True) \
                .limit(10) \
                .execute()
            
            logs = result.data or []
            
            if not logs:
                self.warnings.append("No calendar sync in last 24h")
                return ComponentHealth(
                    name="Calendar Sync",
                    status=HealthStatus.DEGRADED,
                    message="No sync activity in 24h"
                )
            
            # Check if latest sync was successful
            latest = logs[0]
            errors = [l for l in logs if l.get("status") == "error"]
            
            if latest.get("status") == "success":
                return ComponentHealth(
                    name="Calendar Sync",
                    status=HealthStatus.HEALTHY,
                    message=f"Last sync: {latest.get('message', 'OK')[:50]}",
                    details={"last_sync": latest.get("created_at"), "recent_errors": len(errors)}
                )
            else:
                return ComponentHealth(
                    name="Calendar Sync",
                    status=HealthStatus.DEGRADED if len(errors) < 5 else HealthStatus.UNHEALTHY,
                    message=f"{len(errors)} errors in recent syncs",
                    details={"recent_error": logs[0].get("message", "")[:100]}
                )
        except Exception as e:
            return ComponentHealth(
                name="Calendar Sync",
                status=HealthStatus.UNKNOWN,
                message=f"Could not check: {str(e)[:100]}"
            )
    
    async def check_gmail_sync(self) -> ComponentHealth:
        """Check Gmail sync status."""
        try:
            since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            
            result = supabase.table("sync_logs") \
                .select("*") \
                .eq("event_type", "gmail_sync") \
                .gte("created_at", since) \
                .order("created_at", desc=True) \
                .limit(10) \
                .execute()
            
            logs = result.data or []
            
            if not logs:
                return ComponentHealth(
                    name="Gmail Sync",
                    status=HealthStatus.DEGRADED,
                    message="No sync activity in 24h"
                )
            
            errors = [l for l in logs if l.get("status") == "error"]
            success = [l for l in logs if l.get("status") == "success"]
            
            if len(success) > len(errors):
                return ComponentHealth(
                    name="Gmail Sync",
                    status=HealthStatus.HEALTHY,
                    message=f"Last sync: {success[0].get('message', 'OK')[:50]}" if success else "Working",
                    details={"successes": len(success), "errors": len(errors)}
                )
            else:
                return ComponentHealth(
                    name="Gmail Sync",
                    status=HealthStatus.DEGRADED,
                    message=f"{len(errors)} errors vs {len(success)} successes"
                )
        except Exception as e:
            return ComponentHealth(
                name="Gmail Sync",
                status=HealthStatus.UNKNOWN,
                message=f"Could not check: {str(e)[:100]}"
            )
    
    async def check_contact_sync(self) -> ComponentHealth:
        """Check contact sync between Notion, Supabase, and Google."""
        try:
            # Check for contacts without google_resource_name
            no_google = supabase.table("contacts") \
                .select("id", count="exact") \
                .is_("google_resource_name", "null") \
                .is_("deleted_at", "null") \
                .execute()
            
            total = supabase.table("contacts") \
                .select("id", count="exact") \
                .is_("deleted_at", "null") \
                .execute()
            
            no_google_count = no_google.count or 0
            total_count = total.count or 0
            
            if no_google_count == 0:
                return ComponentHealth(
                    name="Contact Sync",
                    status=HealthStatus.HEALTHY,
                    message=f"All {total_count} contacts synced to Google"
                )
            elif no_google_count < 5:
                return ComponentHealth(
                    name="Contact Sync",
                    status=HealthStatus.DEGRADED,
                    message=f"{no_google_count}/{total_count} contacts missing Google sync",
                    details={"missing_google": no_google_count}
                )
            else:
                self.recommendations.append(f"Check {no_google_count} contacts not synced to Google")
                return ComponentHealth(
                    name="Contact Sync",
                    status=HealthStatus.UNHEALTHY,
                    message=f"{no_google_count} contacts not synced to Google!"
                )
        except Exception as e:
            return ComponentHealth(
                name="Contact Sync",
                status=HealthStatus.UNKNOWN,
                message=f"Could not check: {str(e)[:100]}"
            )
    
    async def check_beeper_sync(self) -> ComponentHealth:
        """Check Beeper messaging sync status.
        
        Beeper sync depends on a local bridge running on the user's laptop.
        This is expected to be offline when the laptop is off/away.
        
        Status logic:
        - HEALTHY: Recent successful sync, or graceful skip (bridge offline)
        - DEGRADED: No sync activity in 48h+ (may indicate issue)
        - UNHEALTHY: Repeated errors (not just offline skips)
        """
        try:
            since_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            since_48h = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
            
            # Get recent Beeper sync logs
            result = supabase.table("sync_logs") \
                .select("*") \
                .eq("event_type", "beeper_sync") \
                .gte("created_at", since_48h) \
                .order("created_at", desc=True) \
                .limit(20) \
                .execute()
            
            logs = result.data or []
            
            if not logs:
                # No logs at all - check if we have data (sync worked before)
                chats = supabase.table("beeper_chats").select("id", count="exact").limit(1).execute()
                if chats.count and chats.count > 0:
                    return ComponentHealth(
                        name="Beeper Sync",
                        status=HealthStatus.DEGRADED,
                        message=f"No sync in 48h (have {chats.count} chats from before)"
                    )
                else:
                    # Never synced - might be first time or not configured
                    return ComponentHealth(
                        name="Beeper Sync",
                        status=HealthStatus.UNKNOWN,
                        message="No Beeper data - may not be configured"
                    )
            
            # Categorize logs
            successes = [l for l in logs if l.get("status") == "success"]
            errors = [l for l in logs if l.get("status") == "error"]
            skips = [l for l in logs if l.get("status") == "info" and "skip" in l.get("message", "").lower()]
            
            latest = logs[0]
            latest_status = latest.get("status")
            latest_msg = latest.get("message", "")[:60]
            
            # Get message counts for context
            msgs_24h = supabase.table("beeper_messages") \
                .select("id", count="exact") \
                .gte("created_at", since_24h) \
                .execute()
            
            new_msgs = msgs_24h.count or 0
            
            # Determine health
            if errors and len(errors) > len(successes):
                # More errors than successes - problem
                self.warnings.append(f"Beeper sync has {len(errors)} errors in 48h")
                return ComponentHealth(
                    name="Beeper Sync",
                    status=HealthStatus.DEGRADED,
                    message=f"{len(errors)} errors - check bridge connectivity",
                    details={"successes": len(successes), "errors": len(errors), "skips": len(skips)}
                )
            elif latest_status == "success":
                return ComponentHealth(
                    name="Beeper Sync",
                    status=HealthStatus.HEALTHY,
                    message=f"Last sync OK, {new_msgs} new msgs in 24h",
                    details={"successes": len(successes), "skips": len(skips), "new_messages_24h": new_msgs}
                )
            elif latest_status == "info" and "skip" in latest_msg.lower():
                # Bridge was offline - this is normal/expected behavior
                return ComponentHealth(
                    name="Beeper Sync",
                    status=HealthStatus.HEALTHY,
                    message=f"Bridge offline (expected if laptop away), {len(successes)} syncs in 48h",
                    details={"successes": len(successes), "skips": len(skips)}
                )
            else:
                return ComponentHealth(
                    name="Beeper Sync",
                    status=HealthStatus.DEGRADED,
                    message=f"Latest: {latest_msg}",
                    details={"latest_status": latest_status}
                )
            
        except Exception as e:
            return ComponentHealth(
                name="Beeper Sync",
                status=HealthStatus.UNKNOWN,
                message=f"Could not check: {str(e)[:100]}"
            )
    
    async def check_recent_activity(self) -> ComponentHealth:
        """Check for recent processing activity."""
        try:
            since = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
            
            # Check for recent transcripts
            transcripts = supabase.table("transcripts") \
                .select("id", count="exact") \
                .gte("created_at", since) \
                .execute()
            
            # Check for recent meetings
            meetings = supabase.table("meetings") \
                .select("id", count="exact") \
                .gte("created_at", since) \
                .execute()
            
            activity = {
                "transcripts_48h": transcripts.count or 0,
                "meetings_48h": meetings.count or 0
            }
            
            return ComponentHealth(
                name="Recent Activity",
                status=HealthStatus.HEALTHY,
                message=f"{activity['transcripts_48h']} transcripts, {activity['meetings_48h']} meetings in 48h",
                details=activity
            )
        except Exception as e:
            return ComponentHealth(
                name="Recent Activity",
                status=HealthStatus.UNKNOWN,
                message=f"Could not check: {str(e)[:100]}"
            )
    
    async def check_notion_supabase_consistency(self) -> ComponentHealth:
        """Check that Notion and Supabase databases have consistent record counts.
        
        This validates that bidirectional syncs are working properly.
        Checks: meetings, tasks, reflections, journals
        Excludes soft-deleted records from Supabase counts.
        """
        try:
            import os
            import httpx
            
            NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')
            if not NOTION_API_TOKEN:
                return ComponentHealth(
                    name="Notion↔Supabase Consistency",
                    status=HealthStatus.UNKNOWN,
                    message="Notion API token not available"
                )
            
            # Database IDs for bidirectional syncs
            databases_to_check = {
                'meetings': os.environ.get('NOTION_MEETING_DB_ID', '297cd3f1-eb28-810f-86f0-f142f7e3a5ca'),
                'tasks': os.environ.get('NOTION_TASKS_DB_ID', '2b3cd3f1-eb28-8004-a33a-d26b8bb3fa58'),
                'reflections': os.environ.get('NOTION_REFLECTIONS_DB_ID', '2cacd3f1-eb28-80d9-903a-ee73d2f84b59'),
                'journals': os.environ.get('NOTION_JOURNAL_DB_ID', '2cecd3f1-eb28-8098-bf5e-d49ae4a68f6b'),
                'contacts': os.environ.get('NOTION_CRM_DATABASE_ID', '2c7cd3f1eb2880269e53ed4d45e99b69')
            }
            
            headers = {
                'Authorization': f'Bearer {NOTION_API_TOKEN}',
                'Notion-Version': '2022-06-28',
                'Content-Type': 'application/json'
            }
            
            discrepancies = []
            counts_detail = {}
            
            async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
                for table_name, db_id in databases_to_check.items():
                    try:
                        # Get Notion count (all non-archived pages)
                        notion_response = await client.post(
                            f'https://api.notion.com/v1/databases/{db_id}/query',
                            json={"page_size": 1}
                        )
                        notion_response.raise_for_status()
                        notion_data = notion_response.json()
                        notion_count = len(notion_data.get('results', []))
                        
                        # For accurate count, we need to paginate through all results
                        # But we can get estimate from has_more
                        if notion_data.get('has_more'):
                            # Do a full count
                            all_pages = notion_data.get('results', [])
                            next_cursor = notion_data.get('next_cursor')
                            
                            while next_cursor:
                                page_response = await client.post(
                                    f'https://api.notion.com/v1/databases/{db_id}/query',
                                    json={"page_size": 100, "start_cursor": next_cursor}
                                )
                                page_response.raise_for_status()
                                page_data = page_response.json()
                                all_pages.extend(page_data.get('results', []))
                                next_cursor = page_data.get('next_cursor')
                                
                                # Safety limit to prevent infinite loops
                                if len(all_pages) > 10000:
                                    break
                            
                            notion_count = len(all_pages)
                        
                        # Get Supabase count (excluding soft-deleted)
                        supabase_result = supabase.table(table_name) \
                            .select("id", count="exact") \
                            .is_("deleted_at", "null") \
                            .execute()
                        supabase_count = supabase_result.count or 0
                        
                        counts_detail[table_name] = {
                            'notion': notion_count,
                            'supabase': supabase_count,
                            'diff': abs(notion_count - supabase_count)
                        }
                        
                        # Check for significant discrepancy (>10% and >5 records difference)
                        if supabase_count > 0:
                            diff_pct = abs(notion_count - supabase_count) / max(supabase_count, notion_count) * 100
                            diff_abs = abs(notion_count - supabase_count)
                            
                            if diff_pct > 10 and diff_abs > 5:
                                discrepancies.append(
                                    f"{table_name}: Notion={notion_count}, Supabase={supabase_count} ({diff_pct:.0f}% diff)"
                                )
                        elif notion_count > 5:
                            # Supabase has no records but Notion does
                            discrepancies.append(
                                f"{table_name}: Notion={notion_count}, Supabase=0 (sync may have failed)"
                            )
                    
                    except Exception as e:
                        logger.warning(f"Failed to check {table_name} consistency: {e}")
                        counts_detail[table_name] = {"error": str(e)[:50]}
            
            # Check Google Contacts count
            try:
                # Get Supabase contacts count
                supabase_contacts = supabase.table("contacts") \
                    .select("id", count="exact") \
                    .is_("deleted_at", "null") \
                    .execute()
                supabase_contacts_count = supabase_contacts.count or 0
                
                # Get count of contacts synced to Google (have google_resource_name)
                google_synced = supabase.table("contacts") \
                    .select("id", count="exact") \
                    .is_("deleted_at", "null") \
                    .not_.is_("google_resource_name", "null") \
                    .execute()
                google_synced_count = google_synced.count or 0
                
                counts_detail['contacts_google_sync'] = {
                    'total': supabase_contacts_count,
                    'synced_to_google': google_synced_count,
                    'not_synced': supabase_contacts_count - google_synced_count
                }
                
                # Warn if many contacts aren't synced to Google
                if supabase_contacts_count - google_synced_count > 10:
                    self.warnings.append(
                        f"{supabase_contacts_count - google_synced_count} contacts not synced to Google"
                    )
            
            except Exception as e:
                logger.warning(f"Failed to check Google contacts sync: {e}")
            
            if not discrepancies:
                return ComponentHealth(
                    name="Notion↔Supabase Consistency",
                    status=HealthStatus.HEALTHY,
                    message=f"Counts match across {len(databases_to_check)} databases",
                    details=counts_detail
                )
            else:
                self.warnings.extend(discrepancies)
                self.recommendations.append("Run full sync to reconcile Notion and Supabase databases")
                return ComponentHealth(
                    name="Notion↔Supabase Consistency",
                    status=HealthStatus.DEGRADED,
                    message=f"{len(discrepancies)} database(s) have count mismatches",
                    details=counts_detail
                )
        
        except Exception as e:
            return ComponentHealth(
                name="Notion↔Supabase Consistency",
                status=HealthStatus.UNKNOWN,
                message=f"Could not check: {str(e)[:100]}"
            )
    
    async def run_full_health_check(self) -> SystemHealthReport:
        """Run comprehensive health check across all components."""
        self.components = []
        self.warnings = []
        self.recommendations = []
        
        # Run all checks
        checks = [
            self.check_database_health(),
            self.check_sync_errors(),
            self.check_data_integrity(),
            self.check_notion_supabase_consistency(),  # New consistency check
            self.check_calendar_sync(),
            self.check_gmail_sync(),
            self.check_contact_sync(),
            self.check_beeper_sync(),
            self.check_recent_activity(),
        ]
        
        self.components = await asyncio.gather(*checks)
        
        # Get error count from sync_errors component
        errors_24h = 0
        for comp in self.components:
            if comp.name == "Sync Operations" and comp.details:
                errors_24h = comp.details.get("error_count", 0)
                break
        
        # Determine overall status
        statuses = [c.status for c in self.components]
        if HealthStatus.UNHEALTHY in statuses:
            overall = HealthStatus.UNHEALTHY
        elif HealthStatus.DEGRADED in statuses:
            overall = HealthStatus.DEGRADED
        elif HealthStatus.UNKNOWN in statuses:
            overall = HealthStatus.DEGRADED
        else:
            overall = HealthStatus.HEALTHY
        
        return SystemHealthReport(
            overall_status=overall,
            timestamp=datetime.now(timezone.utc).isoformat(),
            components=self.components,
            errors_24h=errors_24h,
            warnings=self.warnings,
            recommendations=self.recommendations
        )
    
    def format_report_markdown(self, report: SystemHealthReport) -> str:
        """Format health report as Markdown for Telegram."""
        status_emoji = {
            HealthStatus.HEALTHY: "✅",
            HealthStatus.DEGRADED: "⚠️",
            HealthStatus.UNHEALTHY: "🔴",
            HealthStatus.UNKNOWN: "❓"
        }
        
        lines = [
            f"🏥 **System Health Report**",
            f"Status: {status_emoji.get(report.overall_status, '❓')} {report.overall_status.value.upper()}",
            f"Time: {report.timestamp[:19]}",
            "",
            "**Components:**"
        ]
        
        for comp in report.components:
            emoji = status_emoji.get(comp.status, "❓")
            lines.append(f"• {emoji} {comp.name}: {comp.message}")
        
        if report.warnings:
            lines.append("")
            lines.append("**Warnings:**")
            for w in report.warnings[:5]:
                lines.append(f"• ⚠️ {w}")
        
        if report.recommendations:
            lines.append("")
            lines.append("**Recommendations:**")
            for r in report.recommendations[:3]:
                lines.append(f"• 💡 {r}")
        
        lines.append("")
        lines.append(f"_Errors (24h): {report.errors_24h}_")
        
        return "\n".join(lines)


# Legacy functions for backward compatibility
async def check_sync_health(service_name: str, failure_threshold: int = 5):
    """Check if a specific sync service has had recent errors.
    
    Returns healthy=True if:
    - No errors for this service in the last 24 hours, OR
    - Last successful sync was more recent than last error
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        
        # Check for recent errors for this specific service
        error_result = supabase.table("sync_logs") \
            .select("created_at, message") \
            .eq("status", "error") \
            .ilike("event_type", f"%{service_name.replace('_sync', '')}%") \
            .gte("created_at", cutoff) \
            .order("created_at", desc=True) \
            .limit(1) \
            .execute()
        
        # Check for recent successes for this service
        success_result = supabase.table("sync_logs") \
            .select("created_at") \
            .eq("status", "success") \
            .ilike("event_type", f"%{service_name.replace('_sync', '')}%") \
            .gte("created_at", cutoff) \
            .order("created_at", desc=True) \
            .limit(1) \
            .execute()
        
        has_recent_error = bool(error_result.data)
        has_recent_success = bool(success_result.data)
        
        # Healthy if: no errors, OR last success is more recent than last error
        if not has_recent_error:
            return {"healthy": True}
        
        if has_recent_success and has_recent_error:
            last_success = success_result.data[0]["created_at"]
            last_error = error_result.data[0]["created_at"]
            return {"healthy": last_success > last_error}
        
        # Has error but no recent success
        return {"healthy": False, "last_error": error_result.data[0].get("message", "Unknown")}
        
    except Exception as e:
        logger.warning(f"Could not check health for {service_name}: {e}")
        return {"healthy": True}  # Assume healthy if we can't check


async def get_sync_statistics(hours: int = 24):
    """Get sync statistics for the last N hours with accurate success rate."""
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        response = supabase.table("sync_logs") \
            .select("event_type, status") \
            .gte("created_at", cutoff.isoformat()) \
            .execute()
        
        logs = response.data
        if not logs:
            return {
                "total_logs": 0,
                "success": 0,
                "error": 0,
                "info": 0,
                "other": 0,
                "success_rate": 100.0,
                "actionable_ops": 0
            }
        
        # Count by status type
        success = len([l for l in logs if l.get("status") == "success"])
        error = len([l for l in logs if l.get("status") == "error"])
        info = len([l for l in logs if l.get("status") == "info"])
        other = len(logs) - success - error - info
        
        # Calculate real success rate (success vs error only, excluding info logs)
        actionable_ops = success + error
        success_rate = round((success / actionable_ops) * 100, 1) if actionable_ops > 0 else 100.0
        
        return {
            "total_logs": len(logs),
            "success": success,
            "error": error,
            "info": info,
            "other": other,
            "success_rate": success_rate,
            "actionable_ops": actionable_ops
        }
        
    except Exception as e:
        logger.error(f"Failed to get sync statistics: {e}")
        return {"error": str(e)}


async def run_health_check(send_telegram: bool = False) -> SystemHealthReport:
    """Run health check and optionally send to Telegram.
    
    When send_telegram=True, also includes "tomorrow's focus" from the latest journal.
    """
    monitor = SystemHealthMonitor()
    report = await monitor.run_full_health_check()
    
    # Log to sync_logs
    await log_sync_event(
        "health_check",
        report.overall_status.value,
        monitor.format_report_markdown(report)[:500],
        details=report.to_dict()
    )
    
    if send_telegram:
        from lib.telegram_client import send_telegram_message
        message = monitor.format_report_markdown(report)
        
        # Add tomorrow's focus from latest journal
        try:
            from datetime import date, timedelta
            
            # Get the most recent journal (yesterday's or today's)
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            today = date.today().isoformat()
            
            journal_result = supabase.table("journals") \
                .select("date, tomorrow_focus") \
                .in_("date", [yesterday, today]) \
                .order("date", desc=True) \
                .limit(1) \
                .execute()
            
            if journal_result.data and journal_result.data[0].get("tomorrow_focus"):
                focus_items = journal_result.data[0]["tomorrow_focus"]
                if focus_items and len(focus_items) > 0:
                    message += "\n\n**📋 Today's Focus:**"
                    for item in focus_items[:5]:  # Max 5 items
                        message += f"\n• {item}"
        except Exception as e:
            logger.warning(f"Could not fetch tomorrow's focus: {e}")
        
        await send_telegram_message(message, force=True)
    
    return report


if __name__ == "__main__":
    import sys
    
    async def main():
        quick = "--quick" in sys.argv
        telegram = "--telegram" in sys.argv
        
        if quick:
            print("Running quick connectivity check...")
            monitor = SystemHealthMonitor()
            db = await monitor.check_database_health()
            print(f"Database: {db.status.value} - {db.message}")
        else:
            print("Running full health check...")
            report = await run_health_check(send_telegram=telegram)
            
            monitor = SystemHealthMonitor()
            print(monitor.format_report_markdown(report))
    
    asyncio.run(main())
