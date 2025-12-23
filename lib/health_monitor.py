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
        """Analyze recent sync errors."""
        try:
            since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            
            # Get error logs
            result = supabase.table("sync_logs") \
                .select("*") \
                .eq("status", "error") \
                .gte("created_at", since) \
                .execute()
            
            errors = result.data or []
            error_count = len(errors)
            
            # Categorize errors
            error_types = {}
            for err in errors:
                event_type = err.get("event_type", "unknown")
                error_types[event_type] = error_types.get(event_type, 0) + 1
            
            if error_count == 0:
                status = HealthStatus.HEALTHY
                message = "No errors in last 24h"
            elif error_count < 10:
                status = HealthStatus.DEGRADED
                message = f"{error_count} errors in last 24h"
                self.warnings.append(f"Found {error_count} sync errors")
            else:
                status = HealthStatus.UNHEALTHY
                message = f"{error_count} errors in last 24h - investigate!"
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
            self.check_calendar_sync(),
            self.check_gmail_sync(),
            self.check_contact_sync(),
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
    """Legacy: Check if a sync service has failed too many times."""
    monitor = SystemHealthMonitor()
    if service_name == "calendar_sync":
        result = await monitor.check_calendar_sync()
    elif service_name == "gmail_sync":
        result = await monitor.check_gmail_sync()
    else:
        result = await monitor.check_sync_errors()
    
    return {"healthy": result.status == HealthStatus.HEALTHY}


async def get_sync_statistics(hours: int = 24):
    """Legacy: Get sync statistics for the last N hours."""
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        response = supabase.table("sync_logs") \
            .select("event_type, status") \
            .gte("created_at", cutoff.isoformat()) \
            .execute()
        
        logs = response.data
        if not logs:
            return {"total": 0, "success": 0, "error": 0, "rate": 0}
        
        total = len(logs)
        success = len([l for l in logs if l.get("status") == "success"])
        error = len([l for l in logs if l.get("status") == "error"])
        
        return {
            "total": total,
            "success": success,
            "error": error,
            "success_rate": round((success / total) * 100, 1) if total > 0 else 0
        }
        
    except Exception as e:
        logger.error(f"Failed to get sync statistics: {e}")
        return {"error": str(e)}


async def run_health_check(send_telegram: bool = False) -> SystemHealthReport:
    """Run health check and optionally send to Telegram."""
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
