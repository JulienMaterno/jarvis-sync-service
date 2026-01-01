"""Run full health check."""
import asyncio
from lib.health_monitor import run_health_check, SystemHealthMonitor

async def main():
    monitor = SystemHealthMonitor()
    report = await monitor.run_full_health_check()
    
    print(monitor.format_report_markdown(report))

asyncio.run(main())
