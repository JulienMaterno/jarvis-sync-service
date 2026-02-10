#!/usr/bin/env python3
"""
Check progress of batch processing.
"""
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

ENHANCED_DIR = Path(__file__).parent.parent / "data" / "enhanced_haiku"
LOG_FILE = ENHANCED_DIR / "processing_parallel.log"  # Use parallel log
FALLBACK_LOG = ENHANCED_DIR / "processing.log"  # Fallback to old log

# Count enhanced EPUBs
epub_files = list(ENHANCED_DIR.glob("*_enhanced.epub"))
print(f"Enhanced EPUBs created: {len(epub_files)}/50")

# Show most recent 3
if epub_files:
    print(f"\nMost recent:")
    for epub in sorted(epub_files, key=lambda p: p.stat().st_mtime, reverse=True)[:3]:
        mtime = epub.stat().st_mtime
        import datetime
        dt = datetime.datetime.fromtimestamp(mtime)
        print(f"  [{dt.strftime('%H:%M:%S')}] {epub.stem[:60]}")

# Show latest log lines
log_to_read = LOG_FILE if LOG_FILE.exists() else FALLBACK_LOG
if log_to_read.exists():
    print(f"\nLatest log (last 25 lines):")
    print("=" * 70)
    with open(log_to_read, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
        for line in lines[-25:]:
            print(line.rstrip())
else:
    print(f"\nNo log file found")
