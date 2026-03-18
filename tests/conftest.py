"""Shared test fixtures for jarvis-sync-service tests."""

import os
import pytest

# Ensure env vars are set so module-level imports in sync_base don't fail
os.environ.setdefault('NOTION_API_TOKEN', 'test-token')
os.environ.setdefault('SUPABASE_URL', 'https://test.supabase.co')
os.environ.setdefault('SUPABASE_KEY', 'test-key')

# Exclude manual integration scripts from pytest collection.
# These files are CLI scripts (with __main__ blocks) that call real external
# services and are not designed to run as automated pytest tests.
collect_ignore = [
    "test_all_syncs.py",
    "test_calendar_api.py",
    "test_evening_journal.py",
    "test_robin_briefing.py",
    "test_sync_stability.py",
    "test_sync_log.py",
]
