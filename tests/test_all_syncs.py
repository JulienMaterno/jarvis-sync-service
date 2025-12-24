#!/usr/bin/env python3
"""
Quick sync test script - runs each sync and reports results.
Use this to verify syncs are working before/after changes.
"""

import os
import sys
import asyncio
import traceback
from datetime import datetime

# Try to load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def print_header(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

def print_result(name: str, success: bool, details: str = ""):
    status = "✅" if success else "❌"
    print(f"{status} {name}: {details}")

async def test_meetings_sync():
    """Test meetings sync"""
    from syncs.meetings_sync import run_sync
    result = run_sync(full_sync=False, since_hours=24)
    return result

async def test_tasks_sync():
    """Test tasks sync"""
    from syncs.tasks_sync import run_sync
    result = run_sync(full_sync=False, since_hours=24)
    return result

async def test_reflections_sync():
    """Test reflections sync"""
    from syncs.reflections_sync import run_sync
    result = run_sync(full_sync=False, since_hours=24)
    return result

async def test_journals_sync():
    """Test journals sync"""
    from syncs.journals_sync import run_sync
    result = run_sync(full_sync=False, since_hours=24)
    return result

async def test_calendar_sync():
    """Test calendar sync"""
    from sync_calendar import run_calendar_sync
    result = await run_calendar_sync()
    return result

async def test_gmail_sync():
    """Test gmail sync"""
    from sync_gmail import run_gmail_sync
    result = await run_gmail_sync()
    return result

async def test_contacts_sync():
    """Test contacts (Notion only, skip Google if failing)"""
    from sync_contacts_unified import ContactsSyncService
    service = ContactsSyncService()
    result = await service.run()
    return result

async def run_all_tests(skip_google: bool = True):
    """Run all sync tests"""
    print_header("SYNC SERVICE TESTS")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check environment
    notion_token = os.environ.get('NOTION_API_TOKEN')
    supabase_url = os.environ.get('SUPABASE_URL')
    supabase_key = os.environ.get('SUPABASE_KEY')
    
    if not notion_token or not supabase_url or not supabase_key:
        print("\n❌ Missing environment variables!")
        print(f"   NOTION_API_TOKEN: {'Set' if notion_token else 'MISSING'}")
        print(f"   SUPABASE_URL: {'Set' if supabase_url else 'MISSING'}")
        print(f"   SUPABASE_KEY: {'Set' if supabase_key else 'MISSING'}")
        return
    
    tests = [
        ("Meetings Sync", test_meetings_sync),
        ("Tasks Sync", test_tasks_sync),
        ("Reflections Sync", test_reflections_sync),
        ("Journals Sync", test_journals_sync),
        ("Contacts Sync (Notion)", test_contacts_sync),
    ]
    
    if not skip_google:
        tests.extend([
            ("Calendar Sync", test_calendar_sync),
            ("Gmail Sync", test_gmail_sync),
        ])
    
    results = {}
    
    for name, test_func in tests:
        print_header(name)
        try:
            result = await test_func()
            results[name] = {'success': True, 'result': result}
            print_result(name, True, str(result)[:200] if result else "OK")
        except Exception as e:
            results[name] = {'success': False, 'error': str(e)}
            print_result(name, False, str(e)[:200])
            traceback.print_exc()
    
    # Summary
    print_header("SUMMARY")
    success_count = sum(1 for r in results.values() if r.get('success'))
    total = len(results)
    print(f"Passed: {success_count}/{total}")
    
    for name, result in results.items():
        if result.get('success'):
            print(f"  ✅ {name}")
        else:
            print(f"  ❌ {name}: {result.get('error', 'Unknown error')[:100]}")
    
    return results

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Test all sync services')
    parser.add_argument('--include-google', action='store_true', help='Include Google (Calendar/Gmail) syncs')
    args = parser.parse_args()
    
    asyncio.run(run_all_tests(skip_google=not args.include_google))
