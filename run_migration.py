#!/usr/bin/env python3
"""
Run a SQL migration file against Supabase.

Usage:
    python run_migration.py migrations/011_sync_audit.sql
"""

import sys
import os
from dotenv import load_dotenv

load_dotenv()

def run_migration(sql_file: str):
    """Execute SQL migration file using Supabase client"""
    from lib.supabase_client import supabase
    
    # Read the SQL file
    with open(sql_file, 'r') as f:
        sql = f.read()
    
    print(f"Running migration: {sql_file}")
    print("-" * 60)
    
    # Split by semicolons and filter comments/empty
    statements = []
    current = []
    for line in sql.split('\n'):
        line_stripped = line.strip()
        if line_stripped.startswith('--') or not line_stripped:
            continue
        current.append(line)
        if ';' in line:
            stmt = '\n'.join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
    
    # Execute each statement
    success_count = 0
    for i, stmt in enumerate(statements, 1):
        # Skip DROP statements by default
        if 'DROP' in stmt.upper() and 'CREATE' not in stmt.upper():
            print(f"[{i}] Skipping DROP statement")
            continue
            
        try:
            # Use rpc for raw SQL - but Supabase doesn't support this directly
            # We need to use the REST API or run manually
            print(f"[{i}] Executing: {stmt[:80]}...")
            
            # Try to create the table through rpc
            result = supabase.rpc('execute_sql', {'sql_query': stmt}).execute()
            print(f"    ✅ Success")
            success_count += 1
        except Exception as e:
            error_str = str(e)
            if 'already exists' in error_str.lower():
                print(f"    ⚠️ Already exists, skipping")
                success_count += 1
            else:
                print(f"    ❌ Error: {e}")
    
    print("-" * 60)
    print(f"Completed: {success_count}/{len(statements)} statements")
    print()
    print("NOTE: If this failed, run the SQL manually in Supabase SQL Editor:")
    print("  1. Go to https://supabase.com/dashboard")
    print("  2. Open SQL Editor")
    print(f"  3. Paste contents of: {sql_file}")
    print("  4. Click Run")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_migration.py <migration_file.sql>")
        print("Example: python run_migration.py migrations/011_sync_audit.sql")
        sys.exit(1)
    
    run_migration(sys.argv[1])
