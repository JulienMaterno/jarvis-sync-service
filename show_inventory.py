#!/usr/bin/env python3
"""Display sync inventory table."""

from lib.sync_audit import get_database_inventory

inv = get_database_inventory()

print()
print("JARVIS SYNC INVENTORY - DATABASE COMPARISON")
print("=" * 65)
print(f"{'Entity':<12} | {'Supabase':>10} | {'Notion':>10} | {'Diff':>6} | Status")
print("-" * 65)

total_sb, total_n = 0, 0
for entity, counts in inv.items():
    sb = counts.get('supabase', 0)
    n = counts.get('notion', 0)
    diff = counts.get('difference', 0)
    
    if sb >= 0:
        total_sb += sb
    if n >= 0:
        total_n += n
    
    # Status indicator
    if diff is None:
        status = "❓"
    elif diff == 0:
        status = "✅"
    elif abs(diff) <= 3:
        status = "⚠️"
    else:
        status = "❌"
    
    print(f"{entity.title():<12} | {sb:>10} | {n:>10} | {diff:>6} | {status}")

total_diff = total_n - total_sb
print("-" * 65)
print(f"{'TOTAL':<12} | {total_sb:>10} | {total_n:>10} | {total_diff:>6} | {'✅' if total_diff == 0 else '⚠️'}")
print()
