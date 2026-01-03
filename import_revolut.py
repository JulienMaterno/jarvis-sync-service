#!/usr/bin/env python3
"""
Import Revolut CSV statement into Supabase.

Usage:
    python import_revolut.py path/to/statement.csv [--dry-run]
    
The script:
1. Parses the Revolut CSV export
2. Creates a hash for each transaction (deduplication)
3. Inserts new transactions, skipping duplicates
"""

import csv
import hashlib
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()

from supabase import create_client

# Initialize Supabase
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: SUPABASE_URL and SUPABASE_KEY environment variables required")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def create_transaction_hash(row: Dict) -> str:
    """Create a unique hash for deduplication."""
    unique_string = f"{row['type']}|{row['started_date']}|{row['amount']}|{row['description']}|{row['currency']}"
    return hashlib.md5(unique_string.encode()).hexdigest()


def parse_revolut_csv(filepath: str) -> List[Dict]:
    """Parse Revolut CSV and return list of transaction dicts."""
    transactions = []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Parse dates
            started_date = None
            completed_date = None
            
            if row.get('Started Date'):
                try:
                    started_date = datetime.strptime(row['Started Date'], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    started_date = datetime.strptime(row['Started Date'], '%Y-%m-%d')
            
            if row.get('Completed Date'):
                try:
                    completed_date = datetime.strptime(row['Completed Date'], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    completed_date = datetime.strptime(row['Completed Date'], '%Y-%m-%d')
            
            # Parse amounts
            amount = float(row.get('Amount', 0) or 0)
            fee = float(row.get('Fee', 0) or 0)
            balance = float(row.get('Balance', 0) or 0) if row.get('Balance') else None
            
            transaction = {
                'type': row.get('Type', '').strip(),
                'product': row.get('Product', '').strip() or None,
                'started_date': started_date.isoformat() if started_date else None,
                'completed_date': completed_date.isoformat() if completed_date else None,
                'description': row.get('Description', '').strip() or None,
                'amount': amount,
                'fee': fee,
                'currency': row.get('Currency', '').strip(),
                'state': row.get('State', '').strip() or None,
                'balance': balance,
            }
            
            # Create hash for deduplication
            transaction['revolut_hash'] = create_transaction_hash(transaction)
            
            # Auto-categorize based on type and description
            transaction['category'] = auto_categorize(transaction)
            
            transactions.append(transaction)
    
    return transactions


def auto_categorize(tx: Dict) -> Optional[str]:
    """Auto-categorize transaction based on type and description."""
    tx_type = tx.get('type', '').lower()
    description = (tx.get('description') or '').lower()
    
    # Type-based categorization
    if tx_type == 'exchange':
        return 'Currency Exchange'
    elif tx_type == 'topup':
        return 'Income'
    elif tx_type == 'charge':
        return 'Fees'
    elif tx_type == 'transfer':
        if tx.get('amount', 0) > 0:
            return 'Transfer In'
        else:
            return 'Transfer Out'
    
    # Description-based categorization for card payments
    if tx_type == 'card payment':
        desc = description
        
        # Food & Dining
        if any(x in desc for x in ['restaurant', 'cafe', 'coffee', 'food', 'bakery', 'warung', 'grill']):
            return 'Food & Dining'
        
        # Travel & Transport
        if any(x in desc for x in ['uber', 'grab', 'taxi', 'airline', 'flight', 'train', 'booking.com', 'airbnb', 'hotel']):
            return 'Travel'
        
        # Insurance
        if any(x in desc for x in ['insurance', 'safetywing']):
            return 'Insurance'
        
        # Telecom
        if any(x in desc for x in ['telkom', 'finnet', 'mobile', 'phone']):
            return 'Telecom'
        
        # Shopping
        if any(x in desc for x in ['amazon', 'shop', 'store', 'market']):
            return 'Shopping'
        
        # Default for card payments
        return 'Uncategorized'
    
    return None


def import_transactions(transactions: List[Dict], dry_run: bool = False) -> Dict:
    """Import transactions into Supabase."""
    stats = {
        'total': len(transactions),
        'imported': 0,
        'skipped': 0,
        'errors': []
    }
    
    for tx in transactions:
        if dry_run:
            print(f"[DRY RUN] Would import: {tx['started_date'][:10]} | {tx['type']:15} | {tx['amount']:>10.2f} {tx['currency']} | {tx['description'][:40] if tx['description'] else 'N/A'}")
            stats['imported'] += 1
            continue
        
        try:
            # Try to insert (will fail on duplicate hash due to UNIQUE constraint)
            result = supabase.table('revolut_transactions').insert(tx).execute()
            
            if result.data:
                stats['imported'] += 1
                print(f"✓ Imported: {tx['started_date'][:10]} | {tx['amount']:>10.2f} {tx['currency']} | {tx['description'][:40] if tx['description'] else 'N/A'}")
            else:
                stats['skipped'] += 1
                
        except Exception as e:
            if 'duplicate' in str(e).lower() or 'unique' in str(e).lower():
                stats['skipped'] += 1
                print(f"⊘ Skipped (duplicate): {tx['started_date'][:10]} | {tx['description'][:40] if tx['description'] else 'N/A'}")
            else:
                stats['errors'].append(str(e))
                print(f"✗ Error: {e}")
    
    return stats


def main():
    if len(sys.argv) < 2:
        print("Usage: python import_revolut.py <csv_file> [--dry-run]")
        print("\nExample:")
        print("  python import_revolut.py account-statement_2025-11-01_2026-01-02_en_4d106e.csv")
        print("  python import_revolut.py statement.csv --dry-run")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    dry_run = '--dry-run' in sys.argv
    
    if not os.path.exists(csv_file):
        print(f"Error: File not found: {csv_file}")
        sys.exit(1)
    
    print(f"{'[DRY RUN] ' if dry_run else ''}Parsing {csv_file}...")
    transactions = parse_revolut_csv(csv_file)
    print(f"Found {len(transactions)} transactions\n")
    
    if not transactions:
        print("No transactions found in CSV")
        sys.exit(0)
    
    print(f"{'[DRY RUN] ' if dry_run else ''}Importing to Supabase...")
    print("-" * 80)
    
    stats = import_transactions(transactions, dry_run=dry_run)
    
    print("-" * 80)
    print(f"\nSummary:")
    print(f"  Total in CSV:  {stats['total']}")
    print(f"  Imported:      {stats['imported']}")
    print(f"  Skipped:       {stats['skipped']}")
    print(f"  Errors:        {len(stats['errors'])}")
    
    if stats['errors']:
        print(f"\nErrors:")
        for err in stats['errors'][:5]:
            print(f"  - {err}")


if __name__ == '__main__':
    main()
