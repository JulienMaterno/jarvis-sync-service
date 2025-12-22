"""
Bidirectional Notion ↔ Supabase Journal Sync Service

Syncs daily journal entries between Notion and Supabase:
- Notion → Supabase: Journal entries created/updated in Notion
- Supabase → Notion: Journals created from voice pipeline (adds structured content)

Journal Database Properties (Notion):
- Name: title (auto "Journal Entry")
- Date: date (the journal date)
- Day: formula (day of week)
- Mood: select (Great, Good, Okay, Tired, etc.)
- Effort: select (High, Medium, Low)
- Wakeup: select (time ranges)
- Sport: multi_select (Running, Gym, Yoga, etc.)
- Nutrition: select (Good, Okay, Poor)
- Note: rich_text (quick note)

Usage:
    python sync_journals_bidirectional.py --full    # Full sync
    python sync_journals_bidirectional.py           # Incremental (last 24h)
"""

import os
import logging
import argparse
import time
from datetime import datetime, timezone, timedelta, date
from typing import Dict, Any, List, Optional, Tuple
from dotenv import load_dotenv
import httpx
from lib.utils import retry_on_error_sync

load_dotenv()

# Import logging service
try:
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from lib.logging_service import log_sync_event_sync
    HAS_LOGGING_SERVICE = True
except ImportError:
    HAS_LOGGING_SERVICE = False
    def log_sync_event_sync(event_type, status, message, **kwargs):
        pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger('JournalSync')

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')
NOTION_JOURNAL_DB_ID = os.environ.get('NOTION_JOURNAL_DB_ID', '2cecd3f1-eb28-8098-bf5e-d49ae4a68f6b')

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')


# ============================================================================
# NOTION CLIENT
# ============================================================================

class NotionClient:
    """Notion API client for journals."""
    
    def __init__(self, token: str):
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Notion-Version': '2022-06-28',
            'Content-Type': 'application/json'
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)
    
    @retry_on_error_sync()
    def query_database(
        self, 
        database_id: str, 
        filter: Optional[Dict] = None,
        sorts: Optional[List[Dict]] = None,
        page_size: int = 100,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """Query all pages from a database with pagination."""
        results = []
        start_cursor = None
        
        while True:
            body = {"page_size": min(page_size, limit) if limit else page_size}
            if filter:
                body["filter"] = filter
            if sorts:
                body["sorts"] = sorts
            if start_cursor:
                body["start_cursor"] = start_cursor
            
            response = self.client.post(
                f'https://api.notion.com/v1/databases/{database_id}/query',
                json=body
            )
            response.raise_for_status()
            data = response.json()
            
            results.extend(data.get('results', []))
            
            if limit and len(results) >= limit:
                return results[:limit]
            
            if not data.get('has_more'):
                break
            start_cursor = data.get('next_cursor')
        
        return results
    
    @retry_on_error_sync()
    def get_page(self, page_id: str) -> Dict:
        """Get a single page by ID."""
        response = self.client.get(f'https://api.notion.com/v1/pages/{page_id}')
        response.raise_for_status()
        return response.json()
    
    @retry_on_error_sync()
    def get_page_content(self, page_id: str) -> str:
        """Get page content as text."""
        response = self.client.get(f'https://api.notion.com/v1/blocks/{page_id}/children')
        response.raise_for_status()
        blocks = response.json().get('results', [])
        
        content_parts = []
        for block in blocks:
            block_type = block.get('type')
            if block_type in ('paragraph', 'heading_1', 'heading_2', 'heading_3', 'bulleted_list_item', 'numbered_list_item'):
                rich_text = block.get(block_type, {}).get('rich_text', [])
                text = ''.join([t.get('plain_text', '') for t in rich_text])
                if text:
                    content_parts.append(text)
        
        return '\n\n'.join(content_parts)
    
    @retry_on_error_sync()
    def create_page(self, database_id: str, properties: Dict, blocks: List[Dict] = None) -> Dict:
        """Create a new page in a database."""
        body = {
            "parent": {"database_id": database_id},
            "properties": properties
        }
        if blocks:
            body["children"] = blocks
        
        response = self.client.post('https://api.notion.com/v1/pages', json=body)
        response.raise_for_status()
        return response.json()
    
    @retry_on_error_sync()
    def update_page(self, page_id: str, properties: Dict) -> Dict:
        """Update page properties."""
        response = self.client.patch(
            f'https://api.notion.com/v1/pages/{page_id}',
            json={"properties": properties}
        )
        response.raise_for_status()
        return response.json()
    
    @retry_on_error_sync()
    def append_blocks(self, page_id: str, blocks: List[Dict]) -> Dict:
        """Append blocks to a page."""
        response = self.client.patch(
            f'https://api.notion.com/v1/blocks/{page_id}/children',
            json={"children": blocks}
        )
        response.raise_for_status()
        return response.json()
    
    @retry_on_error_sync()
    def clear_page_content(self, page_id: str) -> None:
        """Remove all blocks from a page."""
        response = self.client.get(f'https://api.notion.com/v1/blocks/{page_id}/children')
        response.raise_for_status()
        blocks = response.json().get('results', [])
        
        for block in blocks:
            try:
                self.client.delete(f'https://api.notion.com/v1/blocks/{block["id"]}')
            except:
                pass


# ============================================================================
# SUPABASE CLIENT
# ============================================================================

class SupabaseClient:
    """Supabase client for journals."""
    
    def __init__(self, url: str, key: str):
        self.base_url = f"{url}/rest/v1"
        self.headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)
    
    def get_all_journals(self, since: Optional[datetime] = None) -> List[Dict]:
        """Get all journals from Supabase."""
        url = f"{self.base_url}/journals?select=*&order=date.desc"
        if since:
            # Filter by date OR updated_at using proper PostgREST OR syntax
            since_date = since.strftime('%Y-%m-%d')
            since_iso = since.strftime('%Y-%m-%dT%H:%M:%S')
            # PostgREST OR syntax: or=(condition1,condition2)
            url += f"&or=(date.gte.{since_date},updated_at.gte.{since_iso})"
        response = self.client.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_journal_by_date(self, journal_date: str) -> Optional[Dict]:
        """Find a journal by its date."""
        url = f"{self.base_url}/journals?select=*&date=eq.{journal_date}&limit=1"
        response = self.client.get(url)
        response.raise_for_status()
        data = response.json()
        return data[0] if data else None
    
    def get_journal_by_notion_id(self, notion_page_id: str) -> Optional[Dict]:
        """Find a journal by its Notion page ID."""
        url = f"{self.base_url}/journals?select=*&notion_page_id=eq.{notion_page_id}&limit=1"
        response = self.client.get(url)
        response.raise_for_status()
        data = response.json()
        return data[0] if data else None
    
    def create_journal(self, data: Dict) -> Dict:
        """Create a new journal entry."""
        response = self.client.post(f"{self.base_url}/journals", json=data)
        response.raise_for_status()
        return response.json()[0]
    
    def update_journal(self, journal_id: str, updates: Dict) -> Dict:
        """Update an existing journal."""
        response = self.client.patch(
            f"{self.base_url}/journals?id=eq.{journal_id}",
            json=updates
        )
        response.raise_for_status()
        return response.json()[0] if response.json() else {}
    
    def upsert_journal(self, data: Dict) -> Dict:
        """Upsert a journal entry (insert or update by date)."""
        headers = {**self.headers, 'Prefer': 'resolution=merge-duplicates,return=representation'}
        response = self.client.post(
            f"{self.base_url}/journals",
            json=data,
            headers=headers
        )
        response.raise_for_status()
        return response.json()[0] if response.json() else {}


# ============================================================================
# CONVERSION FUNCTIONS
# ============================================================================

def notion_journal_to_supabase(notion_journal: Dict, notion: NotionClient) -> Dict:
    """Convert Notion journal properties to Supabase format."""
    props = notion_journal.get('properties', {})
    page_id = notion_journal.get('id')
    
    # Extract title (safely handle empty list)
    title_prop = props.get('Name', {}).get('title', [])
    title = title_prop[0].get('plain_text', 'Journal Entry') if title_prop and len(title_prop) > 0 else 'Journal Entry'
    
    # Extract date (required)
    date_prop = props.get('Date', {}).get('date')
    journal_date = date_prop.get('start') if date_prop else None
    
    if not journal_date:
        logger.warning(f"Journal {page_id} has no date, skipping")
        return None
    
    # Extract select fields
    def get_select(prop_name: str) -> Optional[str]:
        select = props.get(prop_name, {}).get('select')
        return select.get('name') if select else None
    
    mood = get_select('Mood')
    effort = get_select('Effort')
    wakeup = get_select('Wakeup')
    nutrition = get_select('Nutrition')
    
    # Extract sports (multi_select)
    sports_prop = props.get('Sport', {}).get('multi_select', [])
    sports = [s.get('name') for s in sports_prop] if sports_prop else []
    
    # Extract note (rich_text)
    note_prop = props.get('Note', {}).get('rich_text', [])
    note = ''.join([t.get('plain_text', '') for t in note_prop]) if note_prop else None
    
    # Get content from page blocks
    try:
        content = notion.get_page_content(page_id)
    except:
        content = ''
    
    return {
        'date': journal_date,
        'title': title,
        'mood': mood,
        'effort': effort,
        'wakeup_time': wakeup,
        'nutrition': nutrition,
        'sports': sports if sports else None,
        'note': note,
        'content': content if content else None,
        'source': 'notion',
    }


def supabase_journal_to_notion_properties(journal: Dict) -> Dict:
    """Convert Supabase journal to Notion properties format."""
    properties = {}
    
    # Title
    title = journal.get('title', 'Journal Entry')
    properties['Name'] = {
        'title': [{'text': {'content': title[:100]}}]
    }
    
    # Date (required)
    if journal.get('date'):
        properties['Date'] = {
            'date': {'start': journal['date']}
        }
    
    # Mood (select)
    if journal.get('mood'):
        properties['Mood'] = {
            'select': {'name': journal['mood']}
        }
    
    # Effort (select)
    if journal.get('effort'):
        properties['Effort'] = {
            'select': {'name': journal['effort']}
        }
    
    # Wakeup (select)
    if journal.get('wakeup_time'):
        properties['Wakeup'] = {
            'select': {'name': journal['wakeup_time']}
        }
    
    # Nutrition (select)
    if journal.get('nutrition'):
        properties['Nutrition'] = {
            'select': {'name': journal['nutrition']}
        }
    
    # Sports (multi_select)
    if journal.get('sports'):
        properties['Sport'] = {
            'multi_select': [{'name': s} for s in journal['sports'][:10]]
        }
    
    # Note (rich_text) - use summary if available
    note = journal.get('summary') or journal.get('note') or ''
    if note:
        properties['Note'] = {
            'rich_text': [{'text': {'content': note[:2000]}}]
        }
    
    return properties


def supabase_journal_to_notion_blocks(journal: Dict) -> List[Dict]:
    """Convert Supabase journal to Notion content blocks."""
    blocks = []
    
    sections = journal.get('sections', [])
    
    if sections:
        for section in sections:
            heading = section.get('heading', '')
            content = section.get('content', '')
            
            if heading:
                blocks.append({
                    'type': 'heading_2',
                    'heading_2': {
                        'rich_text': [{'text': {'content': heading[:100]}}]
                    }
                })
            
            if content:
                # Split into chunks (Notion 2000 char limit)
                for i in range(0, len(content), 1900):
                    chunk = content[i:i+1900]
                    blocks.append({
                        'type': 'paragraph',
                        'paragraph': {
                            'rich_text': [{'text': {'content': chunk}}]
                        }
                    })
    
    # Add key events if present
    if journal.get('key_events'):
        blocks.append({
            'type': 'heading_2',
            'heading_2': {
                'rich_text': [{'text': {'content': '📌 Key Events'}}]
            }
        })
        for event in journal['key_events']:
            blocks.append({
                'type': 'bulleted_list_item',
                'bulleted_list_item': {
                    'rich_text': [{'text': {'content': event[:2000]}}]
                }
            })
    
    # Add accomplishments
    if journal.get('accomplishments'):
        blocks.append({
            'type': 'heading_2',
            'heading_2': {
                'rich_text': [{'text': {'content': '✅ Accomplishments'}}]
            }
        })
        for item in journal['accomplishments']:
            blocks.append({
                'type': 'bulleted_list_item',
                'bulleted_list_item': {
                    'rich_text': [{'text': {'content': item[:2000]}}]
                }
            })
    
    # Add challenges
    if journal.get('challenges'):
        blocks.append({
            'type': 'heading_2',
            'heading_2': {
                'rich_text': [{'text': {'content': '⚠️ Challenges'}}]
            }
        })
        for item in journal['challenges']:
            blocks.append({
                'type': 'bulleted_list_item',
                'bulleted_list_item': {
                    'rich_text': [{'text': {'content': item[:2000]}}]
                }
            })
    
    # Add gratitude
    if journal.get('gratitude'):
        blocks.append({
            'type': 'heading_2',
            'heading_2': {
                'rich_text': [{'text': {'content': '🙏 Gratitude'}}]
            }
        })
        for item in journal['gratitude']:
            blocks.append({
                'type': 'bulleted_list_item',
                'bulleted_list_item': {
                    'rich_text': [{'text': {'content': item[:2000]}}]
                }
            })
    
    # Add tomorrow's focus
    if journal.get('tomorrow_focus'):
        blocks.append({
            'type': 'heading_2',
            'heading_2': {
                'rich_text': [{'text': {'content': '🎯 Tomorrow\'s Focus'}}]
            }
        })
        for item in journal['tomorrow_focus']:
            blocks.append({
                'type': 'bulleted_list_item',
                'bulleted_list_item': {
                    'rich_text': [{'text': {'content': item[:2000]}}]
                }
            })
    
    # Fallback: raw content if no structured data
    if not blocks and journal.get('content'):
        content = journal['content']
        for i in range(0, len(content), 1900):
            chunk = content[i:i+1900]
            blocks.append({
                'type': 'paragraph',
                'paragraph': {
                    'rich_text': [{'text': {'content': chunk}}]
                }
            })
    
    return blocks


# ============================================================================
# SYNC FUNCTIONS
# ============================================================================

def sync_notion_to_supabase(
    notion: NotionClient, 
    supabase: SupabaseClient,
    full_sync: bool = False,
    since_hours: int = 24
) -> Tuple[int, int, int]:
    """Sync journals from Notion → Supabase."""
    created = 0
    updated = 0
    skipped = 0
    errors = 0
    
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours) if not full_sync else None
    
    # Query Notion journals
    filter_obj = None
    if not full_sync and since:
        filter_obj = {
            "timestamp": "last_edited_time",
            "last_edited_time": {"after": since.isoformat()}
        }
    
    journals = notion.query_database(
        NOTION_JOURNAL_DB_ID,
        filter=filter_obj,
        sorts=[{"property": "Date", "direction": "descending"}]
    )
    
    logger.info(f"Found {len(journals)} journals in Notion to process")
    
    # Get existing Supabase journals for safety valve
    existing_supabase = supabase.get_all_journals()
    logger.info(f"Supabase has {len(existing_supabase)} total journals")
    
    # SAFETY VALVE: If Notion returns empty/few but Supabase has many, abort
    if full_sync and len(existing_supabase) > 10 and len(journals) < (len(existing_supabase) * 0.1):
        msg = f"Safety Valve: Notion returned {len(journals)} journals, but Supabase has {len(existing_supabase)}. Aborting to prevent data loss."
        logger.error(msg)
        raise Exception(msg)
    
    for notion_journal in journals:
        notion_page_id = notion_journal['id']
        last_edited = notion_journal.get('last_edited_time', '')
        
        try:
            # Convert to Supabase format
            journal_data = notion_journal_to_supabase(notion_journal, notion)
            
            if not journal_data:
                skipped += 1
                continue
            
            journal_date = journal_data['date']
            
            # Check if journal exists in Supabase (by date - one per day)
            existing = supabase.get_journal_by_date(journal_date)
            
            if existing:
                # Check if Notion is newer
                supabase_updated = existing.get('notion_updated_at', '')
                last_sync_source = existing.get('last_sync_source', '')
                
                # Parse timestamps for buffer comparison
                try:
                    notion_dt = datetime.fromisoformat(last_edited.replace('Z', '+00:00'))
                    existing_dt = datetime.fromisoformat(supabase_updated.replace('Z', '+00:00')) if supabase_updated else None
                except:
                    notion_dt = None
                    existing_dt = None
                
                # Don't overwrite if Supabase was the last source (AI processed)
                if last_sync_source == 'supabase':
                    # Use 5-second buffer to avoid ping-pong
                    if notion_dt and existing_dt and notion_dt <= existing_dt + timedelta(seconds=5):
                        # But still update tracking fields
                        journal_data['notion_page_id'] = notion_page_id
                        journal_data['notion_updated_at'] = last_edited
                        # Don't change last_sync_source
                        del journal_data['source']
                        
                        supabase.update_journal(existing['id'], journal_data)
                        skipped += 1
                        logger.debug(f"Skipped (Supabase is source): {journal_date}")
                        continue
                
                # Standard timestamp comparison with 5-second buffer
                if supabase_updated and notion_dt and existing_dt:
                    if last_sync_source == 'notion':
                        if notion_dt <= existing_dt + timedelta(seconds=5):
                            skipped += 1
                            continue
                    else:
                        if last_edited <= supabase_updated:
                            skipped += 1
                            continue
                elif supabase_updated and last_edited <= supabase_updated:
                    skipped += 1
                    continue
                
                # Content equality check - avoid unnecessary updates
                fields_to_check = ['mood', 'effort', 'wakeup_time', 'nutrition', 'note']
                needs_update = False
                for field in fields_to_check:
                    new_val = journal_data.get(field)
                    existing_val = existing.get(field)
                    if (new_val is None and existing_val == "") or (new_val == "" and existing_val is None):
                        continue
                    if new_val != existing_val:
                        needs_update = True
                        logger.debug(f"Field '{field}' changed")
                        break
                
                # Also check sports (list comparison)
                if not needs_update:
                    new_sports = set(journal_data.get('sports', []) or [])
                    existing_sports = set(existing.get('sports', []) or [])
                    if new_sports != existing_sports:
                        needs_update = True
                
                if needs_update:
                    # Update Supabase
                    journal_data['notion_page_id'] = notion_page_id
                    journal_data['notion_updated_at'] = last_edited
                    journal_data['last_sync_source'] = 'notion'
                    
                    supabase.update_journal(existing['id'], journal_data)
                    updated += 1
                    logger.info(f"Updated Supabase journal: {journal_date}")
                else:
                    skipped += 1
                    logger.debug(f"Skipped (content unchanged): {journal_date}")
            else:
                # Create in Supabase
                journal_data['notion_page_id'] = notion_page_id
                journal_data['notion_updated_at'] = last_edited
                journal_data['last_sync_source'] = 'notion'
                
                supabase.create_journal(journal_data)
                created += 1
                logger.info(f"Created Supabase journal: {journal_date}")
                log_sync_event_sync("create_supabase_journal", "success", f"Created journal for {journal_date}")
                
        except Exception as e:
            logger.error(f"Error syncing Notion journal {notion_page_id}: {e}")
    
    logger.info(f"Notion → Supabase: {created} created, {updated} updated, {skipped} skipped")
    return created, updated, skipped


def sync_supabase_to_notion(
    notion: NotionClient, 
    supabase: SupabaseClient,
    full_sync: bool = False,
    since_hours: int = 24
) -> Tuple[int, int, int]:
    """Sync journals from Supabase → Notion (AI-processed content back to Notion)."""
    created = 0
    updated = 0
    skipped = 0
    
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours) if not full_sync else None
    
    # Get journals from Supabase
    journals = supabase.get_all_journals(since)
    
    # Sync journals that:
    # 1. Have no notion_page_id yet (NEW entries that need to be created in Notion)
    # 2. OR have AI content (sections, summary, key_events) to push back
    # 3. AND were NOT created by Notion (avoid ping-pong)
    journals_to_sync = [
        j for j in journals 
        if (
            # New entries without Notion link - ALWAYS sync to create the page
            not j.get('notion_page_id')
            # OR entries with AI-processed content to push
            or j.get('sections') or j.get('summary') or j.get('key_events')
        )
        # Don't sync if Notion was the source (handled later but pre-filter helps)
        and j.get('last_sync_source') != 'notion'
    ]
    
    logger.info(f"Found {len(journals_to_sync)} journals to sync to Notion ({len([j for j in journals_to_sync if not j.get('notion_page_id')])} new, {len([j for j in journals_to_sync if j.get('notion_page_id')])} updates)")
    
    for journal in journals_to_sync:
        journal_id = journal['id']
        journal_date = journal.get('date')
        notion_page_id = journal.get('notion_page_id')
        last_sync_source = journal.get('last_sync_source', '')
        
        try:
            # Skip if Notion was the last updater (avoid ping-pong)
            if last_sync_source == 'notion':
                skipped += 1
                continue
            
            if notion_page_id:
                # Update existing Notion page
                properties = supabase_journal_to_notion_properties(journal)
                blocks = supabase_journal_to_notion_blocks(journal)
                
                # Update properties
                notion.update_page(notion_page_id, properties)
                
                # Replace content if we have blocks
                if blocks:
                    notion.clear_page_content(notion_page_id)
                    notion.append_blocks(notion_page_id, blocks)
                
                # Update Supabase tracking
                supabase.update_journal(journal_id, {
                    'notion_updated_at': datetime.now(timezone.utc).isoformat(),
                    'last_sync_source': 'supabase'
                })
                
                updated += 1
                logger.info(f"Updated Notion journal: {journal_date}")
            else:
                # Create new Notion page
                properties = supabase_journal_to_notion_properties(journal)
                blocks = supabase_journal_to_notion_blocks(journal)
                
                new_page = notion.create_page(NOTION_JOURNAL_DB_ID, properties, blocks)
                new_notion_id = new_page['id']
                
                # Update Supabase with Notion ID
                supabase.update_journal(journal_id, {
                    'notion_page_id': new_notion_id,
                    'notion_updated_at': datetime.now(timezone.utc).isoformat(),
                    'last_sync_source': 'supabase'
                })
                
                created += 1
                logger.info(f"Created Notion journal: {journal_date}")
                log_sync_event_sync("create_notion_journal", "success", f"Created Notion journal for {journal_date}")
                
        except Exception as e:
            logger.error(f"Error syncing journal {journal_id} to Notion: {e}")
    
    logger.info(f"Supabase → Notion: {created} created, {updated} updated, {skipped} skipped")
    return created, updated, skipped


def run_bidirectional_sync(full_sync: bool = False, since_hours: int = 24):
    """Run full bidirectional sync."""
    logger.info(f"{'='*60}")
    logger.info(f"Starting Journal Bidirectional Sync")
    logger.info(f"Mode: {'Full' if full_sync else f'Incremental (last {since_hours}h)'}")
    logger.info(f"{'='*60}")
    
    start_time = time.time()
    
    # Validate config
    if not NOTION_API_TOKEN:
        logger.error("NOTION_API_TOKEN not set")
        return
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("SUPABASE_URL or SUPABASE_KEY not set")
        return
    
    notion = NotionClient(NOTION_API_TOKEN)
    supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    
    # Sync Notion → Supabase first (get manual entries)
    logger.info("\n--- Notion → Supabase ---")
    n2s_created, n2s_updated, n2s_skipped = sync_notion_to_supabase(
        notion, supabase, full_sync, since_hours
    )
    
    # Then Supabase → Notion (push AI content back)
    logger.info("\n--- Supabase → Notion ---")
    s2n_created, s2n_updated, s2n_skipped = sync_supabase_to_notion(
        notion, supabase, full_sync, since_hours
    )
    
    elapsed = time.time() - start_time
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Sync Complete in {elapsed:.1f}s")
    logger.info(f"Notion → Supabase: {n2s_created} created, {n2s_updated} updated, {n2s_skipped} skipped")
    logger.info(f"Supabase → Notion: {s2n_created} created, {s2n_updated} updated, {s2n_skipped} skipped")
    logger.info(f"{'='*60}")
    
    log_sync_event_sync(
        "journal_sync_complete", 
        "success", 
        f"Synced journals in {elapsed:.1f}s",
        details={
            'notion_to_supabase': {'created': n2s_created, 'updated': n2s_updated, 'skipped': n2s_skipped},
            'supabase_to_notion': {'created': s2n_created, 'updated': s2n_updated, 'skipped': s2n_skipped}
        }
    )


def main():
    parser = argparse.ArgumentParser(description='Bidirectional Notion ↔ Supabase Journal Sync')
    parser.add_argument('--full', action='store_true', help='Full sync (all entries)')
    parser.add_argument('--since', type=int, default=24, help='Hours to look back for incremental sync')
    args = parser.parse_args()
    
    run_bidirectional_sync(full_sync=args.full, since_hours=args.since)


if __name__ == '__main__':
    main()
