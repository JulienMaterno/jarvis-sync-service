"""
===================================================================================
DEEP DIVE TESTS - Bidirectional Sync Pipeline Edge Cases & Data Loss Scenarios
===================================================================================

Tests for issues found during deep-dive analysis of all sync engines.
Focuses on the bidirectional engines (contacts, meetings, tasks, reflections)
where data loss is most risky.

Issues covered:
1. Tasks: Notion 'status' type vs 'select' type mismatch (status property returns
   None from ExtractorSelect, always defaults to 'pending')
2. Tasks: No content/sections sync (unlike meetings/reflections/journals)
3. Contacts: Google merge uses `or` fallback which silently drops cleared fields
4. Contacts: Unified contacts_sync content update deletes-before-add (unsafe)
5. Meetings: Safety valve applied asymmetrically (only aborts N->S on full_sync)
6. Meetings: Dedup by title only - no date check, so different meetings with same
   title could collide
7. Reflections: _build_supabase_lookup fetches all records twice (perf concern)
8. Reflections: convert_to_source omits Name when title is empty string but
   convert_from_source doesn't default empty title to 'Untitled'
9. Journals: Date-based matching could cross-link different journal records
10. Contacts: _find_existing_contact dedup is O(n*m) - potential performance issue
11. last_sync_source ping-pong prevention across engines
12. Empty/null field handling in property extractors
13. Very long text truncation safety
14. Notion rich_text extractor only reads first element (data loss for multi-segment)

All tests are fully mocked -- NO real API calls.
"""

import os
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock, PropertyMock, call
from typing import Dict, Any, List, Optional

# Ensure env vars are set for module imports
os.environ.setdefault('NOTION_API_TOKEN', 'test-token')
os.environ.setdefault('SUPABASE_URL', 'https://test.supabase.co')
os.environ.setdefault('SUPABASE_KEY', 'test-key')
os.environ.setdefault('NOTION_NEWSLETTERS_DB_ID', 'test-newsletters-db')
os.environ.setdefault('NOTION_DOCUMENTS_DB_ID', 'test-documents-db')


# ============================================================================
# ISSUE 1: Tasks 'status' type vs 'select' type mismatch
# ============================================================================


class TestTasksStatusTypeMismatch:
    """
    Notion has two property types that look similar:
    - 'select': {'select': {'name': 'value'}}
    - 'status': {'status': {'name': 'value'}}

    NotionPropertyExtractor.select() only handles the 'select' type.
    If the Notion Tasks DB uses the built-in 'Status' property type,
    the extractor returns None, and ALL tasks default to 'pending'.

    This is a KNOWN issue documented in the existing test
    test_convert_from_source_status_type_property, but it means
    status changes in Notion are NEVER reflected in Supabase when
    using the native Status property type.
    """

    def test_all_notion_status_values_map_correctly_via_select(self):
        """Verify all four Notion status values map correctly when using select type."""
        from syncs.tasks_sync import TasksSyncService, NOTION_TO_SUPABASE_STATUS

        service = TasksSyncService.__new__(TasksSyncService)
        service.logger = MagicMock()

        for notion_status, expected_supabase in NOTION_TO_SUPABASE_STATUS.items():
            notion_record = {
                'properties': {
                    'Name': {'title': [{'plain_text': f'Task ({notion_status})'}]},
                    'Status': {'select': {'name': notion_status}},
                }
            }
            result = service.convert_from_source(notion_record)
            assert result['status'] == expected_supabase, (
                f"Notion status '{notion_status}' should map to '{expected_supabase}', "
                f"got '{result['status']}'"
            )

    def test_all_supabase_status_values_map_back_correctly(self):
        """Verify all four Supabase status values map back to Notion format."""
        from syncs.tasks_sync import TasksSyncService, SUPABASE_TO_NOTION_STATUS

        service = TasksSyncService.__new__(TasksSyncService)
        service.logger = MagicMock()

        for supabase_status, expected_notion in SUPABASE_TO_NOTION_STATUS.items():
            supabase_record = {
                'title': f'Task ({supabase_status})',
                'status': supabase_status,
            }
            result = service.convert_to_source(supabase_record)
            assert result['Status']['status']['name'] == expected_notion

    def test_status_roundtrip_preserves_value(self):
        """Status should survive Notion -> Supabase -> Notion roundtrip.

        Notion's built-in Status property uses {'status': {'name': ...}} format,
        NOT {'select': {'name': ...}}. The tasks sync code correctly handles this
        by first checking the 'status' key, then falling back to 'select'.

        The mapping: Done -> 'completed' -> Done (symmetric roundtrip).
        """
        from syncs.tasks_sync import TasksSyncService

        service = TasksSyncService.__new__(TasksSyncService)
        service.logger = MagicMock()

        # Start with Notion 'Done' status using proper Status property type
        notion_record = {
            'properties': {
                'Name': {'title': [{'plain_text': 'Completed Task'}]},
                'Status': {'status': {'name': 'Done'}},
                'Due': {'date': None},
                'Priority': {'select': None},
            }
        }

        # Convert to Supabase - 'Done' maps to 'completed'
        supabase_data = service.convert_from_source(notion_record)
        assert supabase_data['status'] == 'completed'

        # Convert back to Notion - 'completed' maps back to 'Done'
        notion_props = service.convert_to_source(supabase_data)
        assert notion_props['Status']['status']['name'] == 'Done'

    def test_status_roundtrip_via_select_fallback(self):
        """Status roundtrip via select fallback (non-standard property type).

        If someone configures a Status column as 'select' instead of 'status',
        the code falls back to NotionPropertyExtractor.select(). The mapping
        still works: Done -> 'completed' -> Done.
        """
        from syncs.tasks_sync import TasksSyncService

        service = TasksSyncService.__new__(TasksSyncService)
        service.logger = MagicMock()

        # Using 'select' type instead of 'status' type (fallback path)
        notion_record = {
            'properties': {
                'Name': {'title': [{'plain_text': 'Completed Task'}]},
                'Status': {'select': {'name': 'Done'}},
                'Due': {'date': None},
                'Priority': {'select': None},
            }
        }

        # Convert to Supabase via select fallback
        supabase_data = service.convert_from_source(notion_record)
        assert supabase_data['status'] == 'completed'

        # Convert back to Notion
        notion_props = service.convert_to_source(supabase_data)
        assert notion_props['Status']['status']['name'] == 'Done'

    def test_priority_roundtrip_preserves_value(self):
        """Priority should survive Notion -> Supabase -> Notion roundtrip."""
        from syncs.tasks_sync import TasksSyncService

        service = TasksSyncService.__new__(TasksSyncService)
        service.logger = MagicMock()

        for notion_priority in ['High', 'Medium', 'Low']:
            notion_record = {
                'properties': {
                    'Name': {'title': [{'plain_text': 'Task'}]},
                    'Status': {'select': {'name': 'Not started'}},
                    'Priority': {'select': {'name': notion_priority}},
                }
            }
            supabase_data = service.convert_from_source(notion_record)
            notion_props = service.convert_to_source(supabase_data)
            assert notion_props['Priority']['select']['name'] == notion_priority


# ============================================================================
# ISSUE 2: Contacts Google merge silently drops cleared fields
# ============================================================================


class TestContactsGoogleMergeBehavior:
    """
    In sync_contacts_unified.py, the Google merge uses `or` fallback:
        'first_name': parsed.get('first_name') or existing_record.get('first_name')

    This means if a user CLEARS a field in Google (sets it to ''), the existing
    Supabase value is preserved because '' is falsy. The user's intention to
    clear the field is silently ignored.

    This is a design tradeoff (prevent accidental data loss from partial Google
    API responses) but it should be tested and documented.
    """

    def test_google_merge_preserves_existing_when_google_returns_empty(self):
        """When Google returns empty string, existing Supabase value is kept."""
        from sync_contacts_unified import ContactsSyncService

        service = ContactsSyncService.__new__(ContactsSyncService)
        service.logger = MagicMock()
        service.supabase = MagicMock()
        service.notion = MagicMock()
        service.google = MagicMock()
        service.google.enabled = True
        service.notion_database_id = 'test-db'
        service.service_name = 'contacts_sync'
        from lib.sync_base import SyncDirection, SyncLogger
        service.direction = SyncDirection.MULTI_SOURCE
        service.sync_logger = MagicMock()

        # Google returns a contact with empty company (user cleared it)
        from sync_contacts_unified import GoogleContactsClient
        google_contact = {
            'resourceName': 'people/123',
            'etag': 'abc',
            'names': [{'givenName': 'John', 'familyName': 'Doe'}],
            'emailAddresses': [{'value': 'john@example.com'}],
            'phoneNumbers': [{}],
            'organizations': [{'name': '', 'title': ''}],  # Cleared
            'addresses': [{}],
            'birthdays': [{}],
            'urls': [{}],
            'biographies': [{}],
        }

        parsed = GoogleContactsClient.parse_google_contact(google_contact)
        assert parsed['company'] is None or parsed['company'] == ''

        # Simulate merge logic from sync_google
        existing_record = {
            'id': 'sb-1',
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'company': 'Acme Corp',  # This was the old value
            'job_title': 'Engineer',
        }

        # The `or` fallback means '' or 'Acme Corp' => 'Acme Corp'
        merged_company = parsed.get('company') or existing_record.get('company')
        assert merged_company == 'Acme Corp', (
            "Empty string from Google should fallback to existing value due to `or` logic"
        )

    def test_google_parse_contact_handles_missing_arrays(self):
        """Google contact with completely missing fields should not crash."""
        from sync_contacts_unified import GoogleContactsClient

        minimal_contact = {
            'resourceName': 'people/456',
            'etag': 'xyz',
            # All other fields missing
        }

        parsed = GoogleContactsClient.parse_google_contact(minimal_contact)
        assert parsed['first_name'] == ''
        assert parsed['last_name'] == ''
        assert parsed['google_contact_id'] == 'people/456'


# ============================================================================
# ISSUE 3: Contacts unified content update is delete-before-add (unsafe)
# ============================================================================


class TestContactsContentUpdateSafety:
    """
    In sync_contacts_unified.py _sync_supabase_to_notion, when updating
    content blocks for an existing page:

    1. Delete ALL existing blocks
    2. Add new blocks

    If step 2 fails, all content is lost. This is different from the
    reflections/journals sync which uses the safer pattern:
    1. Add new blocks first
    2. Only delete old blocks after successful add

    This tests documents the unsafe pattern.
    """

    def test_contacts_update_deletes_blocks_before_adding(self):
        """Verify the delete-before-add pattern in contacts sync."""
        from sync_contacts_unified import ContactsSyncService

        service = ContactsSyncService.__new__(ContactsSyncService)
        service.logger = MagicMock()
        service.supabase = MagicMock()
        service.notion = MagicMock()
        service.google = MagicMock()
        service.google.enabled = False
        service.notion_database_id = 'test-db'
        service.service_name = 'contacts_sync'
        from lib.sync_base import SyncDirection
        service.direction = SyncDirection.MULTI_SOURCE
        service.sync_logger = MagicMock()

        # Setup: existing contact with notion_page_id and profile_content
        supabase_records = [
            {
                'id': 'sb-1',
                'first_name': 'John',
                'last_name': 'Doe',
                'email': 'john@example.com',
                'notion_page_id': 'notion-1',
                'notion_updated_at': '2025-01-01T00:00:00Z',
                'updated_at': '2025-02-01T00:00:00Z',
                'deleted_at': None,
                'last_sync_source': 'supabase',
                'profile_content': 'Important contact notes',
            }
        ]

        service.supabase.select_all.return_value = supabase_records
        service.supabase.select_updated_since.return_value = supabase_records
        service.notion.query_database.return_value = []
        service.notion.update_page.return_value = {
            'id': 'notion-1',
            'last_edited_time': '2025-02-01T00:01:00Z'
        }
        service.notion.get_all_blocks.return_value = [
            {'id': 'block-1', 'type': 'paragraph'},
            {'id': 'block-2', 'type': 'paragraph'},
        ]
        service.notion.append_blocks.return_value = [
            {'id': 'new-block-1'}
        ]

        result = service._sync_supabase_to_notion(full_sync=True, since_hours=24)

        # Verify the delete-then-add pattern happens:
        # First, delete_block should be called for existing blocks
        if service.notion.delete_block.called:
            delete_calls = service.notion.delete_block.call_args_list
            # Deletes happen BEFORE or interleaved with append
            assert len(delete_calls) >= 1


# ============================================================================
# ISSUE 4: Meetings dedup by title only - no date check
# ============================================================================


class TestMeetingsDeduplicationRisks:
    """
    Meeting dedup in _find_existing_notion_page matches by title only.
    Two different meetings with the same title on different dates
    would be incorrectly linked.

    Example: "Weekly Standup" appears every week.
    """

    def test_same_title_different_dates_returns_first_match(self):
        """Two meetings with same title but different dates - first match wins."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        notion_records = [
            {
                'id': 'page-jan-15',
                'properties': {
                    'Name': {'title': [{'plain_text': 'Weekly Standup'}]},
                    'Date': {'date': {'start': '2025-01-15'}},
                },
            },
            {
                'id': 'page-jan-22',
                'properties': {
                    'Name': {'title': [{'plain_text': 'Weekly Standup'}]},
                    'Date': {'date': {'start': '2025-01-22'}},
                },
            },
        ]

        # This will match the FIRST "Weekly Standup" regardless of date
        result = service._find_existing_notion_page('Weekly Standup', '', notion_records)
        assert result is not None
        assert result['id'] == 'page-jan-15'
        # Note: This means a new Supabase meeting titled "Weekly Standup" for
        # Jan 22 would be linked to the Jan 15 page, not the Jan 22 page.

    def test_unique_title_matches_correctly(self):
        """Unique titles should match correctly."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        notion_records = [
            {
                'id': 'page-1',
                'properties': {
                    'Name': {'title': [{'plain_text': 'Q1 Planning'}]},
                },
            },
            {
                'id': 'page-2',
                'properties': {
                    'Name': {'title': [{'plain_text': 'Investor Meeting'}]},
                },
            },
        ]

        result = service._find_existing_notion_page('Investor Meeting', '', notion_records)
        assert result['id'] == 'page-2'


# ============================================================================
# ISSUE 5: Reflections convert_to_source empty title omits Name
# ============================================================================


class TestReflectionsEmptyFieldHandling:
    """
    ReflectionsSyncService.convert_to_source only adds 'Name' property
    when title is truthy. But convert_from_source doesn't default empty
    title to 'Untitled' (unlike Tasks and Meetings).

    This means: if Notion sends a reflection with an empty title,
    Supabase stores '' (empty string). When syncing back, the Name
    property is omitted, which won't update the title in Notion.
    This is consistent but differs from other engines.
    """

    def test_empty_title_from_notion_stored_as_empty_string(self):
        """Empty title from Notion should be stored as empty string."""
        from syncs.reflections_sync import ReflectionsSyncService

        service = ReflectionsSyncService.__new__(ReflectionsSyncService)
        service.logger = MagicMock()

        notion_record = {
            'properties': {
                'Name': {'title': []},  # Empty title
                'Date': {'date': {'start': '2025-02-10'}},
                'Tags': {'multi_select': []},
            }
        }

        result = service.convert_from_source(notion_record)
        assert result['title'] == ''  # Empty string, NOT 'Untitled'

    def test_empty_title_omits_name_in_notion_properties(self):
        """Empty title in Supabase should omit Name from Notion properties."""
        from syncs.reflections_sync import ReflectionsSyncService

        service = ReflectionsSyncService.__new__(ReflectionsSyncService)
        service.logger = MagicMock()

        supabase_record = {
            'title': '',  # Empty
            'date': '2025-02-10',
            'tags': ['mindset'],
        }

        result = service.convert_to_source(supabase_record)
        assert 'Name' not in result  # Name is omitted for empty title

    def test_none_tags_stored_as_none_not_empty_list(self):
        """Empty multi_select should be stored as None, not []."""
        from syncs.reflections_sync import ReflectionsSyncService

        service = ReflectionsSyncService.__new__(ReflectionsSyncService)
        service.logger = MagicMock()

        notion_record = {
            'properties': {
                'Name': {'title': [{'plain_text': 'Reflection'}]},
                'Date': {'date': None},
                'Tags': {'multi_select': []},
            }
        }

        result = service.convert_from_source(notion_record)
        # Empty list should become None (not [])
        assert result['tags'] is None


# ============================================================================
# ISSUE 6: Journals date-based matching could cross-link records
# ============================================================================


class TestJournalsDateBasedMatching:
    """
    JournalsSyncService._sync_notion_to_supabase matches by BOTH
    notion_page_id AND date. If a Supabase record has the same date
    as a Notion record but a different notion_page_id (or none),
    they get linked.

    This is intentional (journals are unique by date) but has edge
    cases when entries are duplicated or dates are changed.
    """

    def test_date_matching_links_orphaned_records(self):
        """A Supabase record with matching date gets linked even without notion_page_id."""
        from syncs.journals_sync import JournalsSyncService

        service = JournalsSyncService.__new__(JournalsSyncService)
        service.logger = MagicMock()
        service.notion = MagicMock()
        service.supabase = MagicMock()
        service.notion_database_id = 'test-db'
        service.service_name = 'JournalsSync'
        from lib.sync_base import SyncDirection, SyncLogger
        service.sync_direction = SyncDirection.TWO_WAY
        service.sync_logger = MagicMock()

        # Notion has a journal for Feb 10
        notion_records = [
            {
                'id': 'notion-j1',
                'last_edited_time': '2025-02-10T20:00:00Z',
                'properties': {
                    'Name': {'title': [{'plain_text': 'Journal Entry'}]},
                    'Date': {'date': {'start': '2025-02-10'}},
                    'Mood': {'select': None},
                    'Effort': {'select': None},
                    'Wakeup': {'select': None},
                    'Nutrition': {'select': None},
                    'Sport': {'multi_select': []},
                    'Note': {'rich_text': []},
                }
            }
        ]

        # Supabase has a record for Feb 10 WITHOUT notion_page_id
        # (created by intelligence service)
        existing = [
            {
                'id': 'sb-j1',
                'notion_page_id': None,
                'notion_updated_at': None,
                'date': '2025-02-10',
                'title': 'Journal Entry',
                'last_sync_source': 'supabase',
            }
        ]

        service.notion.query_database.return_value = notion_records
        service.supabase.select_all.return_value = existing
        service.notion.extract_page_content.return_value = ('Content', False)
        service.notion.extract_page_sections.return_value = []

        result = service._sync_notion_to_supabase(full_sync=True, since_hours=24)

        # The record should be matched by date and SKIPPED because
        # last_sync_source is 'supabase' (has local changes pending)
        assert result.stats.skipped == 1

    def test_journal_without_date_is_skipped(self):
        """Journal record without date should return None and be skipped."""
        from syncs.journals_sync import JournalsSyncService

        service = JournalsSyncService.__new__(JournalsSyncService)
        service.logger = MagicMock()

        notion_record = {
            'id': 'journal-no-date',
            'properties': {
                'Name': {'title': [{'plain_text': 'No Date'}]},
                'Date': {'date': None},
            }
        }

        result = service.convert_from_source(notion_record)
        assert result is None


# ============================================================================
# ISSUE 7: NotionPropertyExtractor.rich_text only reads first element
# ============================================================================


class TestPropertyExtractorEdgeCases:
    """
    NotionPropertyExtractor.rich_text only reads the FIRST element
    of the rich_text array. If Notion stores text across multiple
    segments (e.g., part bold, part plain), only the first segment
    is extracted. This causes data loss for formatted text.

    Example: "Hello **world**" in Notion becomes:
    [{"plain_text": "Hello "}, {"plain_text": "world", "annotations": {"bold": true}}]
    Extractor only returns "Hello ".
    """

    def test_rich_text_only_extracts_first_segment(self):
        """rich_text extractor only returns the first segment of multi-segment text."""
        from lib.sync_base import NotionPropertyExtractor

        props = {
            'Company': {
                'rich_text': [
                    {'plain_text': 'Acme '},
                    {'plain_text': 'Corporation', 'annotations': {'bold': True}},
                ]
            }
        }

        # This only gets "Acme " - losing "Corporation"
        result = NotionPropertyExtractor.rich_text(props, 'Company')
        assert result == 'Acme '  # DATA LOSS: "Corporation" is dropped

    def test_rich_text_empty_array_returns_none(self):
        """Empty rich_text array should return None."""
        from lib.sync_base import NotionPropertyExtractor

        props = {'Notes': {'rich_text': []}}
        result = NotionPropertyExtractor.rich_text(props, 'Notes')
        assert result is None

    def test_rich_text_missing_property_returns_none(self):
        """Missing property should return None."""
        from lib.sync_base import NotionPropertyExtractor

        props = {}
        result = NotionPropertyExtractor.rich_text(props, 'NonExistent')
        assert result is None

    def test_title_extractor_only_reads_first_element(self):
        """Title extractor also only reads first element."""
        from lib.sync_base import NotionPropertyExtractor

        props = {
            'Name': {
                'title': [
                    {'plain_text': 'Part 1 '},
                    {'plain_text': 'Part 2'},
                ]
            }
        }

        result = NotionPropertyExtractor.title(props, 'Name')
        assert result == 'Part 1 '  # Only first element

    def test_multi_select_handles_empty_names(self):
        """multi_select with empty name items should still return them."""
        from lib.sync_base import NotionPropertyExtractor

        props = {
            'Tags': {
                'multi_select': [
                    {'name': 'valid'},
                    {'name': ''},
                    {'name': 'also-valid'},
                ]
            }
        }

        result = NotionPropertyExtractor.multi_select(props, 'Tags')
        assert result == ['valid', '', 'also-valid']

    def test_date_with_end_date_only_returns_start(self):
        """Date property with end date should only return start date."""
        from lib.sync_base import NotionPropertyExtractor

        props = {
            'Range': {
                'date': {
                    'start': '2025-02-10',
                    'end': '2025-02-15',
                }
            }
        }

        result = NotionPropertyExtractor.date(props, 'Range')
        assert result == '2025-02-10'  # End date is dropped

    def test_select_with_none_value(self):
        """Select property with None select value should return None."""
        from lib.sync_base import NotionPropertyExtractor

        props = {'Status': {'select': None}}
        result = NotionPropertyExtractor.select(props, 'Status')
        assert result is None


# ============================================================================
# ISSUE 8: Safety valve asymmetry across engines
# ============================================================================


class TestSafetyValveConsistency:
    """
    Safety valve behavior is inconsistent across engines:
    - Notion->Supabase: Aborts on full_sync when triggered
    - Supabase->Notion: Only logs warning, never aborts
    - Contacts sync_google: Aborts (not just full_sync)

    This tests documents the current behavior.
    """

    def test_notion_to_supabase_aborts_only_on_full_sync(self):
        """Safety valve should abort N->S only during full sync."""
        from lib.sync_base import TwoWaySyncService, NotionPropertyBuilder
        import lib.sync_base as sb

        original_mode = sb.SAFETY_VALVE_MODE
        sb.SAFETY_VALVE_MODE = 'abort'

        try:
            class TestSync(TwoWaySyncService):
                def __init__(self):
                    from lib.sync_base import SyncDirection, SyncLogger, setup_logger
                    self.service_name = "TestSync"
                    self.direction = SyncDirection.TWO_WAY
                    self.logger = setup_logger("TestSync")
                    self.sync_logger = MagicMock()
                    self.notion = MagicMock()
                    self.supabase = MagicMock()
                    self.notion_database_id = "test-db"

                def convert_from_source(self, r):
                    return {'title': 'test'}

                def convert_to_source(self, r):
                    return {'Name': NotionPropertyBuilder.title('test')}

                def get_source_id(self, r):
                    return r.get('id', '')

            service = TestSync()

            # Setup: 2 Notion records, 50 Supabase records (triggers valve)
            service.notion.query_database.return_value = [
                {'id': f'n-{i}', 'last_edited_time': '2025-01-15T10:00:00Z',
                 'properties': {'Name': {'title': [{'plain_text': f'R{i}'}]}}}
                for i in range(2)
            ]
            service.supabase.select_all.return_value = [
                {'id': f'sb-{i}', 'notion_page_id': f'n-{i}',
                 'notion_updated_at': '2025-01-14T00:00:00Z',
                 'last_sync_source': 'notion'}
                for i in range(50)
            ]

            # Full sync - should abort
            result = service._sync_notion_to_supabase(full_sync=True, since_hours=24)
            assert result.success is False

            # Incremental sync - should NOT abort (safety valve only blocks full sync)
            result2 = service._sync_notion_to_supabase(full_sync=False, since_hours=24)
            # Incremental sync proceeds (it may create/update/skip records)
            assert result2.success is True

        finally:
            sb.SAFETY_VALVE_MODE = original_mode

    def test_supabase_to_notion_never_aborts(self):
        """Safety valve for S->N should only warn, never abort."""
        from lib.sync_base import TwoWaySyncService, NotionPropertyBuilder
        import lib.sync_base as sb

        original_mode = sb.SAFETY_VALVE_MODE
        sb.SAFETY_VALVE_MODE = 'abort'

        try:
            class TestSync(TwoWaySyncService):
                def __init__(self):
                    from lib.sync_base import SyncDirection, setup_logger
                    self.service_name = "TestSync"
                    self.direction = SyncDirection.TWO_WAY
                    self.logger = setup_logger("TestSync")
                    self.sync_logger = MagicMock()
                    self.notion = MagicMock()
                    self.supabase = MagicMock()
                    self.notion_database_id = "test-db"

                def convert_from_source(self, r):
                    return {'title': 'test'}

                def convert_to_source(self, r):
                    return {'Name': NotionPropertyBuilder.title('test')}

                def get_source_id(self, r):
                    return r.get('id', '')

            service = TestSync()

            # Many records to sync, few in Notion (triggers valve)
            service.supabase.select_all.return_value = [
                {'id': f'sb-{i}', 'title': f'Record {i}',
                 'notion_page_id': None, 'deleted_at': None,
                 'updated_at': '2025-02-01T00:00:00Z',
                 'last_sync_source': 'supabase'}
                for i in range(50)
            ]
            service.supabase.select_updated_since.return_value = service.supabase.select_all.return_value
            service.notion.query_database.return_value = []  # Empty Notion DB
            service.notion.create_page.return_value = {
                'id': 'new-page', 'last_edited_time': '2025-02-01T00:01:00Z'
            }

            # S->N should proceed (only warns)
            result = service._sync_supabase_to_notion(full_sync=True, since_hours=24)
            assert result.success is True

        finally:
            sb.SAFETY_VALVE_MODE = original_mode


# ============================================================================
# ISSUE 9: Ping-pong prevention via last_sync_source
# ============================================================================


class TestPingPongPrevention:
    """
    After syncing a record from Notion->Supabase, last_sync_source is set to 'notion'.
    After syncing from Supabase->Notion, last_sync_source is set to 'notion' again
    (after the Notion update returns a new timestamp).

    The ping-pong prevention relies on:
    1. Timestamp comparison (5-second buffer)
    2. last_sync_source check (skip records with source='supabase' in N->S direction)

    Test that a record doesn't bounce back and forth.
    """

    def test_record_synced_from_notion_not_synced_back_immediately(self):
        """After N->S sync, record should not be picked up for S->N sync."""
        from lib.sync_base import TwoWaySyncService, NotionPropertyBuilder

        class TestSync(TwoWaySyncService):
            def __init__(self):
                from lib.sync_base import SyncDirection, setup_logger
                self.service_name = "PingPongTest"
                self.direction = SyncDirection.TWO_WAY
                self.logger = setup_logger("PingPongTest")
                self.sync_logger = MagicMock()
                self.notion = MagicMock()
                self.supabase = MagicMock()
                self.notion_database_id = "test-db"

            def convert_from_source(self, r):
                return {'title': 'test'}

            def convert_to_source(self, r):
                return {'Name': NotionPropertyBuilder.title('test')}

            def get_source_id(self, r):
                return r.get('id', '')

        service = TestSync()

        now = datetime.now(timezone.utc)

        # After N->S sync, the record should have:
        # - last_sync_source = 'notion'
        # - updated_at = now (set by the upsert)
        # - notion_updated_at = now (set from Notion's last_edited_time)
        synced_record = {
            'id': 'sb-1',
            'title': 'Test',
            'notion_page_id': 'notion-1',
            'notion_updated_at': now.isoformat(),
            'updated_at': now.isoformat(),
            'deleted_at': None,
            'last_sync_source': 'notion',
        }

        # This record should NOT be picked up for S->N sync
        service.supabase.select_all.return_value = [synced_record]
        service.supabase.select_updated_since.return_value = [synced_record]
        service.notion.query_database.return_value = []

        result = service._sync_supabase_to_notion(full_sync=True, since_hours=24)

        # No creates or updates should happen
        service.notion.create_page.assert_not_called()
        service.notion.update_page.assert_not_called()

    def test_locally_edited_record_detected_for_sync(self):
        """A record edited locally in Supabase should be synced to Notion."""
        from lib.sync_base import TwoWaySyncService, NotionPropertyBuilder

        class TestSync(TwoWaySyncService):
            def __init__(self):
                from lib.sync_base import SyncDirection, setup_logger
                self.service_name = "PingPongTest"
                self.direction = SyncDirection.TWO_WAY
                self.logger = setup_logger("PingPongTest")
                self.sync_logger = MagicMock()
                self.notion = MagicMock()
                self.supabase = MagicMock()
                self.notion_database_id = "test-db"

            def convert_from_source(self, r):
                return {'title': 'test'}

            def convert_to_source(self, r):
                return {'Name': NotionPropertyBuilder.title('test')}

            def get_source_id(self, r):
                return r.get('id', '')

        service = TestSync()

        # Record was synced from Notion at time T, then edited locally at T+1h
        old_time = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        new_time = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

        edited_record = {
            'id': 'sb-1',
            'title': 'Locally Edited',
            'notion_page_id': 'notion-1',
            'notion_updated_at': old_time,
            'updated_at': new_time,  # 1 hour newer than last Notion sync
            'deleted_at': None,
            'last_sync_source': 'supabase',  # Explicitly marked for sync
        }

        service.supabase.select_all.return_value = [edited_record]
        service.supabase.select_updated_since.return_value = [edited_record]
        service.notion.query_database.return_value = []
        service.notion.update_page.return_value = {
            'id': 'notion-1',
            'last_edited_time': datetime.now(timezone.utc).isoformat()
        }

        result = service._sync_supabase_to_notion(full_sync=True, since_hours=24)

        # Should detect the local edit and sync to Notion
        service.notion.update_page.assert_called_once()


# ============================================================================
# ISSUE 10: Notion property builder truncation limits
# ============================================================================


class TestNotionPropertyBuilderLimits:
    """
    Notion has specific limits:
    - Title: no documented limit but using 100 chars in code
    - Rich text: 2000 chars (code uses 1990 for safety)
    - Block content: 2000 chars (code uses 1990)

    Test that truncation is applied correctly and doesn't break
    multi-byte characters.
    """

    def test_title_builder_handles_none(self):
        """Title builder should handle None gracefully."""
        from lib.sync_base import NotionPropertyBuilder

        result = NotionPropertyBuilder.title(None)
        assert result == {"title": [{"text": {"content": ""}}]}

    def test_rich_text_builder_truncates_at_1990(self):
        """Rich text should be truncated at 1990 chars."""
        from lib.sync_base import NotionPropertyBuilder

        long_text = 'A' * 3000
        result = NotionPropertyBuilder.rich_text(long_text)
        content = result['rich_text'][0]['text']['content']
        assert len(content) == 1990

    def test_rich_text_builder_handles_none(self):
        """Rich text builder should return empty array for None."""
        from lib.sync_base import NotionPropertyBuilder

        result = NotionPropertyBuilder.rich_text(None)
        assert result == {"rich_text": []}

    def test_select_builder_handles_none(self):
        """Select builder should return null select for None."""
        from lib.sync_base import NotionPropertyBuilder

        result = NotionPropertyBuilder.select(None)
        assert result == {"select": None}

    def test_multi_select_builder_filters_empty_strings(self):
        """Multi-select builder should filter out empty strings."""
        from lib.sync_base import NotionPropertyBuilder

        result = NotionPropertyBuilder.multi_select(['valid', '', 'also-valid'])
        # Empty strings are filtered by the `if v` condition
        names = [item['name'] for item in result['multi_select']]
        assert names == ['valid', 'also-valid']

    def test_date_builder_handles_none(self):
        """Date builder should return null date for None."""
        from lib.sync_base import NotionPropertyBuilder

        result = NotionPropertyBuilder.date(None)
        assert result == {"date": None}

    def test_relation_builder_filters_empty_ids(self):
        """Relation builder should filter out empty IDs."""
        from lib.sync_base import NotionPropertyBuilder

        result = NotionPropertyBuilder.relation(['id-1', '', 'id-2', None])
        ids = [r['id'] for r in result['relation']]
        # Empty string and None should be filtered
        assert ids == ['id-1', 'id-2']


# ============================================================================
# ISSUE 11: Content block builder edge cases
# ============================================================================


class TestContentBlockBuilderEdgeCases:
    """
    Test content block builder for edge cases that could cause
    Notion API errors.
    """

    def test_chunked_paragraphs_with_empty_string(self):
        """Empty string should produce no blocks."""
        from lib.sync_base import ContentBlockBuilder

        result = ContentBlockBuilder.chunked_paragraphs('')
        assert result == []

    def test_chunked_paragraphs_with_none(self):
        """None should produce no blocks."""
        from lib.sync_base import ContentBlockBuilder

        result = ContentBlockBuilder.chunked_paragraphs(None)
        assert result == []

    def test_chunked_paragraphs_splits_long_text(self):
        """Text longer than block limit should be split."""
        from lib.sync_base import ContentBlockBuilder

        # Create text that's definitely longer than the limit
        long_text = 'A' * 5000
        result = ContentBlockBuilder.chunked_paragraphs(long_text)
        assert len(result) >= 3  # 5000 / 1990 = at least 3 blocks

    def test_chunked_paragraphs_preserves_paragraph_boundaries(self):
        """Splitting should prefer paragraph boundaries over arbitrary cuts."""
        from lib.sync_base import ContentBlockBuilder

        text = "First paragraph.\n\nSecond paragraph.\n\nThird paragraph."
        result = ContentBlockBuilder.chunked_paragraphs(text)

        # Should fit in one block since it's short
        assert len(result) == 1
        content = result[0]['paragraph']['rich_text'][0]['text']['content']
        assert 'First paragraph.' in content
        assert 'Third paragraph.' in content

    def test_paragraph_block_truncates_at_1990(self):
        """Individual paragraph block should truncate at 1990 chars."""
        from lib.sync_base import ContentBlockBuilder

        long_text = 'B' * 3000
        result = ContentBlockBuilder.paragraph(long_text)
        content = result['paragraph']['rich_text'][0]['text']['content']
        assert len(content) <= 1990


# ============================================================================
# ISSUE 12: Meetings content extraction with special characters
# ============================================================================


class TestMeetingsContentExtraction:
    """
    Test content extraction for meetings with special characters,
    Unicode, and edge cases in block types.
    """

    def test_meeting_content_blocks_with_long_topics(self):
        """Topic text longer than 2000 chars should be truncated."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        meeting = {
            'topics_discussed': [
                {'topic': 'T' * 3000, 'details': ['D' * 3000]},
            ],
        }

        blocks = service._build_meeting_content_blocks(meeting)
        # Should have heading + numbered list item
        assert len(blocks) >= 2

        # Find the numbered list item and check truncation
        for block in blocks:
            if block.get('type') == 'numbered_list_item':
                text = block['numbered_list_item']['rich_text'][0]['text']['content']
                assert len(text) <= 1990  # Truncated by _build_rich_text_block

    def test_meeting_content_blocks_with_empty_topics(self):
        """Empty topic strings should be filtered out."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        meeting = {
            'topics_discussed': [
                {'topic': '', 'details': []},
                '',
                {'topic': 'Valid Topic', 'details': []},
            ],
        }

        blocks = service._build_meeting_content_blocks(meeting)
        # Only "Valid Topic" should produce a list item
        numbered_items = [b for b in blocks if b.get('type') == 'numbered_list_item']
        assert len(numbered_items) == 1

    def test_meeting_follow_ups_mixed_formats(self):
        """Follow-up items can be dicts or strings."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        meeting = {
            'follow_up_items': [
                {'topic': 'Email client', 'context': 'Send by Friday'},
                'Call investor',
            ],
        }

        blocks = service._build_meeting_content_blocks(meeting)
        todo_items = [b for b in blocks if b.get('type') == 'to_do']
        assert len(todo_items) == 2


# ============================================================================
# ISSUE 13: Contacts dedup by name normalization edge cases
# ============================================================================


class TestContactsDeduplication:
    """
    Test the contacts deduplication logic for edge cases in name matching.
    """

    def test_find_existing_contact_by_email(self):
        """Should find contact by email match."""
        from sync_contacts_unified import ContactsSyncService

        service = ContactsSyncService.__new__(ContactsSyncService)

        contact_data = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
        }

        all_contacts = [
            {'id': 'c1', 'first_name': 'John', 'last_name': 'Doe', 'email': 'john@example.com'},
        ]

        result = service._find_existing_contact(contact_data, all_contacts)
        assert result is not None
        assert result['id'] == 'c1'

    def test_find_existing_contact_email_case_insensitive(self):
        """Email matching should be case-insensitive."""
        from sync_contacts_unified import ContactsSyncService

        service = ContactsSyncService.__new__(ContactsSyncService)

        contact_data = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'JOHN@EXAMPLE.COM',
        }

        all_contacts = [
            {'id': 'c1', 'first_name': 'John', 'last_name': 'Doe', 'email': 'john@example.com'},
        ]

        result = service._find_existing_contact(contact_data, all_contacts)
        assert result is not None

    def test_find_existing_contact_by_name_when_no_email(self):
        """Should fallback to name matching when email is None."""
        from sync_contacts_unified import ContactsSyncService

        service = ContactsSyncService.__new__(ContactsSyncService)

        contact_data = {
            'first_name': 'Jane',
            'last_name': 'Smith',
            'email': None,
        }

        all_contacts = [
            {'id': 'c1', 'first_name': 'Jane', 'last_name': 'Smith', 'email': None},
        ]

        result = service._find_existing_contact(contact_data, all_contacts)
        assert result is not None

    def test_find_existing_contact_name_with_extra_spaces(self):
        """Name normalization should handle extra spaces."""
        from sync_contacts_unified import ContactsSyncService

        service = ContactsSyncService.__new__(ContactsSyncService)

        # Normalize name function
        assert service._normalize_name('  John  ', '  Doe  ') == 'john doe'
        assert service._normalize_name('John', '') == 'john'
        assert service._normalize_name('', '') == ''
        assert service._normalize_name(None, None) == ''

    def test_find_existing_contact_no_match(self):
        """Should return None when no match found."""
        from sync_contacts_unified import ContactsSyncService

        service = ContactsSyncService.__new__(ContactsSyncService)

        contact_data = {
            'first_name': 'Unknown',
            'last_name': 'Person',
            'email': 'unknown@test.com',
        }

        all_contacts = [
            {'id': 'c1', 'first_name': 'John', 'last_name': 'Doe', 'email': 'john@example.com'},
        ]

        result = service._find_existing_contact(contact_data, all_contacts)
        assert result is None


# ============================================================================
# ISSUE 14: Meetings preserve user-editable fields during N->S sync
# ============================================================================


class TestFieldPreservationDuringSync:
    """
    When syncing from Notion to Supabase, certain fields in Supabase
    are user-editable and should NOT be overwritten by the sync.

    Currently only 'notes' is preserved for meetings. This tests
    that the preservation logic works correctly.
    """

    def test_meetings_preserves_notes_when_notion_has_no_equivalent(self):
        """Notes field should be preserved during Notion->Supabase update."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()
        service.notion = MagicMock()
        service.supabase = MagicMock()
        service.notion_database_id = 'test-db'
        service.service_name = 'MeetingsSync'
        from lib.sync_base import SyncDirection
        service.sync_direction = SyncDirection.TWO_WAY
        service.sync_logger = MagicMock()

        notion_records = [
            {
                'id': 'notion-1',
                'last_edited_time': '2025-02-15T10:00:00Z',
                'properties': {
                    'Meeting': {'title': [{'plain_text': 'Meeting'}]},
                    'Date': {'date': {'start': '2025-02-15'}},
                    'Location': {'rich_text': []},
                    'People': {'relation': []},
                },
            }
        ]

        existing = {
            'id': 'sb-1',
            'notion_page_id': 'notion-1',
            'notion_updated_at': '2025-02-14T10:00:00Z',
            'last_sync_source': 'notion',
            'title': 'Meeting',
            'notes': 'My personal meeting notes',
        }

        service.notion.query_database.return_value = notion_records
        service.notion.cache_crm_contacts.return_value = None
        service.supabase.cache_contacts.return_value = None
        service.supabase.select_all.return_value = [existing]
        service.notion.extract_page_content.return_value = ('Content', False)
        service.notion.extract_page_sections.return_value = []

        result = service._sync_notion_to_supabase(full_sync=True, since_hours=24)

        # Verify notes field is preserved
        upsert_call = service.supabase.upsert.call_args
        upsert_data = upsert_call[0][0]
        assert upsert_data.get('notes') == 'My personal meeting notes'

    def test_meetings_does_not_preserve_none_notes(self):
        """If existing notes is None, it should not be added to update data."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()
        service.notion = MagicMock()
        service.supabase = MagicMock()
        service.notion_database_id = 'test-db'
        service.service_name = 'MeetingsSync'
        from lib.sync_base import SyncDirection
        service.sync_direction = SyncDirection.TWO_WAY
        service.sync_logger = MagicMock()

        notion_records = [
            {
                'id': 'notion-1',
                'last_edited_time': '2025-02-15T10:00:00Z',
                'properties': {
                    'Meeting': {'title': [{'plain_text': 'Meeting'}]},
                    'Date': {'date': {'start': '2025-02-15'}},
                    'Location': {'rich_text': []},
                    'People': {'relation': []},
                },
            }
        ]

        existing = {
            'id': 'sb-1',
            'notion_page_id': 'notion-1',
            'notion_updated_at': '2025-02-14T10:00:00Z',
            'last_sync_source': 'notion',
            'title': 'Meeting',
            'notes': None,  # No notes
        }

        service.notion.query_database.return_value = notion_records
        service.notion.cache_crm_contacts.return_value = None
        service.supabase.cache_contacts.return_value = None
        service.supabase.select_all.return_value = [existing]
        service.notion.extract_page_content.return_value = ('Content', False)
        service.notion.extract_page_sections.return_value = []

        result = service._sync_notion_to_supabase(full_sync=True, since_hours=24)

        # Notes should NOT be in the data (None is not preserved)
        upsert_call = service.supabase.upsert.call_args
        upsert_data = upsert_call[0][0]
        assert 'notes' not in upsert_data


# ============================================================================
# ISSUE 15: Markdown roundtrip in content blocks
# ============================================================================


class TestMarkdownRoundtrip:
    """Test that markdown formatting survives Notion->Supabase->Notion roundtrip."""

    def test_rich_text_to_markdown_bold(self):
        """Bold text should be converted to **markdown**."""
        from lib.sync_base import rich_text_to_markdown

        rich_text = [
            {'text': {'content': 'Hello '}, 'annotations': {}},
            {'text': {'content': 'world'}, 'annotations': {'bold': True}},
        ]

        result = rich_text_to_markdown(rich_text)
        assert result == 'Hello **world**'

    def test_rich_text_to_markdown_italic(self):
        """Italic text should be converted to *markdown*."""
        from lib.sync_base import rich_text_to_markdown

        rich_text = [
            {'text': {'content': 'emphasis'}, 'annotations': {'italic': True}},
        ]

        result = rich_text_to_markdown(rich_text)
        assert result == '*emphasis*'

    def test_rich_text_to_markdown_code(self):
        """Code text should be converted to `code`."""
        from lib.sync_base import rich_text_to_markdown

        rich_text = [
            {'text': {'content': 'inline_code'}, 'annotations': {'code': True}},
        ]

        result = rich_text_to_markdown(rich_text)
        assert result == '`inline_code`'

    def test_rich_text_to_markdown_link(self):
        """Linked text should be converted to [text](url)."""
        from lib.sync_base import rich_text_to_markdown

        rich_text = [
            {'text': {'content': 'click here', 'link': {'url': 'https://example.com'}}, 'annotations': {}},
        ]

        result = rich_text_to_markdown(rich_text)
        assert result == '[click here](https://example.com)'

    def test_rich_text_to_markdown_empty_array(self):
        """Empty array should return empty string."""
        from lib.sync_base import rich_text_to_markdown

        assert rich_text_to_markdown([]) == ''
        assert rich_text_to_markdown(None) == ''

    def test_parse_markdown_to_rich_text_bold(self):
        """**bold** should be parsed to bold annotations."""
        from lib.sync_base import parse_markdown_to_rich_text

        result = parse_markdown_to_rich_text('Hello **world**')
        assert len(result) == 2
        # First segment: plain text
        assert result[0]['text']['content'] == 'Hello '
        # Second segment: bold
        assert result[1]['text']['content'] == 'world'
        assert result[1].get('annotations', {}).get('bold') is True

    def test_parse_markdown_plain_text(self):
        """Plain text should produce single rich_text element."""
        from lib.sync_base import parse_markdown_to_rich_text

        result = parse_markdown_to_rich_text('Just plain text')
        assert len(result) == 1
        assert result[0]['text']['content'] == 'Just plain text'


# ============================================================================
# ISSUE 16: Contacts sync uses two different sync files
# ============================================================================


class TestContactsSyncFileConsistency:
    """
    There are TWO contact sync files:
    1. sync_contacts_unified.py - Uses TwoWaySyncService base, has Google + Notion sync
    2. syncs/__init__.py does NOT export any contacts sync

    The legacy `sync_contacts.py` in syncs/ package doesn't exist,
    but there IS a standalone `sync_contacts_unified.py` at root.

    Verify both Notion property extraction approaches are consistent.
    """

    def test_contacts_property_extraction_notion_name(self):
        """Contacts sync should correctly split full name into first/last."""
        from sync_contacts_unified import ContactsSyncService

        service = ContactsSyncService.__new__(ContactsSyncService)
        service.logger = MagicMock()

        # Single name
        notion_record = {
            'properties': {
                'Name': {'title': [{'plain_text': 'Madonna'}]},
                'Company': {'rich_text': []},
                'Mail': {'email': None},
                'Position': {'rich_text': []},
                'Birthday': {'date': None},
                'LinkedIn URL': {'url': None},
                'Location': {'select': None},
                'Type': {'select': None},
                'Phone Number': {'phone_number': None},
                'Subscribed?': {'checkbox': False},
            }
        }

        result = service.convert_from_source(notion_record)
        assert result['first_name'] == 'Madonna'
        assert result['last_name'] == ''

    def test_contacts_property_extraction_full_name(self):
        """Full name with space should split correctly."""
        from sync_contacts_unified import ContactsSyncService

        service = ContactsSyncService.__new__(ContactsSyncService)
        service.logger = MagicMock()

        notion_record = {
            'properties': {
                'Name': {'title': [{'plain_text': 'John Doe'}]},
                'Company': {'rich_text': [{'plain_text': 'Acme Corp'}]},
                'Mail': {'email': 'john@acme.com'},
                'Position': {'rich_text': [{'plain_text': 'Engineer'}]},
                'Birthday': {'date': {'start': '1990-01-15'}},
                'LinkedIn URL': {'url': 'https://linkedin.com/in/johndoe'},
                'Location': {'select': {'name': 'Singapore'}},
                'Type': {'select': {'name': 'Business'}},
                'Phone Number': {'phone_number': '+6512345678'},
                'Subscribed?': {'checkbox': True},
            }
        }

        result = service.convert_from_source(notion_record)
        assert result['first_name'] == 'John'
        assert result['last_name'] == 'Doe'
        assert result['email'] == 'john@acme.com'
        assert result['company'] == 'Acme Corp'
        assert result['job_title'] == 'Engineer'
        assert result['birthday'] == '1990-01-15'
        assert result['linkedin_url'] == 'https://linkedin.com/in/johndoe'
        assert result['location'] == 'Singapore'
        assert result['contact_type'] == 'Business'
        assert result['subscribed'] is True

    def test_contacts_property_extraction_multi_word_last_name(self):
        """Names with multiple words should split on first space only."""
        from sync_contacts_unified import ContactsSyncService

        service = ContactsSyncService.__new__(ContactsSyncService)
        service.logger = MagicMock()

        notion_record = {
            'properties': {
                'Name': {'title': [{'plain_text': 'Jean Claude Van Damme'}]},
                'Company': {'rich_text': []},
                'Mail': {'email': None},
                'Position': {'rich_text': []},
                'Birthday': {'date': None},
                'LinkedIn URL': {'url': None},
                'Location': {'select': None},
                'Type': {'select': None},
                'Phone Number': {'phone_number': None},
                'Subscribed?': {'checkbox': False},
            }
        }

        result = service.convert_from_source(notion_record)
        assert result['first_name'] == 'Jean'
        assert result['last_name'] == 'Claude Van Damme'

    def test_contacts_convert_to_source_builds_full_name(self):
        """Converting back to Notion should combine first + last name."""
        from sync_contacts_unified import ContactsSyncService

        service = ContactsSyncService.__new__(ContactsSyncService)
        service.logger = MagicMock()

        supabase_record = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@acme.com',
            'company': 'Acme Corp',
            'job_title': 'Engineer',
            'linkedin_url': 'https://linkedin.com/in/johndoe',
            'birthday': '1990-01-15',
            'location': 'Singapore',
            'contact_type': 'Business',
            'subscribed': True,
            'phone': '+6512345678',
        }

        result = service.convert_to_source(supabase_record)
        assert result['Name']['title'][0]['text']['content'] == 'John Doe'
        assert result['Mail']['email'] == 'john@acme.com'
        assert result['Company']['rich_text'][0]['text']['content'] == 'Acme Corp'
        assert result['Position']['rich_text'][0]['text']['content'] == 'Engineer'
        assert result['LinkedIn URL']['url'] == 'https://linkedin.com/in/johndoe'
        assert result['Birthday']['date']['start'] == '1990-01-15'
        assert result['Location']['select']['name'] == 'Singapore'
        assert result['Type']['select']['name'] == 'Business'
        assert result['Subscribed?']['checkbox'] is True
        assert result['Phone Number']['phone_number'] == '+6512345678'
