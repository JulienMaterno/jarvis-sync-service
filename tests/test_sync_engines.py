"""
Tests for specific sync engines: Tasks, Meetings, Journals, Reflections.

Validates edge cases in property extraction/building, bidirectional sync,
soft delete propagation, and data loss scenarios that the generic
TwoWaySyncService tests do not cover.

All tests are fully mocked -- NO real API calls.
"""

import os
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock, PropertyMock
from typing import Dict, Any, List, Optional

# Ensure env vars are set for module imports
os.environ.setdefault('NOTION_API_TOKEN', 'test-token')
os.environ.setdefault('SUPABASE_URL', 'https://test.supabase.co')
os.environ.setdefault('SUPABASE_KEY', 'test-key')
os.environ.setdefault('NOTION_NEWSLETTERS_DB_ID', 'test-newsletters-db')
os.environ.setdefault('NOTION_DOCUMENTS_DB_ID', 'test-documents-db')


# ============================================================================
# TASKS SYNC ENGINE TESTS
# ============================================================================


class TestTasksPropertyConversion:
    """Test property extraction and building for Tasks sync."""

    def test_convert_from_source_extracts_all_fields(self):
        """Notion task record should be fully converted to Supabase format.

        Note: The Tasks sync uses NotionPropertyExtractor.select() for the Status
        property, but Notion's Status type uses {'select': {'name': ...}} format
        when accessed through the extractor. The status type in Notion DB properties
        uses the select-compatible structure.
        """
        from syncs.tasks_sync import TasksSyncService

        service = TasksSyncService.__new__(TasksSyncService)
        service.logger = MagicMock()

        notion_record = {
            'properties': {
                'Name': {'title': [{'plain_text': 'Review PR #42'}]},
                'Status': {'select': {'name': 'In progress'}},
                'Due': {'date': {'start': '2025-02-15'}},
                'Priority': {'select': {'name': 'High'}},
            }
        }

        result = service.convert_from_source(notion_record)

        assert result['title'] == 'Review PR #42'
        assert result['status'] == 'in_progress'
        assert result['due_date'] == '2025-02-15'
        assert result['priority'] == 'high'

    def test_convert_from_source_status_type_property(self):
        """Notion 'status' type property (not 'select') is correctly handled.

        The tasks sync code first checks for {'status': {'name': ...}} format
        before falling back to NotionPropertyExtractor.select(). This means
        Notion's built-in Status property type is properly extracted.
        """
        from syncs.tasks_sync import TasksSyncService

        service = TasksSyncService.__new__(TasksSyncService)
        service.logger = MagicMock()

        notion_record = {
            'properties': {
                'Name': {'title': [{'plain_text': 'Task with status type'}]},
                'Status': {'status': {'name': 'In progress'}},  # 'status' type, not 'select'
            }
        }

        result = service.convert_from_source(notion_record)
        # The code first checks props['Status']['status']['name'] before fallback
        assert result['status'] == 'in_progress'

    def test_convert_from_source_handles_missing_title(self):
        """Empty title should default to 'Untitled'."""
        from syncs.tasks_sync import TasksSyncService

        service = TasksSyncService.__new__(TasksSyncService)
        service.logger = MagicMock()

        notion_record = {
            'properties': {
                'Name': {'title': []},
                'Status': {'status': {'name': 'Not started'}},
            }
        }

        result = service.convert_from_source(notion_record)
        assert result['title'] == 'Untitled'

    def test_convert_from_source_unknown_status_defaults_to_pending(self):
        """Unknown Notion status should map to 'pending' in Supabase."""
        from syncs.tasks_sync import TasksSyncService

        service = TasksSyncService.__new__(TasksSyncService)
        service.logger = MagicMock()

        notion_record = {
            'properties': {
                'Name': {'title': [{'plain_text': 'Task'}]},
                'Status': {'status': {'name': 'Blocked'}},  # Not in mapping
            }
        }

        result = service.convert_from_source(notion_record)
        assert result['status'] == 'pending'

    def test_convert_from_source_missing_priority_excluded(self):
        """Missing priority should not be included in output dict."""
        from syncs.tasks_sync import TasksSyncService

        service = TasksSyncService.__new__(TasksSyncService)
        service.logger = MagicMock()

        notion_record = {
            'properties': {
                'Name': {'title': [{'plain_text': 'Task'}]},
                'Status': {'status': {'name': 'Done'}},
            }
        }

        result = service.convert_from_source(notion_record)
        assert 'priority' not in result

    def test_convert_to_source_builds_notion_properties(self):
        """Supabase task should be converted to Notion property format."""
        from syncs.tasks_sync import TasksSyncService

        service = TasksSyncService.__new__(TasksSyncService)
        service.logger = MagicMock()

        supabase_record = {
            'title': 'Write tests',
            'status': 'completed',
            'due_date': '2025-03-01',
            'priority': 'medium',
            'created_at': '2025-01-01T00:00:00Z',
        }

        result = service.convert_to_source(supabase_record)

        assert result['Name']['title'][0]['text']['content'] == 'Write tests'
        assert result['Status']['status']['name'] == 'Done'
        assert result['Due']['date']['start'] == '2025-03-01'
        assert result['Priority']['select']['name'] == 'Medium'
        assert result['Created']['date']['start'] == '2025-01-01'

    def test_convert_to_source_truncates_long_title(self):
        """Title should be truncated to 100 characters for Notion."""
        from syncs.tasks_sync import TasksSyncService

        service = TasksSyncService.__new__(TasksSyncService)
        service.logger = MagicMock()

        long_title = 'A' * 200
        supabase_record = {
            'title': long_title,
            'status': 'pending',
        }

        result = service.convert_to_source(supabase_record)
        assert len(result['Name']['title'][0]['text']['content']) == 100

    def test_convert_to_source_unknown_status_maps_to_not_started(self):
        """Unknown Supabase status should map to 'Not started' in Notion."""
        from syncs.tasks_sync import TasksSyncService

        service = TasksSyncService.__new__(TasksSyncService)
        service.logger = MagicMock()

        supabase_record = {
            'title': 'Task',
            'status': 'unknown_status',
        }

        result = service.convert_to_source(supabase_record)
        assert result['Status']['status']['name'] == 'Not started'


# ============================================================================
# MEETINGS SYNC ENGINE TESTS
# ============================================================================


class TestMeetingsPropertyConversion:
    """Test property extraction and building for Meetings sync."""

    def test_convert_from_source_basic_meeting(self):
        """Basic Notion meeting should extract title, date, and location."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        notion_record = {
            'properties': {
                'Meeting': {'title': [{'plain_text': 'Standup with Team'}]},
                'Date': {'date': {'start': '2025-02-10'}},
                'Location': {'rich_text': [{'plain_text': 'Zoom'}]},
            }
        }

        result = service.convert_from_source(notion_record)

        assert result['title'] == 'Standup with Team'
        assert result['date'] == '2025-02-10'
        assert result['location'] == 'Zoom'
        assert result['source_file'] == 'notion-sync'

    def test_convert_from_source_missing_location_is_none(self):
        """Missing location should be None."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        notion_record = {
            'properties': {
                'Meeting': {'title': [{'plain_text': 'Quick Chat'}]},
                'Date': {'date': None},
                'Location': {'rich_text': []},
            }
        }

        result = service.convert_from_source(notion_record)
        assert result['location'] is None

    def test_convert_to_source_builds_notion_meeting(self):
        """Supabase meeting should be converted to Notion format."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        supabase_record = {
            'title': 'Client Call',
            'date': '2025-02-15T14:00:00+08:00',
            'location': 'Google Meet',
        }

        result = service.convert_to_source(supabase_record)

        assert result['Meeting']['title'][0]['text']['content'] == 'Client Call'
        assert result['Date']['date']['start'] == '2025-02-15'
        assert result['Location']['rich_text'][0]['text']['content'] == 'Google Meet'

    def test_convert_to_source_truncates_location(self):
        """Location should be truncated to 200 chars for Notion rich_text."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        supabase_record = {
            'title': 'Meeting',
            'date': '2025-02-15',
            'location': 'L' * 300,
        }

        result = service.convert_to_source(supabase_record)
        location_text = result['Location']['rich_text'][0]['text']['content']
        assert len(location_text) == 200


class TestMeetingsContentBlocks:
    """Test content block building for meetings."""

    def test_build_content_blocks_from_sections(self):
        """Sections field should take priority over individual fields."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        meeting = {
            'sections': [
                {'heading': 'Overview', 'content': 'Brief overview of the meeting'},
                {'heading': 'Decisions', 'content': 'We decided to go with plan B'},
            ],
            'summary': 'This should be ignored since sections exist',
        }

        blocks = service._build_meeting_content_blocks(meeting)

        # Should have 2 headings + 2 paragraphs
        assert len(blocks) == 4
        assert blocks[0]['type'] == 'heading_2'
        assert blocks[1]['type'] == 'paragraph'

    def test_build_content_blocks_from_structured_fields(self):
        """When no sections, should use topics_discussed, follow_ups, key_points."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        meeting = {
            'summary': 'A productive meeting.',
            'topics_discussed': [
                {'topic': 'Budget', 'details': ['Q1 budget approved', 'Q2 pending']},
                'Hiring plan',
            ],
            'follow_up_items': [
                {'topic': 'Budget report', 'context': 'Due by Friday'},
                'Schedule interviews',
            ],
            'key_points': [
                {'point': 'Revenue up 20%'},
                'Team morale is high',
            ],
        }

        blocks = service._build_meeting_content_blocks(meeting)

        # Should have summary + topics heading + items + follow-ups heading + items + key points heading + items
        assert len(blocks) > 5
        block_types = [b['type'] for b in blocks]
        assert 'heading_3' in block_types
        assert 'numbered_list_item' in block_types
        assert 'to_do' in block_types
        assert 'bulleted_list_item' in block_types

    def test_build_content_blocks_empty_meeting(self):
        """Meeting with no content should produce empty blocks list."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        meeting = {}
        blocks = service._build_meeting_content_blocks(meeting)
        assert blocks == []


class TestMeetingsDeduplication:
    """Test meeting deduplication when syncing Supabase to Notion."""

    def test_find_existing_notion_page_by_title(self):
        """Should find existing Notion page by matching title."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        notion_records = [
            {
                'id': 'page-1',
                'properties': {
                    'Name': {'title': [{'plain_text': 'Client Call'}]},
                },
            },
            {
                'id': 'page-2',
                'properties': {
                    'Name': {'title': [{'plain_text': 'Team Standup'}]},
                },
            },
        ]

        result = service._find_existing_notion_page('Client Call', '', notion_records)
        assert result is not None
        assert result['id'] == 'page-1'

    def test_find_existing_notion_page_case_insensitive(self):
        """Title matching should be case-insensitive."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        notion_records = [
            {
                'id': 'page-1',
                'properties': {
                    'Name': {'title': [{'plain_text': 'client call'}]},
                },
            },
        ]

        result = service._find_existing_notion_page('Client Call', '', notion_records)
        assert result is not None

    def test_find_existing_notion_page_no_match(self):
        """Should return None when no matching title exists."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        notion_records = [
            {
                'id': 'page-1',
                'properties': {
                    'Name': {'title': [{'plain_text': 'Different Meeting'}]},
                },
            },
        ]

        result = service._find_existing_notion_page('Client Call', '', notion_records)
        assert result is None

    def test_find_existing_notion_page_empty_title(self):
        """Empty title should return None."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()

        result = service._find_existing_notion_page('', '', [])
        assert result is None


class TestMeetingsContactLookup:
    """Test contact lookup in MeetingsSupabaseClient."""

    def test_find_contact_by_exact_full_name(self):
        """Should find contact by exact full name match from cache."""
        from syncs.meetings_sync import MeetingsSupabaseClient

        client = MeetingsSupabaseClient.__new__(MeetingsSupabaseClient)
        client._contact_cache = {}
        client._contact_by_name = {'john doe': {'id': 'c1', 'first_name': 'John', 'last_name': 'Doe'}}
        client._contact_by_first_name = {'john': [{'id': 'c1', 'first_name': 'John', 'last_name': 'Doe'}]}
        client.client = MagicMock()
        client.base_url = 'https://test.supabase.co/rest/v1'

        result = client.find_contact_by_name('John Doe')
        assert result is not None
        assert result['id'] == 'c1'

    def test_find_contact_by_first_name_unique_match(self):
        """Should return unique first name match from cache."""
        from syncs.meetings_sync import MeetingsSupabaseClient

        client = MeetingsSupabaseClient.__new__(MeetingsSupabaseClient)
        client._contact_cache = {}
        client._contact_by_name = {}
        client._contact_by_first_name = {'jane': [{'id': 'c2', 'first_name': 'Jane', 'last_name': 'Smith'}]}
        client.client = MagicMock()
        client.base_url = 'https://test.supabase.co/rest/v1'

        result = client.find_contact_by_name('Jane')
        assert result is not None
        assert result['id'] == 'c2'

    def test_find_contact_empty_name_returns_none(self):
        """Empty name should return None."""
        from syncs.meetings_sync import MeetingsSupabaseClient

        client = MeetingsSupabaseClient.__new__(MeetingsSupabaseClient)
        client._contact_cache = {}
        client._contact_by_name = {}
        client._contact_by_first_name = {}

        result = client.find_contact_by_name('')
        assert result is None

    def test_find_contact_ambiguous_first_name_with_last_name(self):
        """Multiple first name matches should use last name to disambiguate."""
        from syncs.meetings_sync import MeetingsSupabaseClient

        client = MeetingsSupabaseClient.__new__(MeetingsSupabaseClient)
        client._contact_cache = {}
        client._contact_by_name = {}
        client._contact_by_first_name = {
            'john': [
                {'id': 'c1', 'first_name': 'John', 'last_name': 'Doe'},
                {'id': 'c2', 'first_name': 'John', 'last_name': 'Smith'},
            ]
        }
        client.client = MagicMock()
        client.base_url = 'https://test.supabase.co/rest/v1'

        result = client.find_contact_by_name('John Smith')
        assert result is not None
        assert result['id'] == 'c2'


# ============================================================================
# JOURNALS SYNC ENGINE TESTS
# ============================================================================


class TestJournalsPropertyConversion:
    """Test property extraction and building for Journals sync."""

    def test_convert_from_source_full_journal(self):
        """Full Notion journal should be converted with all select/multi-select fields."""
        from syncs.journals_sync import JournalsSyncService

        service = JournalsSyncService.__new__(JournalsSyncService)
        service.logger = MagicMock()

        notion_record = {
            'id': 'journal-1',
            'properties': {
                'Name': {'title': [{'plain_text': 'Daily Reflection'}]},
                'Date': {'date': {'start': '2025-02-10'}},
                'Mood': {'select': {'name': 'Great'}},
                'Effort': {'select': {'name': 'High'}},
                'Wakeup': {'select': {'name': '6:00-7:00'}},
                'Nutrition': {'select': {'name': 'Good'}},
                'Sport': {'multi_select': [{'name': 'Running'}, {'name': 'Gym'}]},
                'Note': {'rich_text': [{'plain_text': 'Productive day'}]},
            }
        }

        result = service.convert_from_source(notion_record)

        assert result['title'] == 'Daily Reflection'
        assert result['date'] == '2025-02-10'
        assert result['mood'] == 'Great'
        assert result['effort'] == 'High'
        assert result['wakeup_time'] == '6:00-7:00'
        assert result['nutrition'] == 'Good'
        assert result['sports'] == ['Running', 'Gym']
        assert result['note'] == 'Productive day'
        assert result['source'] == 'notion'

    def test_convert_from_source_no_date_returns_none(self):
        """Journal without date should return None (date is required)."""
        from syncs.journals_sync import JournalsSyncService

        service = JournalsSyncService.__new__(JournalsSyncService)
        service.logger = MagicMock()

        notion_record = {
            'id': 'journal-1',
            'properties': {
                'Name': {'title': [{'plain_text': 'No Date Journal'}]},
                'Date': {'date': None},
            }
        }

        result = service.convert_from_source(notion_record)
        assert result is None

    def test_convert_to_source_uses_summary_for_note(self):
        """Note field should prefer summary over note in Supabase record."""
        from syncs.journals_sync import JournalsSyncService

        service = JournalsSyncService.__new__(JournalsSyncService)
        service.logger = MagicMock()

        supabase_record = {
            'title': 'Journal',
            'date': '2025-02-10',
            'summary': 'AI-generated summary',
            'note': 'Original note',
        }

        result = service.convert_to_source(supabase_record)
        note_text = result['Note']['rich_text'][0]['text']['content']
        assert note_text == 'AI-generated summary'

    def test_convert_to_source_falls_back_to_note(self):
        """When no summary, should use note field."""
        from syncs.journals_sync import JournalsSyncService

        service = JournalsSyncService.__new__(JournalsSyncService)
        service.logger = MagicMock()

        supabase_record = {
            'title': 'Journal',
            'date': '2025-02-10',
            'note': 'My note',
        }

        result = service.convert_to_source(supabase_record)
        note_text = result['Note']['rich_text'][0]['text']['content']
        assert note_text == 'My note'


class TestJournalsContentBlocks:
    """Test content block building for journals."""

    def test_build_content_blocks_structured_fields(self):
        """Should build blocks from key_events, accomplishments, challenges, etc."""
        from syncs.journals_sync import JournalsSyncService

        service = JournalsSyncService.__new__(JournalsSyncService)
        service.logger = MagicMock()

        journal = {
            'key_events': ['Shipped v2.0', 'Met with investors'],
            'accomplishments': ['Closed 3 tickets'],
            'challenges': ['Server outage'],
            'gratitude': ['Supportive team'],
            'tomorrow_focus': ['Fix CI pipeline'],
        }

        blocks = service._build_content_blocks(journal)

        # Should have headings + list items for each category
        assert len(blocks) > 0
        heading_types = [b for b in blocks if b['type'] == 'heading_2']
        list_types = [b for b in blocks if b['type'] == 'bulleted_list_item']
        assert len(heading_types) == 5  # 5 categories
        assert len(list_types) == 6  # 2 + 1 + 1 + 1 + 1

    def test_build_content_blocks_sections_priority(self):
        """Sections should be rendered first, then structured fields."""
        from syncs.journals_sync import JournalsSyncService

        service = JournalsSyncService.__new__(JournalsSyncService)
        service.logger = MagicMock()

        journal = {
            'sections': [{'heading': 'Morning', 'content': 'Had coffee'}],
            'key_events': ['This should also appear'],
        }

        blocks = service._build_content_blocks(journal)

        # Both sections and key_events should produce blocks
        assert len(blocks) > 2

    def test_build_content_blocks_fallback_to_content(self):
        """When no structured data, should use raw content field."""
        from syncs.journals_sync import JournalsSyncService

        service = JournalsSyncService.__new__(JournalsSyncService)
        service.logger = MagicMock()

        journal = {
            'content': 'Just a simple journal entry about my day.',
        }

        blocks = service._build_content_blocks(journal)
        assert len(blocks) == 1
        assert blocks[0]['type'] == 'paragraph'


# ============================================================================
# REFLECTIONS SYNC ENGINE TESTS
# ============================================================================


class TestReflectionsPropertyConversion:
    """Test property extraction and building for Reflections sync."""

    def test_convert_from_source_with_tags(self):
        """Should extract title, date, and tags."""
        from syncs.reflections_sync import ReflectionsSyncService

        service = ReflectionsSyncService.__new__(ReflectionsSyncService)
        service.logger = MagicMock()

        notion_record = {
            'properties': {
                'Name': {'title': [{'plain_text': 'On Focus'}]},
                'Date': {'date': {'start': '2025-02-10'}},
                'Tags': {'multi_select': [{'name': 'productivity'}, {'name': 'mindset'}]},
            }
        }

        result = service.convert_from_source(notion_record)

        assert result['title'] == 'On Focus'
        assert result['date'] == '2025-02-10'
        assert result['tags'] == ['productivity', 'mindset']

    def test_convert_from_source_no_tags_is_none(self):
        """Missing tags should be None, not empty list."""
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
        assert result['tags'] is None

    def test_convert_to_source_limits_tags_to_10(self):
        """Should limit tags to 10 items for Notion."""
        from syncs.reflections_sync import ReflectionsSyncService

        service = ReflectionsSyncService.__new__(ReflectionsSyncService)
        service.logger = MagicMock()

        supabase_record = {
            'title': 'Reflection',
            'date': '2025-02-10',
            'tags': [f'tag-{i}' for i in range(15)],
        }

        result = service.convert_to_source(supabase_record)
        multi_select = result['Tags']['multi_select']
        assert len(multi_select) == 10

    def test_convert_to_source_no_title_omits_name(self):
        """If title is empty string, Name property should not be in result."""
        from syncs.reflections_sync import ReflectionsSyncService

        service = ReflectionsSyncService.__new__(ReflectionsSyncService)
        service.logger = MagicMock()

        supabase_record = {
            'title': '',
            'date': '2025-02-10',
        }

        result = service.convert_to_source(supabase_record)
        assert 'Name' not in result


class TestReflectionsLookup:
    """Test the _build_supabase_lookup for preventing re-creation of deleted records."""

    def test_lookup_includes_soft_deleted_records(self):
        """Lookup should include soft-deleted records to prevent re-creation."""
        from syncs.reflections_sync import ReflectionsSyncService

        service = ReflectionsSyncService.__new__(ReflectionsSyncService)
        service.supabase = MagicMock()
        service.logger = MagicMock()

        service.supabase.get_all_active.return_value = [
            {'id': 'r1', 'notion_page_id': 'np-1', 'deleted_at': None},
        ]
        service.supabase.get_deleted_with_notion_id.return_value = [
            {'id': 'r2', 'notion_page_id': 'np-2', 'deleted_at': '2025-01-15T00:00:00Z'},
        ]

        lookup = service._build_supabase_lookup()

        assert 'np-1' in lookup
        assert 'np-2' in lookup
        assert lookup['np-1']['id'] == 'r1'
        assert lookup['np-2']['id'] == 'r2'

    def test_lookup_active_takes_precedence_over_deleted(self):
        """If same notion_page_id exists in both active and deleted, active wins."""
        from syncs.reflections_sync import ReflectionsSyncService

        service = ReflectionsSyncService.__new__(ReflectionsSyncService)
        service.supabase = MagicMock()
        service.logger = MagicMock()

        service.supabase.get_all_active.return_value = [
            {'id': 'r1-active', 'notion_page_id': 'np-1', 'deleted_at': None},
        ]
        service.supabase.get_deleted_with_notion_id.return_value = [
            {'id': 'r1-deleted', 'notion_page_id': 'np-1', 'deleted_at': '2025-01-15T00:00:00Z'},
        ]

        lookup = service._build_supabase_lookup()

        # Active record should take precedence
        assert lookup['np-1']['id'] == 'r1-active'


# ============================================================================
# MEETINGS DELETION SYNC TESTS
# ============================================================================


class TestMeetingsDeletionSync:
    """Test deletion sync behavior specific to meetings."""

    def test_notion_deletion_soft_deletes_and_clears_link(self):
        """When a Notion page is deleted, should soft-delete AND clear notion_page_id."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()
        service.supabase = MagicMock()
        service.notion = MagicMock()
        service.notion_database_id = 'test-db'

        # Supabase has a linked meeting
        service.supabase.select_all.return_value = [
            {
                'id': 'meeting-1',
                'notion_page_id': 'notion-1',
                'deleted_at': None,
                'title': 'Deleted Meeting',
            }
        ]

        # Notion doesn't have the page anymore
        service.notion.query_database.return_value = []

        deleted_count = service._sync_notion_deletions()

        assert deleted_count == 1
        service.supabase.soft_delete.assert_called_once_with('meeting-1')
        # Should also clear notion_page_id to prevent re-archiving attempts
        service.supabase.update.assert_called_once_with('meeting-1', {
            'notion_page_id': None,
            'notion_updated_at': None,
        })

    def test_notion_deletion_skips_already_deleted(self):
        """Already soft-deleted records should not be processed."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()
        service.supabase = MagicMock()
        service.notion = MagicMock()
        service.notion_database_id = 'test-db'

        # Record is already soft-deleted
        service.supabase.select_all.return_value = [
            {
                'id': 'meeting-1',
                'notion_page_id': 'notion-1',
                'deleted_at': '2025-01-15T00:00:00Z',
                'title': 'Already Deleted',
            }
        ]

        service.notion.query_database.return_value = []

        deleted_count = service._sync_notion_deletions()

        # Should skip since record is already deleted (filtered by linked_records)
        assert deleted_count == 0
        service.supabase.soft_delete.assert_not_called()

    def test_notion_deletion_handles_query_failure(self):
        """Should return 0 and not crash if Notion query fails."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()
        service.supabase = MagicMock()
        service.notion = MagicMock()
        service.notion_database_id = 'test-db'

        service.supabase.select_all.return_value = [
            {
                'id': 'meeting-1',
                'notion_page_id': 'notion-1',
                'deleted_at': None,
                'title': 'Meeting',
            }
        ]
        service.notion.query_database.side_effect = Exception("Notion API error")

        deleted_count = service._sync_notion_deletions()
        assert deleted_count == 0


# ============================================================================
# MEETINGS DATA PRESERVATION TESTS
# ============================================================================


class TestMeetingsDataPreservation:
    """Test that Notion-to-Supabase sync preserves user-editable fields."""

    def test_notion_sync_preserves_notes_field(self):
        """When syncing from Notion, existing 'notes' field should be preserved."""
        from syncs.meetings_sync import MeetingsSyncService

        service = MeetingsSyncService.__new__(MeetingsSyncService)
        service.logger = MagicMock()
        service.notion = MagicMock()
        service.supabase = MagicMock()
        service.notion_database_id = 'test-db'
        service.service_name = 'MeetingsSync'
        from lib.sync_base import SyncDirection, SyncLogger
        service.sync_direction = SyncDirection.TWO_WAY
        service.sync_logger = MagicMock()

        # Setup: Notion has updated properties
        notion_records = [
            {
                'id': 'notion-1',
                'last_edited_time': '2025-02-15T10:00:00Z',
                'properties': {
                    'Meeting': {'title': [{'plain_text': 'Updated Title'}]},
                    'Date': {'date': {'start': '2025-02-15'}},
                    'Location': {'rich_text': []},
                    'People': {'relation': []},
                },
            }
        ]

        # Supabase has existing record with user notes
        existing = {
            'id': 'sb-1',
            'notion_page_id': 'notion-1',
            'notion_updated_at': '2025-02-14T10:00:00Z',
            'last_sync_source': 'notion',
            'title': 'Old Title',
            'notes': 'Important user notes that must NOT be lost',
        }

        service.notion.query_database.return_value = notion_records
        service.notion.cache_crm_contacts.return_value = None
        service.supabase.cache_contacts.return_value = None
        service.supabase.select_all.return_value = [existing]
        service.notion.extract_page_content.return_value = ('Meeting content', False)
        service.notion.extract_page_sections.return_value = []

        result = service._sync_notion_to_supabase(full_sync=True, since_hours=24)

        assert result.success is True
        # Verify that the upsert call preserved the notes field
        upsert_call = service.supabase.upsert.call_args
        upsert_data = upsert_call[0][0]
        assert upsert_data.get('notes') == 'Important user notes that must NOT be lost'


# ============================================================================
# RATE LIMITING TESTS
# ============================================================================


class TestNotionRateLimiting:
    """Test the Notion rate limiting fix in retry_on_error."""

    def test_retry_on_error_uses_retry_after_header(self):
        """Should use Retry-After header value when present."""
        from lib.sync_base import _extract_retry_after
        import httpx

        mock_response = MagicMock(spec=httpx.Response)
        mock_response.headers = {'Retry-After': '2.5'}

        result = _extract_retry_after(mock_response)
        assert result == 2.5

    def test_retry_on_error_handles_missing_retry_after(self):
        """Should return None when Retry-After header is missing."""
        from lib.sync_base import _extract_retry_after
        import httpx

        mock_response = MagicMock(spec=httpx.Response)
        mock_response.headers = {}

        result = _extract_retry_after(mock_response)
        assert result is None

    def test_retry_on_error_handles_invalid_retry_after(self):
        """Should return None when Retry-After header is invalid."""
        from lib.sync_base import _extract_retry_after
        import httpx

        mock_response = MagicMock(spec=httpx.Response)
        mock_response.headers = {'Retry-After': 'not-a-number'}

        result = _extract_retry_after(mock_response)
        assert result is None

    def test_retry_on_error_does_not_retry_client_errors(self):
        """Should not retry 400, 401, 403, 404 errors (only 429 and 5xx)."""
        from lib.sync_base import retry_on_error
        import httpx

        call_count = 0

        @retry_on_error(max_retries=3, base_delay=0.01, exceptions=(httpx.HTTPStatusError,))
        def failing_func():
            nonlocal call_count
            call_count += 1
            response = MagicMock(spec=httpx.Response)
            response.status_code = 400
            response.request = MagicMock()
            raise httpx.HTTPStatusError("Bad request", request=response.request, response=response)

        with pytest.raises(httpx.HTTPStatusError):
            failing_func()

        # Should only be called once - no retries for 400
        assert call_count == 1

    def test_retry_on_error_retries_on_429(self):
        """Should retry on 429 (rate limit) errors."""
        from lib.sync_base import retry_on_error
        import httpx

        call_count = 0

        @retry_on_error(max_retries=3, base_delay=0.01, exceptions=(httpx.HTTPStatusError,))
        def sometimes_rate_limited():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                response = MagicMock(spec=httpx.Response)
                response.status_code = 429
                response.headers = {'Retry-After': '0.01'}
                response.request = MagicMock()
                raise httpx.HTTPStatusError("Rate limited", request=response.request, response=response)
            return "success"

        result = sometimes_rate_limited()
        assert result == "success"
        assert call_count == 3

    def test_retry_on_error_retries_on_server_error(self):
        """Should retry on 500+ server errors."""
        from lib.sync_base import retry_on_error
        import httpx

        call_count = 0

        @retry_on_error(max_retries=3, base_delay=0.01, exceptions=(httpx.HTTPStatusError,))
        def sometimes_failing():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                response = MagicMock(spec=httpx.Response)
                response.status_code = 500
                response.request = MagicMock()
                raise httpx.HTTPStatusError("Server error", request=response.request, response=response)
            return "recovered"

        result = sometimes_failing()
        assert result == "recovered"
        assert call_count == 2


# ============================================================================
# TIMESTAMP COMPARISON EDGE CASES
# ============================================================================


class TestTimestampComparison:
    """Test edge cases in timestamp comparison logic."""

    def test_compare_timestamps_with_5_second_buffer(self):
        """Timestamps within 5 seconds should be considered equal."""
        from lib.sync_base import BaseSyncService

        class TestSync(BaseSyncService):
            def convert_from_source(self, r): return r
            def get_source_id(self, r): return r.get('id', '')

        service = TestSync.__new__(TestSync)

        # 3 seconds apart - should be equal
        result = service.compare_timestamps(
            '2025-02-15T10:00:03Z',
            '2025-02-15T10:00:00Z',
        )
        assert result == 0

    def test_compare_timestamps_source_is_newer(self):
        """Source more than 5 seconds newer should return 1."""
        from lib.sync_base import BaseSyncService

        class TestSync(BaseSyncService):
            def convert_from_source(self, r): return r
            def get_source_id(self, r): return r.get('id', '')

        service = TestSync.__new__(TestSync)

        result = service.compare_timestamps(
            '2025-02-15T10:00:10Z',
            '2025-02-15T10:00:00Z',
        )
        assert result == 1

    def test_compare_timestamps_dest_is_newer(self):
        """Destination more than 5 seconds newer should return -1."""
        from lib.sync_base import BaseSyncService

        class TestSync(BaseSyncService):
            def convert_from_source(self, r): return r
            def get_source_id(self, r): return r.get('id', '')

        service = TestSync.__new__(TestSync)

        result = service.compare_timestamps(
            '2025-02-15T10:00:00Z',
            '2025-02-15T10:00:10Z',
        )
        assert result == -1

    def test_compare_timestamps_none_source(self):
        """None source timestamp should return 0."""
        from lib.sync_base import BaseSyncService

        class TestSync(BaseSyncService):
            def convert_from_source(self, r): return r
            def get_source_id(self, r): return r.get('id', '')

        service = TestSync.__new__(TestSync)

        result = service.compare_timestamps(None, '2025-02-15T10:00:00Z')
        assert result == 0

    def test_compare_timestamps_none_dest(self):
        """None destination timestamp should return 0."""
        from lib.sync_base import BaseSyncService

        class TestSync(BaseSyncService):
            def convert_from_source(self, r): return r
            def get_source_id(self, r): return r.get('id', '')

        service = TestSync.__new__(TestSync)

        result = service.compare_timestamps('2025-02-15T10:00:00Z', None)
        assert result == 0

    def test_compare_timestamps_different_timezones(self):
        """Timestamps with different timezone offsets should compare correctly."""
        from lib.sync_base import BaseSyncService

        class TestSync(BaseSyncService):
            def convert_from_source(self, r): return r
            def get_source_id(self, r): return r.get('id', '')

        service = TestSync.__new__(TestSync)

        # These are the same moment in time (UTC+8 vs UTC)
        result = service.compare_timestamps(
            '2025-02-15T18:00:00+08:00',
            '2025-02-15T10:00:00Z',
        )
        assert result == 0

    def test_compare_timestamps_malformed_input(self):
        """Malformed timestamps should return 0 rather than crash."""
        from lib.sync_base import BaseSyncService

        class TestSync(BaseSyncService):
            def convert_from_source(self, r): return r
            def get_source_id(self, r): return r.get('id', '')

        service = TestSync.__new__(TestSync)

        result = service.compare_timestamps('not-a-timestamp', '2025-02-15T10:00:00Z')
        assert result == 0


# ============================================================================
# SAFETY VALVE EDGE CASES
# ============================================================================


class TestSafetyValveEdgeCases:
    """Test safety valve edge cases."""

    def test_safety_valve_bypassed_for_small_destination(self):
        """Safety valve should not trigger when destination has fewer than MIN_RECORDS."""
        from lib.sync_base import BaseSyncService

        class TestSync(BaseSyncService):
            def convert_from_source(self, r): return r
            def get_source_id(self, r): return r.get('id', '')

        service = TestSync.__new__(TestSync)
        service.logger = MagicMock()

        # 0 source, 5 destination (< MIN_RECORDS=10)
        is_safe, msg = service.check_safety_valve(0, 5, "test")
        assert is_safe is True

    def test_safety_valve_triggers_for_large_deletion(self):
        """Safety valve should trigger when source is <10% of destination."""
        from lib.sync_base import BaseSyncService
        import lib.sync_base as sb

        original_mode = sb.SAFETY_VALVE_MODE
        sb.SAFETY_VALVE_MODE = 'abort'

        try:
            class TestSync(BaseSyncService):
                def convert_from_source(self, r): return r
                def get_source_id(self, r): return r.get('id', '')

            service = TestSync.__new__(TestSync)
            service.logger = MagicMock()

            # 5 source, 100 destination = 5% < 10%
            is_safe, msg = service.check_safety_valve(5, 100, "test")
            assert is_safe is False
            assert "Safety Valve" in msg
        finally:
            sb.SAFETY_VALVE_MODE = original_mode

    def test_safety_valve_warn_mode_continues(self):
        """In warn mode, safety valve should return True but log warning."""
        from lib.sync_base import BaseSyncService
        import lib.sync_base as sb

        original_mode = sb.SAFETY_VALVE_MODE
        sb.SAFETY_VALVE_MODE = 'warn'

        try:
            class TestSync(BaseSyncService):
                def convert_from_source(self, r): return r
                def get_source_id(self, r): return r.get('id', '')

            service = TestSync.__new__(TestSync)
            service.logger = MagicMock()

            # 5 source, 100 destination = 5% < 10%
            is_safe, msg = service.check_safety_valve(5, 100, "test")
            assert is_safe is True  # Should continue in warn mode
            assert "Safety Valve" in msg
        finally:
            sb.SAFETY_VALVE_MODE = original_mode
