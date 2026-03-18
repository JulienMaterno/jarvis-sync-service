"""
Comprehensive tests for TwoWaySyncService in lib/sync_base.py

All tests are fully mocked -- NO real API calls.

Tests cover:
- Notion -> Supabase sync direction
- Supabase -> Notion sync direction
- Bidirectional with no conflicts
- Conflict resolution via timestamps
- Soft delete propagation
- Records skipped when last_sync_source matches
- Safety valve triggering during sync
"""

import os
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock, PropertyMock
from typing import Dict, Any, List, Optional


# ============================================================================
# Test Fixture: Concrete TwoWaySyncService subclass
# ============================================================================


def make_test_sync_service():
    """Create a concrete TwoWaySyncService subclass for testing."""
    from lib.sync_base import TwoWaySyncService, NotionPropertyBuilder

    class TestTwoWaySync(TwoWaySyncService):
        def __init__(self):
            # Don't call super().__init__ to avoid real API client creation
            from lib.sync_base import SyncDirection, SyncLogger, setup_logger
            self.service_name = "TestTwoWay"
            self.direction = SyncDirection.TWO_WAY
            self.logger = setup_logger("TestTwoWay")
            self.sync_logger = MagicMock()
            self.notion = MagicMock()
            self.supabase = MagicMock()
            self.notion_database_id = "test-db-id"

        def convert_from_source(self, notion_record):
            props = notion_record.get('properties', {})
            title_arr = props.get('Name', {}).get('title', [])
            title = title_arr[0].get('plain_text', '') if title_arr else 'Untitled'
            return {'title': title}

        def convert_to_source(self, supabase_record):
            return {
                'Name': NotionPropertyBuilder.title(supabase_record.get('title', 'Untitled'))
            }

        def get_source_id(self, source_record):
            return source_record.get('id', '')

    return TestTwoWaySync()


# ============================================================================
# Notion -> Supabase Tests
# ============================================================================


class TestNotionToSupabase:
    """Test _sync_notion_to_supabase method."""

    def test_creates_new_records(self):
        """New Notion records should be created in Supabase."""
        service = make_test_sync_service()

        notion_records = [
            {
                'id': 'notion-1',
                'last_edited_time': '2025-01-15T10:00:00Z',
                'properties': {
                    'Name': {'title': [{'plain_text': 'Test Record'}]}
                }
            }
        ]

        service.notion.query_database.return_value = notion_records
        service.supabase.select_all.return_value = []  # No existing records

        result = service._sync_notion_to_supabase(full_sync=True, since_hours=24)

        assert result.success is True
        assert result.stats.created == 1
        assert result.stats.updated == 0
        service.supabase.upsert.assert_called_once()

    def test_updates_existing_records(self):
        """Existing records should be updated when Notion is newer."""
        service = make_test_sync_service()

        notion_records = [
            {
                'id': 'notion-1',
                'last_edited_time': '2025-01-15T10:00:00Z',
                'properties': {
                    'Name': {'title': [{'plain_text': 'Updated Record'}]}
                }
            }
        ]

        existing_supabase = [
            {
                'id': 'sb-1',
                'notion_page_id': 'notion-1',
                'notion_updated_at': '2025-01-14T10:00:00Z',
                'last_sync_source': 'notion',
                'title': 'Old Title'
            }
        ]

        service.notion.query_database.return_value = notion_records
        service.supabase.select_all.return_value = existing_supabase

        result = service._sync_notion_to_supabase(full_sync=True, since_hours=24)

        assert result.success is True
        assert result.stats.updated == 1
        assert result.stats.created == 0

    def test_skips_when_supabase_has_local_changes(self):
        """Records with last_sync_source='supabase' should be skipped."""
        service = make_test_sync_service()

        notion_records = [
            {
                'id': 'notion-1',
                'last_edited_time': '2025-01-15T10:00:00Z',
                'properties': {
                    'Name': {'title': [{'plain_text': 'Record'}]}
                }
            }
        ]

        existing_supabase = [
            {
                'id': 'sb-1',
                'notion_page_id': 'notion-1',
                'notion_updated_at': '2025-01-14T10:00:00Z',
                'last_sync_source': 'supabase',  # Has local changes
                'title': 'Local Edit'
            }
        ]

        service.notion.query_database.return_value = notion_records
        service.supabase.select_all.return_value = existing_supabase

        result = service._sync_notion_to_supabase(full_sync=True, since_hours=24)

        assert result.stats.skipped == 1
        assert result.stats.updated == 0
        service.supabase.upsert.assert_not_called()

    def test_skips_when_notion_not_newer(self):
        """Records should be skipped when Notion timestamp is not newer than Supabase."""
        service = make_test_sync_service()

        notion_records = [
            {
                'id': 'notion-1',
                'last_edited_time': '2025-01-15T10:00:00Z',
                'properties': {
                    'Name': {'title': [{'plain_text': 'Record'}]}
                }
            }
        ]

        existing_supabase = [
            {
                'id': 'sb-1',
                'notion_page_id': 'notion-1',
                'notion_updated_at': '2025-01-15T10:00:00Z',  # Same timestamp
                'last_sync_source': 'notion',
                'title': 'Record'
            }
        ]

        service.notion.query_database.return_value = notion_records
        service.supabase.select_all.return_value = existing_supabase

        result = service._sync_notion_to_supabase(full_sync=True, since_hours=24)

        assert result.stats.skipped == 1
        service.supabase.upsert.assert_not_called()


# ============================================================================
# Supabase -> Notion Tests
# ============================================================================


class TestSupabaseToNotion:
    """Test _sync_supabase_to_notion method."""

    def test_creates_new_notion_page(self):
        """New Supabase records without notion_page_id should create Notion pages."""
        service = make_test_sync_service()

        supabase_records = [
            {
                'id': 'sb-1',
                'title': 'New Record',
                'notion_page_id': None,
                'notion_updated_at': None,
                'updated_at': '2025-01-15T10:00:00Z',
                'deleted_at': None,
                'last_sync_source': 'supabase',
            }
        ]

        service.supabase.select_all.return_value = supabase_records
        service.supabase.select_updated_since.return_value = supabase_records
        service.notion.query_database.return_value = []
        service.notion.create_page.return_value = {
            'id': 'notion-new',
            'last_edited_time': '2025-01-15T10:01:00Z'
        }

        result = service._sync_supabase_to_notion(full_sync=True, since_hours=24)

        assert result.success is True
        assert result.stats.created == 1
        service.notion.create_page.assert_called_once()
        service.supabase.update.assert_called_once()

    def test_updates_existing_notion_page(self):
        """Existing Supabase records with notion_page_id should update Notion pages."""
        service = make_test_sync_service()

        supabase_records = [
            {
                'id': 'sb-1',
                'title': 'Updated Record',
                'notion_page_id': 'notion-1',
                'notion_updated_at': '2025-01-14T10:00:00Z',
                'updated_at': '2025-01-15T10:00:00Z',
                'deleted_at': None,
                'last_sync_source': 'supabase',
            }
        ]

        service.supabase.select_all.return_value = supabase_records
        service.supabase.select_updated_since.return_value = supabase_records
        service.notion.query_database.return_value = []
        service.notion.update_page.return_value = {
            'id': 'notion-1',
            'last_edited_time': '2025-01-15T10:01:00Z'
        }

        result = service._sync_supabase_to_notion(full_sync=True, since_hours=24)

        assert result.success is True
        assert result.stats.updated == 1
        service.notion.update_page.assert_called_once()

    def test_skips_soft_deleted_records(self):
        """Soft-deleted records should not be synced to Notion."""
        service = make_test_sync_service()

        supabase_records = [
            {
                'id': 'sb-1',
                'title': 'Deleted Record',
                'notion_page_id': 'notion-1',
                'deleted_at': '2025-01-15T10:00:00Z',
                'updated_at': '2025-01-15T10:00:00Z',
                'last_sync_source': 'supabase',
            }
        ]

        service.supabase.select_all.return_value = supabase_records
        service.supabase.select_updated_since.return_value = supabase_records
        service.notion.query_database.return_value = []

        result = service._sync_supabase_to_notion(full_sync=True, since_hours=24)

        # Soft-deleted records should be filtered out
        service.notion.update_page.assert_not_called()
        service.notion.create_page.assert_not_called()


# ============================================================================
# Deletion Sync Tests
# ============================================================================


class TestDeletionSync:
    """Test bidirectional deletion synchronization."""

    def test_notion_deletion_soft_deletes_supabase(self):
        """When a Notion page is deleted, the Supabase record should be soft-deleted."""
        service = make_test_sync_service()

        # Supabase has a record linked to Notion
        supabase_records = [
            {
                'id': 'sb-1',
                'notion_page_id': 'notion-1',
                'deleted_at': None,
                'title': 'Test Record'
            }
        ]

        # Notion no longer has the page
        notion_pages = []  # Empty = page was deleted

        service.supabase.select_all.return_value = supabase_records
        service.notion.query_database.return_value = notion_pages

        deleted_count = service._sync_notion_deletions()

        assert deleted_count == 1
        service.supabase.soft_delete.assert_called_once_with('sb-1')

    def test_supabase_deletion_archives_notion_page(self):
        """When a Supabase record is soft-deleted, the Notion page should be archived."""
        service = make_test_sync_service()

        deleted_records = [
            {
                'id': 'sb-1',
                'notion_page_id': 'notion-1',
                'deleted_at': '2025-01-15T10:00:00Z',
                'title': 'Deleted Record'
            }
        ]

        service.supabase.get_deleted_with_notion_id.return_value = deleted_records
        service.notion.archive_page.return_value = {'archived': True}

        archived_count = service._sync_supabase_deletions()

        assert archived_count == 1
        service.notion.archive_page.assert_called_once_with('notion-1')
        service.supabase.clear_notion_page_id.assert_called_once_with('sb-1')

    def test_supabase_deletion_handles_already_archived(self):
        """Should handle gracefully when Notion page is already archived."""
        service = make_test_sync_service()

        deleted_records = [
            {
                'id': 'sb-1',
                'notion_page_id': 'notion-1',
                'deleted_at': '2025-01-15T10:00:00Z',
                'title': 'Already Archived'
            }
        ]

        service.supabase.get_deleted_with_notion_id.return_value = deleted_records
        # Simulate already archived error
        service.notion.archive_page.side_effect = Exception("400: page already archived")

        archived_count = service._sync_supabase_deletions()

        # Should still count as archived since end state is same
        assert archived_count == 1
        service.supabase.clear_notion_page_id.assert_called_once()


# ============================================================================
# Conflict Resolution Tests
# ============================================================================


class TestConflictResolution:
    """Test conflict resolution via timestamps in bidirectional sync."""

    def test_notion_wins_when_newer(self):
        """When Notion is newer, its data should overwrite Supabase."""
        service = make_test_sync_service()

        notion_records = [
            {
                'id': 'notion-1',
                'last_edited_time': '2025-01-15T10:00:00Z',  # Newer
                'properties': {
                    'Name': {'title': [{'plain_text': 'Notion Version'}]}
                }
            }
        ]

        existing_supabase = [
            {
                'id': 'sb-1',
                'notion_page_id': 'notion-1',
                'notion_updated_at': '2025-01-14T10:00:00Z',  # Older
                'last_sync_source': 'notion',
                'title': 'Supabase Version'
            }
        ]

        service.notion.query_database.return_value = notion_records
        service.supabase.select_all.return_value = existing_supabase

        result = service._sync_notion_to_supabase(full_sync=True, since_hours=24)

        assert result.stats.updated == 1
        service.supabase.upsert.assert_called_once()

    def test_supabase_preserved_when_has_local_changes(self):
        """When Supabase has last_sync_source='supabase', Notion should not overwrite."""
        service = make_test_sync_service()

        notion_records = [
            {
                'id': 'notion-1',
                'last_edited_time': '2025-01-15T10:00:00Z',
                'properties': {
                    'Name': {'title': [{'plain_text': 'Notion Version'}]}
                }
            }
        ]

        existing_supabase = [
            {
                'id': 'sb-1',
                'notion_page_id': 'notion-1',
                'notion_updated_at': '2025-01-14T10:00:00Z',
                'last_sync_source': 'supabase',  # Local edits pending
                'title': 'Local Edit'
            }
        ]

        service.notion.query_database.return_value = notion_records
        service.supabase.select_all.return_value = existing_supabase

        result = service._sync_notion_to_supabase(full_sync=True, since_hours=24)

        assert result.stats.skipped == 1
        service.supabase.upsert.assert_not_called()


# ============================================================================
# Full Bidirectional Sync Tests
# ============================================================================


class TestBidirectionalSync:
    """Test the full sync() method."""

    def test_bidirectional_sync_combines_results(self):
        """Full sync should combine results from both directions."""
        service = make_test_sync_service()

        # Mock deletion phases
        service.supabase.select_all.return_value = []
        service.supabase.get_deleted_with_notion_id.return_value = []
        service.notion.query_database.return_value = []
        service.supabase.select_updated_since.return_value = []

        result = service.sync(full_sync=True)

        assert result.success is True
        assert result.direction == "bidirectional"
        service.sync_logger.log_complete.assert_called_once()

    def test_sync_sets_metrics(self):
        """Sync should populate metrics object."""
        service = make_test_sync_service()

        service.supabase.select_all.return_value = []
        service.supabase.get_deleted_with_notion_id.return_value = []
        service.notion.query_database.return_value = []
        service.supabase.select_updated_since.return_value = []

        result = service.sync(full_sync=True)

        assert result.metrics is not None
        assert result.metrics.end_time is not None
        assert result.elapsed_seconds >= 0


# ============================================================================
# Safety Valve in Sync Tests
# ============================================================================


class TestSafetyValveInSync:
    """Test safety valve behavior during actual sync operations."""

    def test_safety_valve_aborts_notion_to_supabase_on_full_sync(self):
        """Safety valve should abort Notion->Supabase on full sync when triggered."""
        service = make_test_sync_service()
        import lib.sync_base as sb
        original_mode = sb.SAFETY_VALVE_MODE
        sb.SAFETY_VALVE_MODE = 'abort'

        try:
            # Notion returns very few records compared to Supabase
            notion_records = [
                {
                    'id': f'notion-{i}',
                    'last_edited_time': '2025-01-15T10:00:00Z',
                    'properties': {'Name': {'title': [{'plain_text': f'Record {i}'}]}}
                }
                for i in range(2)  # Only 2 records
            ]

            existing_supabase = [
                {
                    'id': f'sb-{i}',
                    'notion_page_id': f'notion-{i}',
                    'notion_updated_at': '2025-01-14T10:00:00Z',
                    'last_sync_source': 'notion',
                }
                for i in range(50)  # 50 records -- 2/50 = 4% < 10%
            ]

            service.notion.query_database.return_value = notion_records
            service.supabase.select_all.return_value = existing_supabase

            result = service._sync_notion_to_supabase(full_sync=True, since_hours=24)

            assert result.success is False
            assert "Safety Valve" in result.error_message
        finally:
            sb.SAFETY_VALVE_MODE = original_mode


# ============================================================================
# filter_records_needing_notion_sync Tests
# ============================================================================


class TestFilterRecordsNeedingNotionSync:
    """Test the filter_records_needing_notion_sync helper."""

    def test_new_records_need_sync(self):
        """Records without notion_page_id should need syncing."""
        service = make_test_sync_service()
        records = [
            {'id': 'sb-1', 'title': 'New', 'notion_page_id': None, 'deleted_at': None}
        ]
        result = service.filter_records_needing_notion_sync(records)
        assert len(result) == 1

    def test_supabase_source_records_need_sync(self):
        """Records with last_sync_source='supabase' should need syncing."""
        service = make_test_sync_service()
        records = [
            {
                'id': 'sb-1', 'title': 'Edited',
                'notion_page_id': 'notion-1',
                'last_sync_source': 'supabase',
                'deleted_at': None,
                'updated_at': '2025-01-15T10:00:00Z',
                'notion_updated_at': '2025-01-14T10:00:00Z',
            }
        ]
        result = service.filter_records_needing_notion_sync(records)
        assert len(result) == 1

    def test_notion_source_records_skipped(self):
        """Records with last_sync_source='notion' and no local changes should be skipped."""
        service = make_test_sync_service()
        records = [
            {
                'id': 'sb-1', 'title': 'Synced',
                'notion_page_id': 'notion-1',
                'last_sync_source': 'notion',
                'deleted_at': None,
                'updated_at': '2025-01-14T10:00:00Z',
                'notion_updated_at': '2025-01-14T10:00:00Z',
            }
        ]
        result = service.filter_records_needing_notion_sync(records)
        assert len(result) == 0

    def test_soft_deleted_records_skipped(self):
        """Soft-deleted records should always be skipped."""
        service = make_test_sync_service()
        records = [
            {
                'id': 'sb-1', 'title': 'Deleted',
                'notion_page_id': None,
                'deleted_at': '2025-01-15T10:00:00Z',
            }
        ]
        result = service.filter_records_needing_notion_sync(records)
        assert len(result) == 0

    def test_locally_changed_records_need_sync(self):
        """Records with updated_at > notion_updated_at should need syncing
        when last_sync_source is not 'notion'."""
        service = make_test_sync_service()
        records = [
            {
                'id': 'sb-1', 'title': 'Changed',
                'notion_page_id': 'notion-1',
                'last_sync_source': 'supabase',
                'deleted_at': None,
                'updated_at': '2025-01-15T10:00:00Z',  # Much newer
                'notion_updated_at': '2025-01-10T10:00:00Z',
            }
        ]
        result = service.filter_records_needing_notion_sync(records)
        assert len(result) == 1

    def test_notion_synced_records_skipped_despite_newer_updated_at(self):
        """Records with last_sync_source='notion' should be skipped even if
        updated_at > notion_updated_at (timestamp precision bug guard)."""
        service = make_test_sync_service()
        records = [
            {
                'id': 'sb-1', 'title': 'Just synced',
                'notion_page_id': 'notion-1',
                'last_sync_source': 'notion',
                'deleted_at': None,
                'updated_at': '2025-01-15T10:00:00Z',
                'notion_updated_at': '2025-01-10T10:00:00Z',
            }
        ]
        result = service.filter_records_needing_notion_sync(records)
        assert len(result) == 0
