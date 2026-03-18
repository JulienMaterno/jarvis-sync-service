"""
Comprehensive tests for lib/sync_base.py

Tests cover:
- Safety valve with various count ratios
- Configurable threshold via env var
- Warning-only mode
- Timestamp comparison edge cases
- NotionPropertyExtractor for each Notion property type
- NotionPropertyBuilder for each type
- rich_text_to_markdown() and parse_markdown_to_rich_text() roundtrip
- ContentBlockBuilder.chunked_paragraphs() with text exceeding Notion limits
- retry_on_error with 429 rate limit handling
"""

import os
import time
import pytest
from unittest.mock import patch, MagicMock
import httpx


# ============================================================================
# Safety Valve Tests
# ============================================================================


class TestSafetyValve:
    """Test the check_safety_valve method with various count ratios and configurations."""

    def _make_service(self):
        """Create a BaseSyncService subclass for testing."""
        from lib.sync_base import BaseSyncService, SyncDirection

        class TestSync(BaseSyncService):
            def __init__(self):
                super().__init__("TestSync", SyncDirection.TWO_WAY)

            def convert_from_source(self, source_record):
                return source_record

            def get_source_id(self, source_record):
                return source_record.get('id', '')

        return TestSync()

    def test_equal_counts_passes(self):
        """100 source / 100 destination should pass safely."""
        service = self._make_service()
        is_safe, msg = service.check_safety_valve(100, 100, "test")
        assert is_safe is True

    def test_half_counts_passes(self):
        """50 source / 100 destination (50%) should pass since > 10% threshold."""
        service = self._make_service()
        is_safe, msg = service.check_safety_valve(50, 100, "test")
        assert is_safe is True

    def test_low_ratio_triggers_abort(self):
        """5 source / 100 destination (5%) should trigger safety valve in abort mode."""
        service = self._make_service()
        with patch.dict(os.environ, {'SAFETY_VALVE_MODE': 'abort'}, clear=False):
            # Need to reimport to pick up env var change
            import lib.sync_base as sb
            original_mode = sb.SAFETY_VALVE_MODE
            sb.SAFETY_VALVE_MODE = 'abort'
            try:
                is_safe, msg = service.check_safety_valve(5, 100, "Notion -> Supabase")
                assert is_safe is False
                assert "Safety Valve Triggered" in msg
                assert "source=5" in msg
                assert "destination=100" in msg
            finally:
                sb.SAFETY_VALVE_MODE = original_mode

    def test_zero_source_triggers(self):
        """0 source / 100 destination should trigger safety valve."""
        service = self._make_service()
        import lib.sync_base as sb
        original_mode = sb.SAFETY_VALVE_MODE
        sb.SAFETY_VALVE_MODE = 'abort'
        try:
            is_safe, msg = service.check_safety_valve(0, 100, "test")
            assert is_safe is False
            assert "Safety Valve Triggered" in msg
        finally:
            sb.SAFETY_VALVE_MODE = original_mode

    def test_small_destination_bypasses(self):
        """If destination has fewer records than SAFETY_VALVE_MIN_RECORDS, valve is bypassed."""
        service = self._make_service()
        # 0 source / 5 destination - would trigger but destination is too small
        is_safe, msg = service.check_safety_valve(0, 5, "test")
        assert is_safe is True
        assert "bypassed" in msg.lower()

    def test_configurable_threshold_via_module_var(self):
        """Test changing SAFETY_VALVE_THRESHOLD to a different value."""
        service = self._make_service()
        import lib.sync_base as sb
        original = sb.SAFETY_VALVE_THRESHOLD
        original_mode = sb.SAFETY_VALVE_MODE
        sb.SAFETY_VALVE_THRESHOLD = 0.5
        sb.SAFETY_VALVE_MODE = 'abort'
        try:
            # 40 / 100 = 40%, below 50% threshold
            is_safe, msg = service.check_safety_valve(40, 100, "test")
            assert is_safe is False
            assert "threshold=50%" in msg
        finally:
            sb.SAFETY_VALVE_THRESHOLD = original
            sb.SAFETY_VALVE_MODE = original_mode

    def test_warning_mode_continues(self):
        """In warning mode, safety valve logs but allows sync to continue."""
        service = self._make_service()
        import lib.sync_base as sb
        original_mode = sb.SAFETY_VALVE_MODE
        sb.SAFETY_VALVE_MODE = 'warn'
        try:
            is_safe, msg = service.check_safety_valve(5, 100, "test")
            assert is_safe is True  # Continues in warn mode
            assert "Safety Valve Triggered" in msg
            assert "mode=warn" in msg
        finally:
            sb.SAFETY_VALVE_MODE = original_mode

    def test_safety_valve_message_includes_ratio(self):
        """Verify the safety valve message includes the actual ratio for debugging."""
        service = self._make_service()
        import lib.sync_base as sb
        original_mode = sb.SAFETY_VALVE_MODE
        sb.SAFETY_VALVE_MODE = 'abort'
        try:
            is_safe, msg = service.check_safety_valve(3, 100, "test")
            assert is_safe is False
            assert "ratio=" in msg
            assert "3.00%" in msg
        finally:
            sb.SAFETY_VALVE_MODE = original_mode

    def test_exactly_at_threshold_passes(self):
        """Source count exactly at threshold (10% of dest) should pass."""
        service = self._make_service()
        # 10 / 100 = exactly 10%, which is NOT less than 10%, so should pass
        is_safe, msg = service.check_safety_valve(10, 100, "test")
        assert is_safe is True

    def test_just_below_threshold_triggers(self):
        """Source count just below threshold should trigger."""
        service = self._make_service()
        import lib.sync_base as sb
        original_mode = sb.SAFETY_VALVE_MODE
        sb.SAFETY_VALVE_MODE = 'abort'
        try:
            # 9 / 100 = 9%, below 10%
            is_safe, msg = service.check_safety_valve(9, 100, "test")
            assert is_safe is False
        finally:
            sb.SAFETY_VALVE_MODE = original_mode


# ============================================================================
# Timestamp Comparison Tests
# ============================================================================


class TestCompareTimestamps:
    """Test compare_timestamps with edge cases."""

    def _make_service(self):
        from lib.sync_base import BaseSyncService, SyncDirection

        class TestSync(BaseSyncService):
            def __init__(self):
                super().__init__("TestSync", SyncDirection.TWO_WAY)

            def convert_from_source(self, source_record):
                return source_record

            def get_source_id(self, source_record):
                return source_record.get('id', '')

        return TestSync()

    def test_none_source_returns_zero(self):
        """None source timestamp returns 0 (unknown)."""
        service = self._make_service()
        result = service.compare_timestamps(None, "2025-01-01T00:00:00Z")
        assert result == 0

    def test_none_dest_returns_zero(self):
        """None destination timestamp returns 0 (unknown)."""
        service = self._make_service()
        result = service.compare_timestamps("2025-01-01T00:00:00Z", None)
        assert result == 0

    def test_both_none_returns_zero(self):
        """Both timestamps None returns 0."""
        service = self._make_service()
        result = service.compare_timestamps(None, None)
        assert result == 0

    def test_source_newer_returns_positive(self):
        """Source clearly newer returns 1."""
        service = self._make_service()
        result = service.compare_timestamps(
            "2025-01-02T00:00:00Z",
            "2025-01-01T00:00:00Z"
        )
        assert result == 1

    def test_dest_newer_returns_negative(self):
        """Destination clearly newer returns -1."""
        service = self._make_service()
        result = service.compare_timestamps(
            "2025-01-01T00:00:00Z",
            "2025-01-02T00:00:00Z"
        )
        assert result == -1

    def test_equal_timestamps_returns_zero(self):
        """Equal timestamps returns 0."""
        service = self._make_service()
        result = service.compare_timestamps(
            "2025-01-01T12:00:00Z",
            "2025-01-01T12:00:00Z"
        )
        assert result == 0

    def test_within_buffer_returns_zero(self):
        """Timestamps within 5-second buffer returns 0."""
        service = self._make_service()
        result = service.compare_timestamps(
            "2025-01-01T12:00:03Z",
            "2025-01-01T12:00:00Z"
        )
        assert result == 0  # 3 seconds within 5 second buffer

    def test_outside_buffer_returns_nonzero(self):
        """Timestamps outside 5-second buffer returns non-zero."""
        service = self._make_service()
        result = service.compare_timestamps(
            "2025-01-01T12:00:10Z",
            "2025-01-01T12:00:00Z"
        )
        assert result == 1  # 10 seconds outside 5 second buffer

    def test_custom_buffer(self):
        """Custom buffer_seconds is respected."""
        service = self._make_service()
        # 10 seconds apart, 15 second buffer
        result = service.compare_timestamps(
            "2025-01-01T12:00:10Z",
            "2025-01-01T12:00:00Z",
            buffer_seconds=15
        )
        assert result == 0  # Within buffer

    def test_iso_format_with_offset(self):
        """ISO timestamps with timezone offsets work."""
        service = self._make_service()
        result = service.compare_timestamps(
            "2025-01-01T12:00:00+00:00",
            "2025-01-01T11:59:50+00:00"
        )
        assert result == 1

    def test_invalid_timestamp_returns_zero(self):
        """Malformed timestamps return 0 instead of crashing."""
        service = self._make_service()
        result = service.compare_timestamps(
            "not-a-date",
            "2025-01-01T00:00:00Z"
        )
        assert result == 0


# ============================================================================
# NotionPropertyExtractor Tests
# ============================================================================


class TestNotionPropertyExtractor:
    """Test extraction of each Notion property type."""

    def test_extract_title(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Name": {"title": [{"plain_text": "Hello World"}]}}
        assert NotionPropertyExtractor.title(props, "Name") == "Hello World"

    def test_extract_title_empty(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Name": {"title": []}}
        assert NotionPropertyExtractor.title(props, "Name") == ""

    def test_extract_title_missing(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {}
        assert NotionPropertyExtractor.title(props, "Name") == ""

    def test_extract_rich_text(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Notes": {"rich_text": [{"plain_text": "Some notes"}]}}
        assert NotionPropertyExtractor.rich_text(props, "Notes") == "Some notes"

    def test_extract_rich_text_empty(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Notes": {"rich_text": []}}
        assert NotionPropertyExtractor.rich_text(props, "Notes") is None

    def test_extract_number(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Score": {"number": 42}}
        assert NotionPropertyExtractor.number(props, "Score") == 42

    def test_extract_number_none(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Score": {"number": None}}
        assert NotionPropertyExtractor.number(props, "Score") is None

    def test_extract_select(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Status": {"select": {"name": "Active"}}}
        assert NotionPropertyExtractor.select(props, "Status") == "Active"

    def test_extract_select_none(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Status": {"select": None}}
        assert NotionPropertyExtractor.select(props, "Status") is None

    def test_extract_multi_select(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Tags": {"multi_select": [{"name": "Tag1"}, {"name": "Tag2"}]}}
        result = NotionPropertyExtractor.multi_select(props, "Tags")
        assert result == ["Tag1", "Tag2"]

    def test_extract_multi_select_empty(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Tags": {"multi_select": []}}
        assert NotionPropertyExtractor.multi_select(props, "Tags") == []

    def test_extract_date(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Due": {"date": {"start": "2025-01-15"}}}
        assert NotionPropertyExtractor.date(props, "Due") == "2025-01-15"

    def test_extract_date_none(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Due": {"date": None}}
        assert NotionPropertyExtractor.date(props, "Due") is None

    def test_extract_url(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Link": {"url": "https://example.com"}}
        assert NotionPropertyExtractor.url(props, "Link") == "https://example.com"

    def test_extract_url_none(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Link": {"url": None}}
        assert NotionPropertyExtractor.url(props, "Link") is None

    def test_extract_checkbox_true(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Done": {"checkbox": True}}
        assert NotionPropertyExtractor.checkbox(props, "Done") is True

    def test_extract_checkbox_false(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Done": {"checkbox": False}}
        assert NotionPropertyExtractor.checkbox(props, "Done") is False

    def test_extract_checkbox_missing(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {}
        assert NotionPropertyExtractor.checkbox(props, "Done") is False

    def test_extract_email(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Email": {"email": "test@example.com"}}
        assert NotionPropertyExtractor.email(props, "Email") == "test@example.com"

    def test_extract_email_none(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Email": {"email": None}}
        assert NotionPropertyExtractor.email(props, "Email") is None

    def test_extract_phone(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Phone": {"phone_number": "+1234567890"}}
        assert NotionPropertyExtractor.phone(props, "Phone") == "+1234567890"

    def test_extract_phone_none(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"Phone": {"phone_number": None}}
        assert NotionPropertyExtractor.phone(props, "Phone") is None

    def test_extract_relation(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"People": {"relation": [{"id": "page-1"}, {"id": "page-2"}]}}
        result = NotionPropertyExtractor.relation(props, "People")
        assert result == ["page-1", "page-2"]

    def test_extract_relation_empty(self):
        from lib.sync_base import NotionPropertyExtractor
        props = {"People": {"relation": []}}
        assert NotionPropertyExtractor.relation(props, "People") == []


# ============================================================================
# NotionPropertyBuilder Tests
# ============================================================================


class TestNotionPropertyBuilder:
    """Test building of each Notion property type."""

    def test_build_title(self):
        from lib.sync_base import NotionPropertyBuilder
        result = NotionPropertyBuilder.title("Hello")
        assert result == {"title": [{"text": {"content": "Hello"}}]}

    def test_build_title_none(self):
        from lib.sync_base import NotionPropertyBuilder
        result = NotionPropertyBuilder.title(None)
        assert result == {"title": [{"text": {"content": ""}}]}

    def test_build_rich_text(self):
        from lib.sync_base import NotionPropertyBuilder
        result = NotionPropertyBuilder.rich_text("Some text")
        assert result["rich_text"][0]["text"]["content"] == "Some text"

    def test_build_rich_text_empty(self):
        from lib.sync_base import NotionPropertyBuilder
        result = NotionPropertyBuilder.rich_text(None)
        assert result == {"rich_text": []}

    def test_build_rich_text_truncation(self):
        from lib.sync_base import NotionPropertyBuilder
        long_text = "x" * 3000
        result = NotionPropertyBuilder.rich_text(long_text)
        assert len(result["rich_text"][0]["text"]["content"]) == 1990

    def test_build_number(self):
        from lib.sync_base import NotionPropertyBuilder
        assert NotionPropertyBuilder.number(42) == {"number": 42}

    def test_build_number_none(self):
        from lib.sync_base import NotionPropertyBuilder
        assert NotionPropertyBuilder.number(None) == {"number": None}

    def test_build_select(self):
        from lib.sync_base import NotionPropertyBuilder
        assert NotionPropertyBuilder.select("High") == {"select": {"name": "High"}}

    def test_build_select_none(self):
        from lib.sync_base import NotionPropertyBuilder
        assert NotionPropertyBuilder.select(None) == {"select": None}

    def test_build_multi_select(self):
        from lib.sync_base import NotionPropertyBuilder
        result = NotionPropertyBuilder.multi_select(["A", "B"])
        assert result == {"multi_select": [{"name": "A"}, {"name": "B"}]}

    def test_build_multi_select_filters_empty(self):
        from lib.sync_base import NotionPropertyBuilder
        result = NotionPropertyBuilder.multi_select(["A", "", "B"])
        assert result == {"multi_select": [{"name": "A"}, {"name": "B"}]}

    def test_build_date(self):
        from lib.sync_base import NotionPropertyBuilder
        assert NotionPropertyBuilder.date("2025-01-15") == {"date": {"start": "2025-01-15"}}

    def test_build_date_none(self):
        from lib.sync_base import NotionPropertyBuilder
        assert NotionPropertyBuilder.date(None) == {"date": None}

    def test_build_url(self):
        from lib.sync_base import NotionPropertyBuilder
        assert NotionPropertyBuilder.url("https://example.com") == {"url": "https://example.com"}

    def test_build_checkbox(self):
        from lib.sync_base import NotionPropertyBuilder
        assert NotionPropertyBuilder.checkbox(True) == {"checkbox": True}

    def test_build_email(self):
        from lib.sync_base import NotionPropertyBuilder
        assert NotionPropertyBuilder.email("a@b.com") == {"email": "a@b.com"}

    def test_build_phone(self):
        from lib.sync_base import NotionPropertyBuilder
        assert NotionPropertyBuilder.phone("+1234") == {"phone_number": "+1234"}

    def test_build_relation(self):
        from lib.sync_base import NotionPropertyBuilder
        result = NotionPropertyBuilder.relation(["id-1", "id-2"])
        assert result == {"relation": [{"id": "id-1"}, {"id": "id-2"}]}

    def test_build_relation_filters_empty(self):
        from lib.sync_base import NotionPropertyBuilder
        result = NotionPropertyBuilder.relation(["id-1", "", "id-2"])
        assert result == {"relation": [{"id": "id-1"}, {"id": "id-2"}]}


# ============================================================================
# Markdown / Rich Text Conversion Tests
# ============================================================================


class TestRichTextMarkdownConversion:
    """Test rich_text_to_markdown and parse_markdown_to_rich_text roundtrip."""

    def test_plain_text_roundtrip(self):
        from lib.sync_base import rich_text_to_markdown, parse_markdown_to_rich_text
        original = "Hello world"
        rt = parse_markdown_to_rich_text(original)
        result = rich_text_to_markdown(rt)
        assert result == original

    def test_bold_roundtrip(self):
        from lib.sync_base import rich_text_to_markdown, parse_markdown_to_rich_text
        original = "Hello **bold** world"
        rt = parse_markdown_to_rich_text(original)
        result = rich_text_to_markdown(rt)
        assert result == original

    def test_italic_roundtrip(self):
        from lib.sync_base import rich_text_to_markdown, parse_markdown_to_rich_text
        original = "Hello *italic* world"
        rt = parse_markdown_to_rich_text(original)
        result = rich_text_to_markdown(rt)
        assert result == original

    def test_code_roundtrip(self):
        from lib.sync_base import rich_text_to_markdown, parse_markdown_to_rich_text
        original = "Use `print()` function"
        rt = parse_markdown_to_rich_text(original)
        result = rich_text_to_markdown(rt)
        assert result == original

    def test_strikethrough_roundtrip(self):
        from lib.sync_base import rich_text_to_markdown, parse_markdown_to_rich_text
        original = "This is ~~deleted~~ text"
        rt = parse_markdown_to_rich_text(original)
        result = rich_text_to_markdown(rt)
        assert result == original

    def test_link_roundtrip(self):
        from lib.sync_base import rich_text_to_markdown, parse_markdown_to_rich_text
        original = "Click [here](https://example.com) to visit"
        rt = parse_markdown_to_rich_text(original)
        result = rich_text_to_markdown(rt)
        assert result == original

    def test_bold_italic_roundtrip(self):
        from lib.sync_base import rich_text_to_markdown, parse_markdown_to_rich_text
        original = "This is ***bold italic*** text"
        rt = parse_markdown_to_rich_text(original)
        result = rich_text_to_markdown(rt)
        assert result == original

    def test_empty_input(self):
        from lib.sync_base import rich_text_to_markdown, parse_markdown_to_rich_text
        assert rich_text_to_markdown([]) == ""
        assert rich_text_to_markdown(None) == ""
        assert parse_markdown_to_rich_text("") == []
        assert parse_markdown_to_rich_text(None) == []

    def test_rich_text_to_markdown_with_annotations(self):
        """Test converting Notion rich_text with annotations to markdown."""
        from lib.sync_base import rich_text_to_markdown
        rich_text = [
            {"text": {"content": "normal "}, "annotations": {}},
            {"text": {"content": "bold"}, "annotations": {"bold": True}},
            {"text": {"content": " text"}, "annotations": {}},
        ]
        result = rich_text_to_markdown(rich_text)
        assert result == "normal **bold** text"


# ============================================================================
# ContentBlockBuilder Tests
# ============================================================================


class TestContentBlockBuilder:
    """Test ContentBlockBuilder.chunked_paragraphs and other builders."""

    def test_chunked_paragraphs_empty(self):
        from lib.sync_base import ContentBlockBuilder
        assert ContentBlockBuilder.chunked_paragraphs("") == []
        assert ContentBlockBuilder.chunked_paragraphs(None) == []

    def test_chunked_paragraphs_short_text(self):
        from lib.sync_base import ContentBlockBuilder
        blocks = ContentBlockBuilder.chunked_paragraphs("Hello world")
        assert len(blocks) == 1
        assert blocks[0]["type"] == "paragraph"

    def test_chunked_paragraphs_exceeds_limit(self):
        """Text exceeding Notion's 1990 char limit should be split into multiple blocks."""
        from lib.sync_base import ContentBlockBuilder
        # Create text that exceeds the limit
        long_text = "A" * 3000
        blocks = ContentBlockBuilder.chunked_paragraphs(long_text)
        assert len(blocks) >= 2
        for block in blocks:
            assert block["type"] == "paragraph"

    def test_chunked_paragraphs_respects_paragraph_boundaries(self):
        """Splitting should prefer paragraph boundaries."""
        from lib.sync_base import ContentBlockBuilder
        para1 = "A" * 500
        para2 = "B" * 500
        text = f"{para1}\n\n{para2}"
        blocks = ContentBlockBuilder.chunked_paragraphs(text)
        # Both paragraphs fit in one chunk (< 1990)
        assert len(blocks) == 1

    def test_chunked_paragraphs_splits_on_boundary(self):
        """Long paragraphs that exceed limit individually get split."""
        from lib.sync_base import ContentBlockBuilder
        para1 = "A" * 1000
        para2 = "B" * 1000
        para3 = "C" * 1000
        text = f"{para1}\n\n{para2}\n\n{para3}"
        blocks = ContentBlockBuilder.chunked_paragraphs(text)
        # Should have at least 2 blocks since total > 1990
        assert len(blocks) >= 2

    def test_paragraph_builder(self):
        from lib.sync_base import ContentBlockBuilder
        block = ContentBlockBuilder.paragraph("Hello")
        assert block["type"] == "paragraph"
        assert block["paragraph"]["rich_text"][0]["text"]["content"] == "Hello"

    def test_heading_builders(self):
        from lib.sync_base import ContentBlockBuilder
        h1 = ContentBlockBuilder.heading_1("H1")
        h2 = ContentBlockBuilder.heading_2("H2")
        h3 = ContentBlockBuilder.heading_3("H3")
        assert h1["type"] == "heading_1"
        assert h2["type"] == "heading_2"
        assert h3["type"] == "heading_3"

    def test_bulleted_list_item(self):
        from lib.sync_base import ContentBlockBuilder
        block = ContentBlockBuilder.bulleted_list_item("Item 1")
        assert block["type"] == "bulleted_list_item"

    def test_to_do(self):
        from lib.sync_base import ContentBlockBuilder
        block = ContentBlockBuilder.to_do("Task", checked=True)
        assert block["type"] == "to_do"
        assert block["to_do"]["checked"] is True

    def test_divider(self):
        from lib.sync_base import ContentBlockBuilder
        block = ContentBlockBuilder.divider()
        assert block["type"] == "divider"

    def test_quote(self):
        from lib.sync_base import ContentBlockBuilder
        block = ContentBlockBuilder.quote("Quote text")
        assert block["type"] == "quote"


# ============================================================================
# Retry Decorator with 429 Handling Tests
# ============================================================================


class TestRetryOnError:
    """Test retry_on_error decorator with 429 rate limit handling."""

    def test_successful_call_no_retry(self):
        """Successful calls should not retry."""
        from lib.sync_base import retry_on_error

        call_count = 0

        @retry_on_error(max_retries=3, base_delay=0.01)
        def succeed():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = succeed()
        assert result == "ok"
        assert call_count == 1

    def test_retries_on_transient_error(self):
        """Should retry on generic exceptions."""
        from lib.sync_base import retry_on_error

        call_count = 0

        @retry_on_error(max_retries=3, base_delay=0.01)
        def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("transient")
            return "ok"

        result = fail_then_succeed()
        assert result == "ok"
        assert call_count == 3

    def test_raises_after_max_retries(self):
        """Should raise after exhausting all retries."""
        from lib.sync_base import retry_on_error

        @retry_on_error(max_retries=2, base_delay=0.01)
        def always_fail():
            raise ConnectionError("permanent")

        with pytest.raises(ConnectionError):
            always_fail()

    def test_no_retry_on_client_error(self):
        """Should NOT retry on non-429 4xx errors."""
        from lib.sync_base import retry_on_error

        call_count = 0
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.headers = {}

        @retry_on_error(max_retries=3, base_delay=0.01)
        def bad_request():
            nonlocal call_count
            call_count += 1
            raise httpx.HTTPStatusError("bad request", request=MagicMock(), response=mock_response)

        with pytest.raises(httpx.HTTPStatusError):
            bad_request()
        assert call_count == 1  # No retry

    def test_retries_on_429_rate_limit(self):
        """Should retry on 429 responses with appropriate backoff."""
        from lib.sync_base import retry_on_error

        call_count = 0
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {'Retry-After': '0.01'}

        @retry_on_error(max_retries=3, base_delay=0.01)
        def rate_limited_then_ok():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.HTTPStatusError("rate limited", request=MagicMock(), response=mock_response)
            return "ok"

        result = rate_limited_then_ok()
        assert result == "ok"
        assert call_count == 3

    def test_retries_on_500_server_error(self):
        """Should retry on 5xx server errors."""
        from lib.sync_base import retry_on_error

        call_count = 0
        mock_response = MagicMock()
        mock_response.status_code = 502
        mock_response.headers = {}

        @retry_on_error(max_retries=3, base_delay=0.01)
        def server_error_then_ok():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise httpx.HTTPStatusError("bad gateway", request=MagicMock(), response=mock_response)
            return "ok"

        result = server_error_then_ok()
        assert result == "ok"
        assert call_count == 2

    def test_no_retry_on_404(self):
        """404 errors should not be retried."""
        from lib.sync_base import retry_on_error

        call_count = 0
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.headers = {}

        @retry_on_error(max_retries=3, base_delay=0.01)
        def not_found():
            nonlocal call_count
            call_count += 1
            raise httpx.HTTPStatusError("not found", request=MagicMock(), response=mock_response)

        with pytest.raises(httpx.HTTPStatusError):
            not_found()
        assert call_count == 1


# ============================================================================
# _extract_retry_after Tests
# ============================================================================


class TestExtractRetryAfter:
    """Test _extract_retry_after helper."""

    def test_numeric_value(self):
        from lib.sync_base import _extract_retry_after
        mock_response = MagicMock()
        mock_response.headers = {'Retry-After': '30'}
        assert _extract_retry_after(mock_response) == 30.0

    def test_float_value(self):
        from lib.sync_base import _extract_retry_after
        mock_response = MagicMock()
        mock_response.headers = {'Retry-After': '1.5'}
        assert _extract_retry_after(mock_response) == 1.5

    def test_missing_header(self):
        from lib.sync_base import _extract_retry_after
        mock_response = MagicMock()
        mock_response.headers = {}
        assert _extract_retry_after(mock_response) is None

    def test_non_numeric_value(self):
        from lib.sync_base import _extract_retry_after
        mock_response = MagicMock()
        mock_response.headers = {'Retry-After': 'not-a-number'}
        assert _extract_retry_after(mock_response) is None

    def test_lowercase_header(self):
        from lib.sync_base import _extract_retry_after
        mock_response = MagicMock()
        mock_response.headers = {'retry-after': '10'}
        assert _extract_retry_after(mock_response) == 10.0


# ============================================================================
# SyncStats Tests
# ============================================================================


class TestSyncStats:
    """Test SyncStats dataclass."""

    def test_total_processed(self):
        from lib.sync_base import SyncStats
        stats = SyncStats(created=5, updated=3, deleted=1, skipped=2, errors=1)
        assert stats.total_processed == 11

    def test_to_dict(self):
        from lib.sync_base import SyncStats
        stats = SyncStats(created=1, updated=2)
        d = stats.to_dict()
        assert d['created'] == 1
        assert d['updated'] == 2
        assert d['total_processed'] == 3


# ============================================================================
# SyncResult Tests
# ============================================================================


class TestSyncResult:
    """Test SyncResult dataclass."""

    def test_to_dict(self):
        from lib.sync_base import SyncResult, SyncStats
        stats = SyncStats(created=1, updated=2)
        result = SyncResult(success=True, direction="test", stats=stats)
        d = result.to_dict()
        assert d['success'] is True
        assert d['direction'] == "test"
        assert d['stats']['created'] == 1
