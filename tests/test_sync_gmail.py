from datetime import datetime, timezone
from typing import Dict, List, Optional
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

import sync_gmail


class FakeResponse:
    def __init__(self, data: Optional[List[Dict[str, object]]] = None):
        self.data = data or []


class FakeExecute:
    def execute(self) -> "FakeResponse":
        return FakeResponse([])


class FakeUpdate:
    def __init__(self, supabase: "FakeSupabase", item: Dict[str, object]):
        self._supabase = supabase
        self._item = item
        self._filter: Optional[tuple[str, object]] = None

    def eq(self, key: str, value: object) -> "FakeUpdate":
        self._filter = (key, value)
        return self

    def execute(self) -> FakeResponse:
        self._supabase.updates.append({"item": self._item, "filter": self._filter})
        return FakeResponse([])


class FakeEmailsSelect:
    def __init__(self, supabase: "FakeSupabase"):
        self._supabase = supabase
        self._filter: Optional[List[str]] = None
        self._eq_value: Optional[str] = None
        self._limit: Optional[int] = None

    def in_(self, key: str, values: List[str]) -> "FakeEmailsSelect":
        self._filter = list(values)
        return self

    def eq(self, key: str, value: str) -> "FakeEmailsSelect":
        self._eq_value = value
        return self

    def limit(self, value: int) -> "FakeEmailsSelect":
        self._limit = value
        return self

    def execute(self) -> FakeResponse:
        if self._eq_value:
            record = self._supabase.existing_email_records.get(self._eq_value)
            if record:
                return FakeResponse([record])
            return FakeResponse([])

        if self._filter:
            selected = []
            for message_id in self._filter:
                record = self._supabase.existing_email_records.get(message_id)
                if record:
                    selected.append(record)
            if self._limit is not None:
                selected = selected[:self._limit]
            return FakeResponse(selected)

        return FakeResponse([])


class FakeEmailsTable:
    def __init__(self, supabase: "FakeSupabase"):
        self._supabase = supabase

    def select(self, *_, **__) -> FakeEmailsSelect:
        return FakeEmailsSelect(self._supabase)

    def upsert(self, batch: List[Dict[str, object]], on_conflict: Optional[str] = None) -> FakeExecute:
        self._supabase.upserts.append({"batch": batch, "on_conflict": on_conflict})
        return FakeExecute()

    def update(self, item: Dict[str, object]) -> FakeUpdate:
        return FakeUpdate(self._supabase, item)


class FakeSyncStateSelect:
    def __init__(self, supabase: "FakeSupabase"):
        self._supabase = supabase

    def eq(self, key: str, value: object) -> "FakeSyncStateSelect":
        return self

    def execute(self) -> FakeResponse:
        if self._supabase.history_value is None:
            return FakeResponse([])
        return FakeResponse([{"value": self._supabase.history_value}])


class FakeSyncStateTable:
    def __init__(self, supabase: "FakeSupabase"):
        self._supabase = supabase

    def select(self, *_, **__) -> FakeSyncStateSelect:
        return FakeSyncStateSelect(self._supabase)

    def upsert(self, payload: Dict[str, object]) -> FakeExecute:
        self._supabase.history_value = payload.get("value")
        self._supabase.sync_state_upserts.append(payload)
        return FakeExecute()


class FakeSyncLogsTable:
    def __init__(self, supabase: "FakeSupabase"):
        self._supabase = supabase

    def insert(self, payload: Dict[str, object]) -> FakeExecute:
        self._supabase.log_entries.append(payload)
        return FakeExecute()


class FakeSupabase:
    def __init__(self, existing_ids: Optional[List[str]] = None, existing_records: Optional[Dict[str, Dict[str, object]]] = None):
        self.existing_ids = set(existing_ids or [])
        self.upserts: List[Dict[str, object]] = []
        self.updates: List[Dict[str, object]] = []
        self.sync_state_upserts: List[Dict[str, object]] = []
        self.log_entries: List[Dict[str, object]] = []
        self.history_value: Optional[str] = None
        self.existing_email_records: Dict[str, Dict[str, object]] = existing_records or {}

        for message_id in self.existing_ids:
            self.existing_email_records.setdefault(
                message_id,
                {
                    "google_message_id": message_id,
                    "thread_id": f"thread-{message_id}",
                    "label_ids": [],
                    "snippet": "",
                },
            )

    def table(self, name: str):
        if name == "emails":
            return FakeEmailsTable(self)
        if name == "sync_state":
            return FakeSyncStateTable(self)
        if name == "sync_logs":
            return FakeSyncLogsTable(self)
        raise ValueError(f"Unexpected table {name}")


class StubGmailClient:
    def __init__(self, message_map: Dict[str, Dict[str, object]], minimal_map: Optional[Dict[str, Dict[str, object]]] = None, history_id: str = "history-1"):
        self._message_map = message_map
        self._minimal_map = minimal_map or {}
        self._history_id = history_id

    async def get_profile(self) -> Dict[str, object]:
        return {"historyId": self._history_id}

    async def list_messages(self, query: Optional[str] = None, max_results: Optional[int] = None, include_spam_trash: bool = False, client: Optional[object] = None) -> List[Dict[str, object]]:
        return [
            {"id": message_id, "threadId": message.get("threadId", f"thread-{message_id}")}
            for message_id, message in self._message_map.items()
        ]

    async def get_message(self, message_id: str, format: str = "full", client: Optional[object] = None) -> Dict[str, object]:
        if format == "full":
            return self._message_map[message_id]
        if format == "minimal":
            return self._minimal_map[message_id]
        raise AssertionError(f"Unexpected format request: {format}")

    def parse_message_body(self, payload: Dict[str, object]) -> Dict[str, str]:
        return {"text": "Body text", "html": "<p>Body text</p>"}

    def get_header(self, payload: Dict[str, object], name: str) -> str:
        for header in payload.get("headers", []):
            if header["name"].lower() == name.lower():
                return header["value"]
        return ""

    async def list_history(self, start_history_id: str, max_results: int = 100) -> Dict[str, object]:
        return {"history": [], "historyId": None, "expired": True}


class GmailSyncTests(IsolatedAsyncioTestCase):
    async def test_sync_emails_inserts_new_message(self) -> None:
        fake_supabase = FakeSupabase()

        def fake_find_contact(email: Optional[str]) -> Optional[str]:
            mapping = {"sender@example.com": "contact-sender"}
            return mapping.get(email)

        events: List[tuple[str, str, str]] = []

        async def fake_log(event_type: str, status: str, message: str, contact_id: Optional[str] = None, details: Optional[Dict[str, object]] = None) -> None:
            events.append((event_type, status, message))

        with patch("sync_gmail.supabase", fake_supabase), patch("sync_gmail.find_contact_by_email", fake_find_contact), patch("sync_gmail.log_sync_event", fake_log):
            email_dt = datetime(2025, 12, 20, 10, 0, tzinfo=timezone.utc)
            message_map = {
                "msg-new": {
                    "id": "msg-new",
                    "threadId": "thread-1",
                    "labelIds": ["INBOX"],
                    "snippet": "Hello snippet",
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Test Subject"},
                            {"name": "From", "value": "Sender <sender@example.com>"},
                            {"name": "To", "value": "Recipient <recipient@example.com>"},
                            {"name": "Date", "value": "Sat, 20 Dec 2025 10:00:00 +0000"},
                        ]
                    },
                    "internalDate": str(int(email_dt.timestamp() * 1000)),
                }
            }

            gmail_sync = sync_gmail.GmailSync()
            gmail_sync.gmail_client = StubGmailClient(message_map, history_id="history-123")

            result = await gmail_sync.sync_emails(days_history=1, max_results=5)

        self.assertEqual(result, {"status": "success", "count": 1})
        self.assertFalse(fake_supabase.updates)
        self.assertEqual(len(fake_supabase.upserts), 1)

        inserted_record = fake_supabase.upserts[0]["batch"][0]
        self.assertEqual(inserted_record["google_message_id"], "msg-new")
        self.assertEqual(inserted_record["subject"], "Test Subject")
        self.assertEqual(inserted_record["contact_id"], "contact-sender")
        self.assertEqual(inserted_record["date"], datetime(2025, 12, 20, 10, 0, tzinfo=timezone.utc).isoformat())
        self.assertEqual(inserted_record["body_text"], "Body text")

        self.assertEqual(fake_supabase.history_value, "history-123")
        self.assertTrue(events and events[-1][1] == "success" and events[-1][2].endswith("1 emails"))

    async def test_sync_emails_updates_existing_message(self) -> None:
        fake_supabase = FakeSupabase(existing_ids=["msg-existing"])
        fake_supabase.existing_email_records["msg-existing"].update(
            {
                "label_ids": ["INBOX"],
                "snippet": "Original snippet",
                "thread_id": "thread-2",
            }
        )

        async def noop_log(*args, **kwargs) -> None:
            return None

        with patch("sync_gmail.supabase", fake_supabase), patch("sync_gmail.find_contact_by_email", lambda email: None), patch("sync_gmail.log_sync_event", noop_log):
            minimal_map = {
                "msg-existing": {
                    "id": "msg-existing",
                    "threadId": "thread-2",
                    "labelIds": ["INBOX", "STARRED"],
                    "snippet": "Updated snippet",
                }
            }

            message_map = {
                "msg-existing": {
                    "id": "msg-existing",
                    "threadId": "thread-2",
                    "labelIds": ["INBOX"],
                    "snippet": "Original snippet",
                    "payload": {"headers": []},
                }
            }

            gmail_sync = sync_gmail.GmailSync()
            gmail_sync.gmail_client = StubGmailClient(message_map, minimal_map=minimal_map, history_id="history-456")

            result = await gmail_sync.sync_emails(days_history=1, max_results=5)

        self.assertEqual(result, {"status": "success", "count": 1})
        self.assertEqual(fake_supabase.upserts, [])
        self.assertEqual(len(fake_supabase.updates), 1)

        update_entry = fake_supabase.updates[0]
        self.assertEqual(update_entry["filter"], ("google_message_id", "msg-existing"))
        self.assertEqual(update_entry["item"]["label_ids"], ["INBOX", "STARRED"])
        self.assertEqual(update_entry["item"]["snippet"], "Updated snippet")

        self.assertEqual(fake_supabase.history_value, "history-456")

    async def test_sync_emails_skips_unchanged_message(self) -> None:
        fake_supabase = FakeSupabase(existing_ids=["msg-stable"])
        fake_supabase.existing_email_records["msg-stable"].update(
            {
                "label_ids": ["INBOX", "STARRED"],
                "snippet": "Stable snippet",
                "thread_id": "thread-stable",
            }
        )

        async def noop_log(*args, **kwargs) -> None:
            return None

        with patch("sync_gmail.supabase", fake_supabase), patch("sync_gmail.find_contact_by_email", lambda email: None), patch("sync_gmail.log_sync_event", noop_log):
            minimal_map = {
                "msg-stable": {
                    "id": "msg-stable",
                    "threadId": "thread-stable",
                    "labelIds": ["STARRED", "INBOX"],
                    "snippet": "Stable snippet",
                }
            }

            message_map = {
                "msg-stable": {
                    "id": "msg-stable",
                    "threadId": "thread-stable",
                    "labelIds": ["STARRED", "INBOX"],
                    "snippet": "Stable snippet",
                    "payload": {"headers": []},
                }
            }

            gmail_sync = sync_gmail.GmailSync()
            gmail_sync.gmail_client = StubGmailClient(message_map, minimal_map=minimal_map, history_id="history-789")

            result = await gmail_sync.sync_emails(days_history=1, max_results=5)

        self.assertEqual(result, {"status": "success", "count": 0})
        self.assertEqual(fake_supabase.upserts, [])
        self.assertEqual(fake_supabase.updates, [])
        self.assertEqual(fake_supabase.history_value, "history-789")


if __name__ == "__main__":
    import unittest

    unittest.main()
