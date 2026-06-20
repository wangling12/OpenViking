# Copyright (c) 2026 Beijing Volvo Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from openviking.storage.queuefs.extraction_msg import ExtractionMsg
from openviking.storage.queuefs.extraction_queue import ExtractionQueue
from openviking.storage.queuefs.named_queue import NamedQueue


@pytest.mark.asyncio
async def test_extraction_queue_enqueue_deduplicates_same_session():
    mock_agfs = MagicMock()
    with patch.object(NamedQueue, "enqueue", new_callable=AsyncMock) as named_enqueue:
        named_enqueue.return_value = "queued-id"
        q = ExtractionQueue(mock_agfs, "/queue", "extraction")
        msg1 = ExtractionMsg(
            session_id="sess-1",
            archive_uri="viking://user/default/archive/sess-1",
            account_id="acc",
            user_id="u1",
        )
        msg2 = ExtractionMsg(
            session_id="sess-1",
            archive_uri="viking://user/default/archive/sess-1",
            account_id="acc",
            user_id="u1",
        )
        r1 = await q.enqueue(msg1)
        r2 = await q.enqueue(msg2)
        assert r1 == "queued-id"
        assert r2 == "deduplicated"
        assert named_enqueue.call_count == 1


@pytest.mark.asyncio
async def test_extraction_queue_enqueue_allows_different_sessions():
    mock_agfs = MagicMock()
    with patch.object(NamedQueue, "enqueue", new_callable=AsyncMock) as named_enqueue:
        named_enqueue.return_value = "queued-id"
        q = ExtractionQueue(mock_agfs, "/queue", "extraction")
        msg1 = ExtractionMsg(
            session_id="sess-1",
            archive_uri="viking://user/default/archive/sess-1",
            account_id="acc",
            user_id="u1",
        )
        msg2 = ExtractionMsg(
            session_id="sess-2",
            archive_uri="viking://user/default/archive/sess-2",
            account_id="acc",
            user_id="u1",
        )
        r1 = await q.enqueue(msg1)
        r2 = await q.enqueue(msg2)
        assert r1 == "queued-id"
        assert r2 == "queued-id"
        assert named_enqueue.call_count == 2


@pytest.mark.asyncio
async def test_extraction_queue_enqueue_allows_after_dedupe_window():
    mock_agfs = MagicMock()
    with patch.object(NamedQueue, "enqueue", new_callable=AsyncMock) as named_enqueue:
        named_enqueue.return_value = "queued-id"
        q = ExtractionQueue(mock_agfs, "/queue", "extraction")
        msg1 = ExtractionMsg(
            session_id="sess-1",
            archive_uri="viking://user/default/archive/sess-1",
            account_id="acc",
            user_id="u1",
        )
        r1 = await q.enqueue(msg1)
        assert r1 == "queued-id"

        key = f"acc|u1|sess-1"
        q._last_enqueue[key] = time.monotonic() - 50.0

        msg2 = ExtractionMsg(
            session_id="sess-1",
            archive_uri="viking://user/default/archive/sess-1",
            account_id="acc",
            user_id="u1",
        )
        r2 = await q.enqueue(msg2)
        assert r2 == "queued-id"
        assert named_enqueue.call_count == 2


@pytest.mark.asyncio
async def test_extraction_queue_dequeue_returns_extraction_msg():
    mock_agfs = MagicMock()
    msg = ExtractionMsg(
        session_id="sess-1",
        archive_uri="viking://user/default/archive/sess-1",
        account_id="acc-1",
        user_id="user-1",
        role="admin",
        telemetry_id="tm-001",
    )
    serialized = msg.to_dict()

    with patch.object(NamedQueue, "dequeue", new_callable=AsyncMock) as named_dequeue:
        named_dequeue.return_value = serialized
        q = ExtractionQueue(mock_agfs, "/queue", "extraction")
        result = await q.dequeue()

        assert result is not None
        assert isinstance(result, ExtractionMsg)
        assert result.id == msg.id
        assert result.session_id == "sess-1"
        assert result.archive_uri == "viking://user/default/archive/sess-1"
        assert result.account_id == "acc-1"
        assert result.user_id == "user-1"
        assert result.role == "admin"
        assert result.telemetry_id == "tm-001"
        assert result.created_at == msg.created_at
