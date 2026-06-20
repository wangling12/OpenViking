# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import json
import time

import pytest

from openviking.storage.queuefs.extraction_msg import ExtractionMsg


def test_extraction_msg_init():
    msg = ExtractionMsg(
        session_id="sess-1",
        archive_uri="viking://user/default/archive/sess-1",
        account_id="acc-1",
        user_id="user-1",
        role="admin",
        telemetry_id="tm-123",
    )

    assert len(msg.id) == 36
    assert msg.session_id == "sess-1"
    assert msg.archive_uri == "viking://user/default/archive/sess-1"
    assert msg.account_id == "acc-1"
    assert msg.user_id == "user-1"
    assert msg.role == "admin"
    assert msg.telemetry_id == "tm-123"
    assert msg.retry_count == 0
    assert msg.max_retries == 3
    assert msg.created_at > 0
    assert msg.created_at <= int(time.time())


def test_extraction_msg_to_dict():
    msg = ExtractionMsg(
        session_id="sess-1",
        archive_uri="viking://user/default/archive/sess-1",
        account_id="acc-1",
        user_id="user-1",
    )

    d = msg.to_dict()

    assert isinstance(d, dict)
    assert d["id"] == msg.id
    assert d["session_id"] == "sess-1"
    assert d["archive_uri"] == "viking://user/default/archive/sess-1"
    assert d["account_id"] == "acc-1"
    assert d["user_id"] == "user-1"
    assert d["role"] == "root"
    assert d["telemetry_id"] == ""
    assert d["retry_count"] == 0
    assert d["max_retries"] == 3
    assert d["created_at"] == msg.created_at


def test_extraction_msg_from_dict():
    data = {
        "id": "existing-id-123",
        "session_id": "sess-2",
        "archive_uri": "viking://user/default/archive/sess-2",
        "account_id": "acc-2",
        "user_id": "user-2",
        "role": "editor",
        "telemetry_id": "tm-456",
        "retry_count": 2,
        "max_retries": 5,
        "created_at": 1700000000,
    }

    msg = ExtractionMsg.from_dict(data)

    assert msg.id == "existing-id-123"
    assert msg.session_id == "sess-2"
    assert msg.archive_uri == "viking://user/default/archive/sess-2"
    assert msg.account_id == "acc-2"
    assert msg.user_id == "user-2"
    assert msg.role == "editor"
    assert msg.telemetry_id == "tm-456"
    assert msg.retry_count == 2
    assert msg.max_retries == 5
    assert msg.created_at == 1700000000


def test_extraction_msg_from_dict_with_existing_id():
    original = ExtractionMsg(
        session_id="sess-3",
        archive_uri="viking://user/default/archive/sess-3",
    )
    original_id = original.id

    restored = ExtractionMsg.from_dict(original.to_dict())

    assert restored.id == original_id
    assert restored.session_id == original.session_id
    assert restored.archive_uri == original.archive_uri
    assert restored.created_at == original.created_at


def test_extraction_msg_from_dict_missing_required_fields():
    with pytest.raises(ValueError, match="Data dictionary is empty"):
        ExtractionMsg.from_dict({})

    with pytest.raises(ValueError, match="Missing required fields"):
        ExtractionMsg.from_dict({"session_id": "sess-1"})

    with pytest.raises(ValueError, match="Missing required fields"):
        ExtractionMsg.from_dict({"archive_uri": "viking://archive"})

    with pytest.raises(ValueError, match="Missing required fields"):
        ExtractionMsg.from_dict({"session_id": "", "archive_uri": ""})


def test_extraction_msg_to_json_from_json_roundtrip():
    msg = ExtractionMsg(
        session_id="sess-4",
        archive_uri="viking://user/default/archive/sess-4",
        account_id="acc-4",
        user_id="user-4",
        role="viewer",
        telemetry_id="tm-789",
        retry_count=1,
        max_retries=10,
    )

    json_str = msg.to_json()
    restored = ExtractionMsg.from_json(json_str)

    assert restored.id == msg.id
    assert restored.session_id == msg.session_id
    assert restored.archive_uri == msg.archive_uri
    assert restored.account_id == msg.account_id
    assert restored.user_id == msg.user_id
    assert restored.role == msg.role
    assert restored.telemetry_id == msg.telemetry_id
    assert restored.retry_count == msg.retry_count
    assert restored.max_retries == msg.max_retries
    assert restored.created_at == msg.created_at

    with pytest.raises(ValueError, match="Invalid JSON string"):
        ExtractionMsg.from_json("not valid json{{{")
