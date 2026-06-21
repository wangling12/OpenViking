# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""ExtractionMsg: Memory extraction queue message."""

import json
import time
from typing import Any, Dict, Optional
from uuid import uuid4


class ExtractionMsg:
    """Memory extraction queue message."""

    def __init__(
        self,
        session_id: str,
        archive_uri: str,
        account_id: str = "default",
        user_id: str = "default",
        role: str = "root",
        telemetry_id: str = "",
        retry_count: int = 0,
        memory_policy: Optional[Dict[str, Any]] = None,
    ):
        self.id = str(uuid4())
        self.session_id = session_id
        self.archive_uri = archive_uri
        self.account_id = account_id
        self.user_id = user_id
        self.role = role
        self.telemetry_id = telemetry_id
        self.retry_count = retry_count
        self.memory_policy = memory_policy
        self.created_at = int(time.time())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "session_id": self.session_id,
            "archive_uri": self.archive_uri,
            "account_id": self.account_id,
            "user_id": self.user_id,
            "role": self.role,
            "telemetry_id": self.telemetry_id,
            "retry_count": self.retry_count,
            "memory_policy": self.memory_policy,
            "created_at": self.created_at,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExtractionMsg":
        if not data:
            raise ValueError("Data dictionary is empty")

        session_id = data.get("session_id")
        archive_uri = data.get("archive_uri")

        if not session_id or not archive_uri:
            missing = []
            if not session_id:
                missing.append("session_id")
            if not archive_uri:
                missing.append("archive_uri")
            raise ValueError(f"Missing required fields: {missing}")

        obj = cls(
            session_id=session_id,
            archive_uri=archive_uri,
            account_id=data.get("account_id", "default"),
            user_id=data.get("user_id", "default"),
            role=data.get("role", "root"),
            telemetry_id=data.get("telemetry_id", ""),
            retry_count=data.get("retry_count", 0),
            memory_policy=data.get("memory_policy"),
        )
        if "id" in data and data["id"]:
            obj.id = data["id"]
        if "created_at" in data:
            obj.created_at = data["created_at"]
        return obj

    @classmethod
    def from_json(cls, json_str: str) -> "ExtractionMsg":
        try:
            data = json.loads(json_str)
            return cls.from_dict(data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON string: {e}")
