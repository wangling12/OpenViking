# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""ExtractionQueue: Memory extraction queue."""

import threading
import time
from typing import Optional

from openviking_cli.utils.logger import get_logger

from .extraction_msg import ExtractionMsg
from .named_queue import NamedQueue

logger = get_logger(__name__)

_EXTRACTION_DEDUPE_SEC = 45.0


class ExtractionQueue(NamedQueue):
    """Memory extraction queue for async extraction of session memories."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._last_enqueue: dict[str, float] = {}
        self._dedupe_lock = threading.Lock()

    @staticmethod
    def _dedupe_key(msg: ExtractionMsg) -> str:
        return f"{msg.account_id}|{msg.user_id}|{msg.session_id}"

    async def enqueue(self, msg: ExtractionMsg) -> str:
        """Serialize ExtractionMsg object and store in queue."""
        key = self._dedupe_key(msg)
        now = time.monotonic()
        with self._dedupe_lock:
            last = self._last_enqueue.get(key, 0.0)
            if now - last < _EXTRACTION_DEDUPE_SEC:
                logger.debug(
                    "[ExtractionQueue] Skipping duplicate extraction for session %s",
                    msg.session_id,
                )
                return "deduplicated"
            self._last_enqueue[key] = now
            if len(self._last_enqueue) > 2000:
                cutoff = now - (_EXTRACTION_DEDUPE_SEC * 4)
                stale = [k for k, t in self._last_enqueue.items() if t < cutoff]
                for k in stale[:800]:
                    self._last_enqueue.pop(k, None)

        return await super().enqueue(msg.to_dict())

    async def dequeue(self) -> Optional[ExtractionMsg]:
        """Get message from queue and deserialize to ExtractionMsg object."""
        data_dict = await super().dequeue()
        if not data_dict:
            return None

        if "data" in data_dict and isinstance(data_dict["data"], str):
            try:
                return ExtractionMsg.from_json(data_dict["data"])
            except Exception as e:
                logger.debug(f"[ExtractionQueue] Failed to parse message data: {e}")
                return None

        try:
            return ExtractionMsg.from_dict(data_dict)
        except Exception as e:
            logger.debug(f"[ExtractionQueue] Failed to create ExtractionMsg from dict: {e}")
            return None

    async def peek(self) -> Optional[ExtractionMsg]:
        """Peek at message from queue."""
        data_dict = await super().peek()
        if not data_dict:
            return None

        if "data" in data_dict and isinstance(data_dict["data"], str):
            try:
                return ExtractionMsg.from_json(data_dict["data"])
            except Exception:
                return None

        try:
            return ExtractionMsg.from_dict(data_dict)
        except Exception:
            return None
