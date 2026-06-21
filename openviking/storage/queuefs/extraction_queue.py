# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""ExtractionQueue: Memory extraction queue."""

import asyncio
import time

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
        self._dedupe_lock = asyncio.Lock()

    @staticmethod
    def _dedupe_key(msg: ExtractionMsg) -> str:
        return f"{msg.account_id}|{msg.user_id}|{msg.session_id}"

    async def enqueue(self, msg: ExtractionMsg, skip_dedupe: bool = False) -> str:
        """Serialize ExtractionMsg object and store in queue.
        
        Args:
            msg: The extraction message to enqueue
            skip_dedupe: If True, bypass deduplication (used for retries)
        """
        if not skip_dedupe:
            key = self._dedupe_key(msg)
            now = time.monotonic()
            async with self._dedupe_lock:
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
