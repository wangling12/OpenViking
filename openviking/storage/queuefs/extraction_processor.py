# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""ExtractionProcessor: Dequeue handler for memory extraction."""

import asyncio
import json
from typing import Any, Dict, List, Optional

from openviking.server.identity import RequestContext, Role
from openviking.storage.queuefs.named_queue import DequeueHandlerBase
from openviking.utils.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpen,
)
from openviking_cli.session.user_id import UserIdentifier
from openviking_cli.utils.config import get_openviking_config
from openviking_cli.utils.logger import get_logger

logger = get_logger(__name__)


class ExtractionProcessor(DequeueHandlerBase):
    """Processes ExtractionMsg: runs memory extraction with rate limiting."""

    def __init__(self, max_concurrent_llm: int = 10):
        config = get_openviking_config()
        breaker_cfg = config.embedding.circuit_breaker
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=breaker_cfg.failure_threshold,
            reset_timeout=breaker_cfg.reset_timeout,
            max_reset_timeout=breaker_cfg.max_reset_timeout,
        )
        self._max_concurrent_llm = max_concurrent_llm

    async def on_dequeue(
        self,
        data: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """Process dequeued ExtractionMsg: run memory extraction."""
        from openviking.storage.queuefs.extraction_msg import ExtractionMsg

        if not data:
            return None

        try:
            if "data" in data and isinstance(data["data"], str):
                data = json.loads(data["data"])

            assert data is not None
            msg = ExtractionMsg.from_dict(data)
        except Exception as e:
            logger.error("Failed to parse ExtractionMsg: %s", e, exc_info=True)
            self.report_error(str(e), data)
            return None

        try:
            self._circuit_breaker.check()
        except CircuitBreakerOpen:
            msg.retry_count += 1
            logger.warning(
                "Circuit breaker is open, re-enqueueing extraction message "
                "(retry %d): %s",
                msg.retry_count,
                msg.archive_uri,
            )
            self.report_requeue()
            return msg.to_dict()

        ctx = RequestContext(
            user=UserIdentifier(account_id=msg.account_id, user_id=msg.user_id),
            role=Role(msg.role or "root"),
        )

        try:
            messages = await self._read_archive_messages(msg.archive_uri, ctx)
            if not messages:
                logger.info(
                    "No messages in archive %s, skipping extraction", msg.archive_uri
                )
                self.report_success()
                return None

            latest_overview = await self._get_latest_archive_overview(msg, ctx)

            result = await self._run_extraction(msg, messages, ctx, latest_overview)

            labels = result["labels"]
            results = result["results"]

            extraction_errors: List[BaseException] = []
            for label, res in zip(labels, results):
                if isinstance(res, Exception):
                    logger.error(
                        "Extraction task %s failed: %s", label, res, exc_info=res
                    )
                    self._circuit_breaker.record_failure(res)
                    extraction_errors.append(res)
                else:
                    if label == "archive_summary":
                        logger.info("Archive summary generated for %s", msg.archive_uri)
                    elif isinstance(res, dict):
                        count = len(res.get("contexts", []))
                        logger.info("Extracted %d %s memories", count, label)
                    elif isinstance(res, list):
                        logger.info("Extracted %d %s memories", len(res), label)

            if extraction_errors:
                self._circuit_breaker.record_failure(extraction_errors[0])
                msg.retry_count += 1
                logger.warning(
                    "Extraction failed, re-enqueueing (retry %d): %s",
                    msg.retry_count,
                    msg.archive_uri,
                )
                self.report_requeue()
                return msg.to_dict()

            self.report_success()
            self._circuit_breaker.record_success()
            return None

        except Exception as e:
            msg.retry_count += 1
            logger.warning(
                "Extraction error for %s, re-enqueueing (retry %d): %s",
                msg.archive_uri,
                msg.retry_count,
                e,
                exc_info=True,
            )
            self._circuit_breaker.record_failure(e)
            self.report_requeue()
            return msg.to_dict()

    async def _read_archive_messages(
        self, archive_uri: str, ctx: RequestContext
    ) -> List[Any]:
        from openviking.message import Message
        from openviking.storage.viking_fs import get_viking_fs

        viking_fs = get_viking_fs()
        messages_uri = f"{archive_uri}/messages.jsonl"
        agfs_path = viking_fs._uri_to_path(messages_uri, ctx=ctx)

        messages: List[Message] = []
        try:
            content = await viking_fs._async_agfs.cat(agfs_path)
            if isinstance(content, bytes):
                content = content.decode("utf-8")
            for line in content.strip().split("\n"):
                if line.strip():
                    try:
                        messages.append(Message.from_dict(json.loads(line)))
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(
                "Cannot read archive for extraction: %s: %s", agfs_path, e
            )
        return messages

    async def _get_latest_archive_overview(
        self, msg: Any, ctx: RequestContext
    ) -> str:
        from openviking.storage.viking_fs import get_viking_fs

        viking_fs = get_viking_fs()
        archive_uri = msg.archive_uri
        parent_uri = archive_uri.rsplit("/", 1)[0] if "/" in archive_uri else ""
        if not parent_uri:
            return ""

        try:
            entries = await viking_fs.ls(parent_uri, ctx=ctx)
        except Exception:
            return ""

        archive_dirs = sorted(
            [
                e["name"]
                for e in entries
                if e.get("isDir")
                and e["name"] != msg.session_id
            ],
            reverse=True,
        )

        for archive_name in archive_dirs[:5]:
            overview_uri = f"{parent_uri}/{archive_name}/.overview.md"
            try:
                overview = await viking_fs.read_file(overview_uri, ctx=ctx)
                if overview:
                    return overview
            except Exception:
                continue
        return ""

    async def _run_archive_summary(
        self,
        msg: Any,
        messages: List[Any],
        ctx: RequestContext,
        latest_archive_overview: str,
    ) -> None:
        from openviking.storage.viking_fs import get_viking_fs

        if not messages:
            return

        viking_fs = get_viking_fs()
        archive_uri = msg.archive_uri

        formatted_lines = []
        for m in messages:
            role = getattr(m, "role", "unknown")
            content = getattr(m, "content", "")
            if isinstance(content, list):
                text_parts = []
                for part in content:
                    if isinstance(part, dict) and part.get("type") == "text":
                        text_parts.append(part.get("text", ""))
                    elif isinstance(part, str):
                        text_parts.append(part)
                content = "\n".join(text_parts)
            formatted_lines.append(f"[{role}]: {content}")
        formatted = "\n".join(formatted_lines)

        summary = ""
        config = get_openviking_config()
        vlm = config.vlm
        if vlm and vlm.is_available():
            try:
                from openviking.prompts import render_prompt

                prompt = render_prompt(
                    "compression.structured_summary",
                    {
                        "messages": formatted,
                        "latest_archive_overview": latest_archive_overview,
                    },
                )
                summary = await vlm.get_completion_async(prompt)
            except Exception as e:
                logger.warning("LLM summary generation failed: %s", e)

        if not summary:
            turn_count = len([m for m in messages if getattr(m, "role", "") == "user"])
            summary = (
                f"# Session Summary\n\n"
                f"**Overview**: {turn_count} turns, {len(messages)} messages"
            )

        abstract = summary.split("\n")[0].lstrip("# ").strip()
        if len(abstract) > 200:
            abstract = abstract[:200]

        try:
            await viking_fs.write_file(
                uri=f"{archive_uri}/.abstract.md",
                content=abstract,
                ctx=ctx,
            )
            await viking_fs.write_file(
                uri=f"{archive_uri}/.overview.md",
                content=summary,
                ctx=ctx,
            )
        except Exception as e:
            logger.warning("Failed to write archive summary files: %s", e)

    async def _run_extraction(
        self,
        msg: Any,
        messages: List[Any],
        ctx: RequestContext,
        latest_archive_overview: str,
    ) -> Dict[str, Any]:
        from openviking.session import create_session_compressor

        compressor = create_session_compressor(vikingdb=None)

        tasks = []
        labels = []

        tasks.append(
            self._run_archive_summary(msg, messages, ctx, latest_archive_overview)
        )
        labels.append("archive_summary")

        tasks.append(
            compressor.extract_long_term_memories(
                messages=messages,
                user=ctx.user,
                session_id=msg.session_id,
                ctx=ctx,
                latest_archive_overview=latest_archive_overview,
                archive_uri=msg.archive_uri,
            )
        )
        labels.append("long_term")

        if hasattr(compressor, "extract_execution_memories"):
            tasks.append(
                compressor.extract_execution_memories(
                    messages=messages,
                    ctx=ctx,
                    latest_archive_overview=latest_archive_overview,
                    archive_uri=msg.archive_uri,
                )
            )
            labels.append("execution")

        results = await asyncio.gather(*tasks, return_exceptions=True)

        return {"labels": labels, "results": results}
