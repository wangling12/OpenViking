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

    _MAX_RETRIES = 5

    def __init__(self):
        config = get_openviking_config()
        breaker_cfg = config.embedding.circuit_breaker
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=breaker_cfg.failure_threshold,
            reset_timeout=breaker_cfg.reset_timeout,
            max_reset_timeout=breaker_cfg.max_reset_timeout,
        )

    async def _reenqueue_extraction_msg(self, msg) -> None:
        from openviking.storage.queuefs.queue_manager import get_queue_manager

        wait = self._circuit_breaker.retry_after
        if wait > 0:
            await asyncio.sleep(wait)

        queue_manager = get_queue_manager()
        if queue_manager is not None:
            extraction_queue = queue_manager.get_queue(queue_manager.EXTRACTION)
            await extraction_queue.enqueue(msg, skip_dedupe=True)
            logger.info("Re-enqueued extraction message: %s", msg.archive_uri)
        else:
            logger.warning("No queue manager available, cannot re-enqueue: %s", msg.archive_uri)

    async def on_dequeue(
        self,
        data: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        from openviking.storage.queuefs.extraction_msg import ExtractionMsg

        if not data:
            return None

        try:
            if "data" in data and isinstance(data["data"], str):
                data = json.loads(data["data"])

            if data is None:
                raise ValueError("Message data is None after parsing")
            msg = ExtractionMsg.from_dict(data)
        except Exception as e:
            logger.error("Failed to parse ExtractionMsg: %s", e, exc_info=True)
            self.report_error(str(e), data)
            return None

        try:
            self._circuit_breaker.check()
        except CircuitBreakerOpen:
            msg.retry_count += 1
            if msg.retry_count > self._MAX_RETRIES:
                logger.error(
                    "Max retries exceeded for extraction %s (retry %d), dropping",
                    msg.archive_uri,
                    msg.retry_count,
                )
                self.report_error("Max retries exceeded", data)
                return None
            logger.warning(
                "Circuit breaker is open, re-enqueueing extraction (retry %d): %s",
                msg.retry_count,
                msg.archive_uri,
            )
            try:
                await self._reenqueue_extraction_msg(msg)
                self.report_requeue()
            except Exception as requeue_err:
                logger.error("Failed to re-enqueue extraction: %s", requeue_err)
                self.report_error(str(requeue_err), data)
            return None

        ctx = RequestContext(
            user=UserIdentifier(account_id=msg.account_id, user_id=msg.user_id),
            role=Role(msg.role or "root"),
        )

        try:
            config = get_openviking_config()
            if not config.memory.extraction_enabled:
                logger.info("Memory extraction disabled, skipping %s", msg.archive_uri)
                self.report_success()
                return None

            messages = await self._read_archive_messages(msg.archive_uri, ctx)
            if not messages:
                logger.info(
                    "No messages in archive %s, skipping extraction", msg.archive_uri
                )
                self.report_success()
                return None

            session_uri = msg.archive_uri.rsplit("/history/", 1)[0]
            messages = await self._hydrate_tool_outputs(messages, session_uri, ctx)

            latest_overview = await self._get_latest_archive_overview(msg, ctx)

            stats = await self._run_extraction(msg, messages, ctx, latest_overview)

            # Write stats to file for session to read
            await self._write_extraction_stats(msg.archive_uri, stats, ctx)

            self.report_success()
            self._circuit_breaker.record_success()
            return None

        except Exception as e:
            msg.retry_count += 1
            if msg.retry_count > self._MAX_RETRIES:
                logger.error(
                    "Max retries exceeded for extraction %s (retry %d), dropping: %s",
                    msg.archive_uri,
                    msg.retry_count,
                    e,
                )
                self.report_error(str(e), data)
                return None
            logger.warning(
                "Extraction error for %s, re-enqueueing (retry %d): %s",
                msg.archive_uri,
                msg.retry_count,
                e,
                exc_info=True,
            )
            self._circuit_breaker.record_failure(e)
            try:
                await self._reenqueue_extraction_msg(msg)
                self.report_requeue()
            except Exception as requeue_err:
                logger.error("Failed to re-enqueue extraction: %s", requeue_err)
                self.report_error(str(requeue_err), data)
            return None

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

    async def _hydrate_tool_outputs(
        self, messages: List[Any], session_uri: str, ctx: RequestContext
    ) -> List[Any]:
        from openviking.message.part import ToolPart
        from openviking.session.tool_result_store import ToolResultStore
        from openviking.storage.viking_fs import get_viking_fs

        viking_fs = get_viking_fs()
        session_id = session_uri.rstrip("/").rsplit("/", 1)[-1]
        store = ToolResultStore(viking_fs, session_uri, session_id, ctx)

        hydrated = []
        for msg in messages:
            msg_dict = msg.to_dict() if hasattr(msg, "to_dict") else dict(msg.__dict__)
            hydrated_msg = type(msg).from_dict(msg_dict)
            for part in hydrated_msg.parts:
                if not isinstance(part, ToolPart):
                    continue
                if not part.tool_output_ref:
                    continue
                if not (part.tool_output_truncated or part.tool_output_source_ref):
                    continue

                ref = part.tool_output_source_ref or part.tool_output_ref
                tool_result_id = ref.rstrip("/").split("/")[-1]
                offset = part.tool_output_source_offset if part.tool_output_source_ref else 0
                limit = part.tool_output_source_limit if part.tool_output_source_ref else -1
                if (
                    part.tool_output_source_ref
                    and limit is None
                    and part.tool_output_original_chars is not None
                ):
                    limit = part.tool_output_original_chars
                try:
                    result = await store.read(
                        tool_result_id,
                        offset=max(0, int(offset or 0)),
                        limit=int(limit) if limit is not None else -1,
                        include_metadata=False,
                    )
                    part.tool_output = result.get("content", "")
                except Exception as exc:
                    logger.warning(
                        "Failed to hydrate tool output for extraction: "
                        "session=%s message_id=%s tool_id=%s ref=%s error=%s",
                        session_id,
                        msg.id,
                        part.tool_id,
                        ref,
                        exc,
                    )
                    continue
            hydrated.append(hydrated_msg)
        return hydrated

    async def _get_latest_archive_overview(
        self, msg: Any, ctx: RequestContext
    ) -> str:
        import re

        from openviking.storage.viking_fs import get_viking_fs

        viking_fs = get_viking_fs()
        archive_uri = msg.archive_uri.rstrip("/")
        parent_uri = archive_uri.rsplit("/", 1)[0] if "/" in archive_uri else ""
        if not parent_uri:
            return ""

        try:
            entries = await viking_fs.ls(parent_uri, ctx=ctx)
        except Exception:
            return ""

        def parse_archive_index(name: str) -> int:
            match = re.search(r"archive_(\d+)$", name)
            return int(match.group(1)) if match else -1

        archive_dirs = sorted(
            [
                e["name"]
                for e in entries
                if e.get("isDir") and e["name"].startswith("archive_")
            ],
            key=parse_archive_index,
            reverse=True,
        )

        for archive_name in archive_dirs[:5]:
            candidate_uri = f"{parent_uri}/{archive_name}"
            if candidate_uri == archive_uri:
                continue
            done_uri = f"{candidate_uri}/.done"
            try:
                await viking_fs.read_file(done_uri, ctx=ctx)
            except Exception:
                continue
            overview_uri = f"{candidate_uri}/.overview.md"
            try:
                overview = await viking_fs.read_file(overview_uri, ctx=ctx)
                if overview:
                    return overview
            except Exception:
                continue
        return ""

    async def _run_extraction(
        self,
        msg: Any,
        messages: List[Any],
        ctx: RequestContext,
        latest_archive_overview: str,
    ) -> Dict[str, Any]:
        from openviking.session import create_session_compressor
        from openviking.session.memory_policy import MemoryPolicy
        from openviking.session.session import (
            _resolve_memory_extraction_scope,
            _split_policy_memory_types,
        )

        config = get_openviking_config()
        effective_policy = MemoryPolicy.from_dict(msg.memory_policy)
        extraction_scope = _resolve_memory_extraction_scope(
            ctx,
            effective_policy,
            messages,
            config_session_skill_extraction_enabled=config.memory.session_skill_extraction_enabled,
        )
        self_memory_enabled = extraction_scope.allow_self_memory
        allowed_peer_ids = extraction_scope.allowed_peer_ids
        memory_type_filter = extraction_scope.memory_types
        long_term_memory_types, execution_memory_types = _split_policy_memory_types(
            memory_type_filter
        )

        long_term_has_work = (
            (self_memory_enabled or allowed_peer_ids)
            and (long_term_memory_types is None or bool(long_term_memory_types))
        )
        execution_memory_has_work = (
            self_memory_enabled
            and (execution_memory_types is None or bool(execution_memory_types))
        )

        compressor = create_session_compressor(vikingdb=None)

        tasks = []
        labels = []

        tasks.append(
            self._run_archive_summary(msg, messages, ctx, latest_archive_overview)
        )
        labels.append("archive_summary")

        if long_term_has_work:
            tasks.append(
                compressor.extract_long_term_memories(
                    messages=messages,
                    user=ctx.user,
                    session_id=msg.session_id,
                    ctx=ctx,
                    latest_archive_overview=latest_archive_overview,
                    archive_uri=msg.archive_uri,
                    allowed_memory_types=long_term_memory_types,
                    allow_self_memory=self_memory_enabled,
                    allowed_peer_ids=allowed_peer_ids,
                )
            )
            labels.append("long_term")

        has_execution_memory = hasattr(compressor, "extract_execution_memories")
        if has_execution_memory and execution_memory_has_work:
            session_skill_extraction_enabled = (
                config.memory.session_skill_extraction_enabled and self_memory_enabled
            )
            tasks.append(
                compressor.extract_execution_memories(
                    messages=messages,
                    ctx=ctx,
                    latest_archive_overview=latest_archive_overview,
                    archive_uri=msg.archive_uri,
                    allowed_memory_types=execution_memory_types,
                    include_session_skills=session_skill_extraction_enabled,
                )
            )
            labels.append("execution")

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect stats
        stats = {
            "memories_extracted": {},
            "session_skills_extracted": 0,
            "session_skill_uris": [],
        }

        extraction_errors: List[BaseException] = []
        for label, res in zip(labels, results):
            if isinstance(res, Exception):
                logger.error("Extraction task %s failed: %s", label, res, exc_info=res)
                self._circuit_breaker.record_failure(res)
                extraction_errors.append(res)
            else:
                if label == "archive_summary":
                    logger.info("Archive summary generated for %s", msg.archive_uri)
                elif isinstance(res, dict):
                    contexts = res.get("contexts", [])
                    stats["memories_extracted"][label] = len(contexts)
                    logger.info("Extracted %d %s memories", len(contexts), label)
                    
                    # Collect session skills if present
                    if "session_skills" in res:
                        session_skills = res["session_skills"]
                        stats["session_skills_extracted"] += len(session_skills)
                        for skill in session_skills:
                            if isinstance(skill, dict):
                                uri = skill.get("uri") or skill.get("root_uri")
                                if uri:
                                    stats["session_skill_uris"].append(uri)
                elif isinstance(res, list):
                    stats["memories_extracted"][label] = len(res)
                    logger.info("Extracted %d %s memories", len(res), label)

        if extraction_errors:
            raise extraction_errors[0]

        return stats

    async def _write_extraction_stats(
        self, archive_uri: str, stats: Dict[str, Any], ctx: RequestContext
    ) -> None:
        from openviking.storage.viking_fs import get_viking_fs

        viking_fs = get_viking_fs()
        stats_uri = f"{archive_uri}/.extraction_stats.json"
        try:
            await viking_fs.write_file(
                stats_uri,
                json.dumps(stats, ensure_ascii=False),
                ctx=ctx,
            )
        except Exception as e:
            logger.warning("Failed to write extraction stats: %s", e)

    async def _run_archive_summary(
        self,
        msg: Any,
        messages: List[Any],
        ctx: RequestContext,
        latest_archive_overview: str,
    ) -> None:
        from openviking.storage.viking_fs import get_viking_fs
        from openviking.utils.token_estimation import estimate_text_tokens

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
            await viking_fs.write_file(
                uri=f"{archive_uri}/.meta.json",
                content=json.dumps(
                    {
                        "overview_tokens": estimate_text_tokens(summary),
                        "abstract_tokens": estimate_text_tokens(abstract),
                    }
                ),
                ctx=ctx,
            )
        except Exception as e:
            logger.warning("Failed to write archive summary files: %s", e)
