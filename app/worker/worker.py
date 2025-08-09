from __future__ import annotations

import asyncio
import logging
import os
import socket
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple

import json as _json
import redis.asyncio as redis

from app.common.config import settings
from app.common.db import get_engine, init_db
# from app.models.blogs import BlogRow, bulk_insert
from app.models.blogs import BlogRow
from sqlalchemy import text

logger = logging.getLogger("worker")
logging.basicConfig(level=logging.INFO)


@dataclass
class BufferedItem:
    row: BlogRow
    redis_stream: str
    redis_message_id: str


class BlogWorker:
    def __init__(self) -> None:
        self.redis: redis.Redis = redis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)
        self.consumer_group = os.getenv("CONSUMER_GROUP", settings.consumer_group)
        self.consumer_name = os.getenv("CONSUMER_NAME", f"{socket.gethostname()}-{os.getpid()}")
        self.batch_max_count = int(os.getenv("BATCH_MAX_COUNT", settings.batch_max_count))
        self.batch_max_age_ms = int(os.getenv("BATCH_MAX_AGE_MS", settings.batch_max_age_ms))
        self.batch_max_bytes = int(os.getenv("BATCH_MAX_BYTES", settings.batch_max_bytes))

        self.buffers: Dict[Tuple[str, str], List[BufferedItem]] = defaultdict(list)  # key: (genre, location)
        self.buffer_first_at: Dict[Tuple[str, str], datetime] = {}
        self.buffer_bytes: Dict[Tuple[str, str], int] = defaultdict(int)

    @staticmethod
    def stream_for_genre(genre: str) -> str:
        return f"blogs:genre:{genre}"

    async def ensure_groups(self, streams: list[str]) -> None:
        for s in streams:
            try:
                # Start from the beginning to avoid missing the first message if the group is created after initial XADD
                await self.redis.xgroup_create(name=s, groupname=self.consumer_group, id="0", mkstream=True)
                logger.info("Created consumer group %s on %s starting at 0", self.consumer_group, s)
            except redis.ResponseError as e:  # type: ignore
                if "BUSYGROUP" in str(e):
                    continue
                if "NOGROUP" in str(e):
                    continue
                # For already exists or race, ignore
                if "exists" in str(e).lower():
                    continue
                logger.warning("xgroup_create error on %s: %s", s, e)

    async def discover_streams(self) -> list[str]:
        genres = await self.redis.smembers("blogs:genres")
        return [self.stream_for_genre(g) for g in sorted(genres)]

    def _add_to_buffer(self, stream: str, mid: str, fields: dict) -> None:
        genre = fields.get("genre", "")
        location = fields.get("location", "")
        author = fields.get("author", "")
        content = fields.get("content", "")
        client_msg_id = fields.get("client_msg_id")
        created_at_iso = fields.get("created_at_iso")
        try:
            created_at = datetime.fromisoformat(created_at_iso)
        except Exception:
            created_at = datetime.now(timezone.utc)
        now = datetime.now(timezone.utc)
        row = BlogRow(
            client_msg_id=client_msg_id,
            author=author,
            created_at=created_at,
            updated_at=now,
            genre=genre,
            location=location,
            content=content,
        )
        key = (genre, location)
        self.buffers[key].append(BufferedItem(row=row, redis_stream=stream, redis_message_id=mid))
        if key not in self.buffer_first_at:
            self.buffer_first_at[key] = now
        approx_bytes = len(content) + len(author) + len(location) + len(genre) + 64
        self.buffer_bytes[key] += approx_bytes

    def _should_flush(self, key: Tuple[str, str]) -> bool:
        items = self.buffers[key]
        if not items:
            return False
        if len(items) >= self.batch_max_count:
            return True
        first_at = self.buffer_first_at.get(key)
        if first_at and (datetime.now(timezone.utc) - first_at) >= timedelta(milliseconds=self.batch_max_age_ms):
            return True
        if self.buffer_bytes.get(key, 0) >= self.batch_max_bytes:
            return True
        return False

    @staticmethod
    def _format_dt(dt: datetime) -> str:
        # MySQL DATETIME(6) string format
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    async def _flush_key(self, key: Tuple[str, str]) -> None:
        items = self.buffers.get(key, [])
        if not items:
            return
        # Build JSON array for stored procedure
        rows_json_list = []
        for bi in items:
            r = bi.row
            rows_json_list.append({
                "client_msg_id": r.client_msg_id or "",
                "author": r.author,
                "created_at": self._format_dt(r.created_at),
                "updated_at": self._format_dt(r.updated_at),
                "genre": r.genre,
                "location": r.location,
                "content": r.content,
            })
        rows_json = _json.dumps(rows_json_list, ensure_ascii=False)

        engine = get_engine()
        async with engine.begin() as conn:
            await conn.execute(text("CALL sp_bulk_insert_blogs(:rows_json)"), {"rows_json": rows_json})
        # ack messages
        stream_to_mids: Dict[str, list[str]] = defaultdict(list)
        for bi in items:
            stream_to_mids[bi.redis_stream].append(bi.redis_message_id)
        for stream, mids in stream_to_mids.items():
            try:
                await self.redis.xack(stream, self.consumer_group, *mids)
                # Optional delete to trim memory faster
                await self.redis.xdel(stream, *mids)
            except Exception as e:  # noqa: BLE001
                logger.warning("Failed to ack/del on %s: %s", stream, e)
        # reset buffer
        self.buffers[key].clear()
        self.buffer_first_at.pop(key, None)
        self.buffer_bytes[key] = 0

    async def run(self) -> None:
        await init_db()
        while True:
            try:
                streams = await self.discover_streams()
                if not streams:
                    await asyncio.sleep(0.5)
                    continue
                await self.ensure_groups(streams)
                # IDs array of '>' of equal length
                ids = [">" for _ in streams]
                resp = await self.redis.xreadgroup(
                    groupname=self.consumer_group,
                    consumername=self.consumer_name,
                    streams=dict(zip(streams, ids)),
                    count=self.batch_max_count,
                    block=1000,
                )
                # resp: list of (stream, [(id, {fields})...])
                if resp:
                    for stream, messages in resp:
                        for mid, fields in messages:
                            self._add_to_buffer(stream, mid, fields)
                # Flush as needed
                keys_to_flush = [k for k in list(self.buffers.keys()) if self._should_flush(k)]
                for key in keys_to_flush:
                    await self._flush_key(key)
            except Exception as e:  # noqa: BLE001
                logger.exception("Worker loop error: %s", e)
                await asyncio.sleep(1.0)


async def main() -> None:
    worker = BlogWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main()) 