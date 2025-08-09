from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime, timezone

import grpc
# Ensure generated stubs are importable when blog_pb2_grpc does `import blog_pb2`
import sys as _sys
import os as _os
_gen_dir = _os.path.join(_os.path.dirname(__file__), "gen")
if _gen_dir not in _sys.path:
    _sys.path.insert(0, _gen_dir)

from app.common.config import settings
from app.common.redis_client import get_redis
from app.common.db import get_engine, init_db
from app.common.sp_check import verify_stored_procedures
from app.models.blogs import SELECT_BY_ID_SQL, SELECT_LIST_SQL, UPDATE_CONTENT_SQL, DELETE_SQL
from sqlalchemy import text, bindparam
import json as _json

# Generated modules will be placed under app/dataservice/gen
from app.dataservice.gen import blog_pb2, blog_pb2_grpc  # type: ignore

logger = logging.getLogger("dataservice")
logging.basicConfig(level=logging.INFO)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dt_to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _iso_to_dt(s: str | None) -> datetime:
    if not s:
        return datetime.now(timezone.utc)
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


class DataService(blog_pb2_grpc.DataServiceServicer):
    def __init__(self) -> None:
        self.redis = get_redis()
        self.stream_maxlen = int(os.getenv("STREAM_MAXLEN", settings.stream_maxlen))

    @staticmethod
    def _stream_for_genre(genre: str) -> str:
        return f"blogs:genre:{genre}"

    async def EnqueueBlog(self, request: blog_pb2.BlogCreateRequest, context: grpc.aio.ServicerContext) -> blog_pb2.BlogEnqueueResponse:  # type: ignore
        client_msg_id = request.client_msg_id or str(uuid.uuid4())
        genre = (request.genre or "").strip()
        location = (request.location or "").strip()
        author = (request.author or "").strip()
        content = request.content or ""
        created_at_iso = request.created_at_iso or _now_iso()

        if not genre or not location or not author or not content:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "author, content, genre, location are required")

        stream = self._stream_for_genre(genre)

        fields = {
            "client_msg_id": client_msg_id,
            "author": author,
            "content": content,
            "genre": genre,
            "location": location,
            "created_at_iso": created_at_iso,
        }

        await self.redis.sadd("blogs:genres", genre)
        message_id = await self.redis.xadd(stream, fields, maxlen=self.stream_maxlen, approximate=True)
        logger.info("Enqueued blog to %s id %s", stream, message_id)
        return blog_pb2.BlogEnqueueResponse(enqueued=True, stream=stream, message_id=message_id)

    async def GetBlog(self, request: blog_pb2.GetBlogRequest, context: grpc.aio.ServicerContext) -> blog_pb2.GetBlogResponse:  # type: ignore
        engine = get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(text(SELECT_BY_ID_SQL), {"id": request.id})
            row = result.mappings().first()
            if not row:
                await context.abort(grpc.StatusCode.NOT_FOUND, "Not found")
            blog = blog_pb2.Blog(
                id=row["id"],
                client_msg_id=row["client_msg_id"] or "",
                author=row["author"],
                created_at_iso=_dt_to_iso(row["created_at"]),
                updated_at_iso=_dt_to_iso(row["updated_at"]),
                genre=row["genre"],
                location=row["location"],
                content=row["content"],
            )
            return blog_pb2.GetBlogResponse(blog=blog)

    async def ListBlogs(self, request: blog_pb2.ListBlogsRequest, context: grpc.aio.ServicerContext) -> blog_pb2.ListBlogsResponse:  # type: ignore
        engine = get_engine()
        async with engine.connect() as conn:
            params = {
                "author": request.author or None,
                "genre": request.genre or None,
                "location": request.location or None,
                "limit": request.limit or 50,
                "offset": request.offset or 0,
            }
            result = await conn.execute(text(SELECT_LIST_SQL), params)
            blogs = []
            for m in result.mappings().all():
                blogs.append(blog_pb2.Blog(
                    id=m["id"],
                    client_msg_id=m["client_msg_id"] or "",
                    author=m["author"],
                    created_at_iso=_dt_to_iso(m["created_at"]),
                    updated_at_iso=_dt_to_iso(m["updated_at"]),
                    genre=m["genre"],
                    location=m["location"],
                    content=m["content"],
                ))
            return blog_pb2.ListBlogsResponse(blogs=blogs)

    async def UpdateBlog(self, request: blog_pb2.UpdateBlogRequest, context: grpc.aio.ServicerContext) -> blog_pb2.UpdateBlogResponse:  # type: ignore
        updated_at = _iso_to_dt(request.updated_at_iso)
        engine = get_engine()
        async with engine.begin() as conn:
            # CALL sp_update_blog_content(:id, :content, :updated_at)
            result = await conn.execute(
                text("CALL sp_update_blog_content(:id, :content, :updated_at)"),
                {"id": request.id, "content": request.content, "updated_at": updated_at},
            )
            row = result.mappings().first()
            updated = int(row["updated"]) if row and "updated" in row else 0
            if updated == 0:
                await context.abort(grpc.StatusCode.NOT_FOUND, "Not found")
        return blog_pb2.UpdateBlogResponse(updated=True)

    async def DeleteBlog(self, request: blog_pb2.DeleteBlogRequest, context: grpc.aio.ServicerContext) -> blog_pb2.DeleteBlogResponse:  # type: ignore
        engine = get_engine()
        async with engine.begin() as conn:
            # CALL sp_delete_blog(:id)
            result = await conn.execute(text("CALL sp_delete_blog(:id)"), {"id": request.id})
            row = result.mappings().first()
            deleted = int(row["deleted"]) if row and "deleted" in row else 0
            if deleted == 0:
                await context.abort(grpc.StatusCode.NOT_FOUND, "Not found")
        return blog_pb2.DeleteBlogResponse(deleted=True)

    async def BulkDelete(self, request: blog_pb2.BulkDeleteRequest, context: grpc.aio.ServicerContext) -> blog_pb2.BulkDeleteResponse:  # type: ignore
        ids = list(request.ids)
        if not ids:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "ids required")
        ids_json = _json.dumps(ids)
        engine = get_engine()
        async with engine.begin() as conn:
            # CALL returns a result set: SELECT ROW_COUNT() AS deleted
            result = await conn.execute(text("CALL sp_bulk_delete_blogs(:ids_json)"), {"ids_json": ids_json})
            row = result.mappings().first()
            deleted = int(row["deleted"]) if row and "deleted" in row else 0
        return blog_pb2.BulkDeleteResponse(deleted=deleted)

    async def BulkUpdate(self, request: blog_pb2.BulkUpdateRequest, context: grpc.aio.ServicerContext) -> blog_pb2.BulkUpdateResponse:  # type: ignore
        ids = list(request.ids)
        if not ids:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "ids required")
        if not (request.genre or request.location or request.content):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "no fields to update")
        ids_json = _json.dumps(ids)
        engine = get_engine()
        async with engine.begin() as conn:
            result = await conn.execute(
                text("CALL sp_bulk_update_blogs(:ids_json, :genre, :location, :content)"),
                {
                    "ids_json": ids_json,
                    "genre": request.genre or "",
                    "location": request.location or "",
                    "content": request.content or "",
                },
            )
            row = result.mappings().first()
            updated = int(row["updated"]) if row and "updated" in row else 0
        return blog_pb2.BulkUpdateResponse(updated=updated)


async def serve() -> None:
    await init_db()
    # Verify required stored procedures exist
    missing = await verify_stored_procedures({
        "sp_bulk_insert_blogs",
        "sp_bulk_delete_blogs",
        "sp_bulk_update_blogs",
        "sp_update_blog_content",
        "sp_delete_blog",
    })
    if missing:
        logger.error("Startup check: missing stored procedures: %s", ", ".join(sorted(missing)))
    
    server = grpc.aio.server(options=[
        ("grpc.max_send_message_length", 20 * 1024 * 1024),
        ("grpc.max_receive_message_length", 20 * 1024 * 1024),
    ])
    blog_pb2_grpc.add_DataServiceServicer_to_server(DataService(), server)
    listen_addr = f"0.0.0.0:{settings.grpc_port}"
    server.add_insecure_port(listen_addr)
    logger.info("Starting gRPC DataService on %s", listen_addr)
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve()) 