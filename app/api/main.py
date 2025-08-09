from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import grpc
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware

# Ensure generated stubs are importable when blog_pb2_grpc does `import blog_pb2`
import sys as _sys
import os as _os
_gen_dir = _os.path.join(_os.path.dirname(__file__), "../dataservice/gen")
_gen_dir = _os.path.normpath(_gen_dir)
if _gen_dir not in _sys.path:
    _sys.path.insert(0, _gen_dir)

from app.common.config import settings

# gRPC stubs are generated at runtime into this package
from app.dataservice.gen import blog_pb2, blog_pb2_grpc  # type: ignore

app = FastAPI(title="Blogs API", version="0.4.0")

# CORS for separate frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class BlogCreate(BaseModel):
    author: str = Field(min_length=1, max_length=128)
    content: str = Field(min_length=1)
    genre: str = Field(min_length=1, max_length=64)
    location: str = Field(min_length=1, max_length=128)
    created_at_iso: Optional[str] = None
    client_msg_id: Optional[str] = None


class BlogOut(BaseModel):
    id: int
    client_msg_id: Optional[str]
    author: str
    created_at_iso: str
    updated_at_iso: str
    genre: str
    location: str
    content: str


class BulkDeleteIn(BaseModel):
    ids: list[int] = Field(min_length=1)


class BulkUpdateSet(BaseModel):
    genre: Optional[str] = None
    location: Optional[str] = None
    content: Optional[str] = None


class BulkUpdateIn(BaseModel):
    ids: list[int] = Field(min_length=1)
    set: BulkUpdateSet


async def _grpc_stub() -> blog_pb2_grpc.DataServiceStub:
    channel = grpc.aio.insecure_channel(f"{settings.dataservice_host}:{settings.dataservice_port}")
    return blog_pb2_grpc.DataServiceStub(channel)


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.post("/blogs", response_model=dict)
async def create_blog(req: BlogCreate, sync: bool = Query(default=False)) -> dict:
    stub = await _grpc_stub()
    try:
        if sync:
            resp = await stub.CreateBlogSync(blog_pb2.BlogCreateSyncRequest(
                client_msg_id=req.client_msg_id or "",
                author=req.author,
                content=req.content,
                genre=req.genre,
                location=req.location,
                created_at_iso=req.created_at_iso or "",
            ))
            return {"status": "created", "id": resp.id, "stream": resp.stream, "message_id": resp.message_id}
        else:
            resp = await stub.EnqueueBlog(blog_pb2.BlogCreateRequest(
                client_msg_id=req.client_msg_id or "",
                author=req.author,
                content=req.content,
                genre=req.genre,
                location=req.location,
                created_at_iso=req.created_at_iso or "",
            ))
            return {"status": "enqueued", "stream": resp.stream, "message_id": resp.message_id}
    except grpc.aio.AioRpcError as e:  # type: ignore
        raise HTTPException(status_code=400, detail=e.details())


@app.get("/blogs/{blog_id}", response_model=BlogOut)
async def get_blog(blog_id: int) -> BlogOut:
    stub = await _grpc_stub()
    try:
        resp = await stub.GetBlog(blog_pb2.GetBlogRequest(id=blog_id))
    except grpc.aio.AioRpcError as e:  # type: ignore
        if e.code() == grpc.StatusCode.NOT_FOUND:  # type: ignore
            raise HTTPException(status_code=404, detail="Not found")
        raise HTTPException(status_code=500, detail=e.details())
    b = resp.blog
    return BlogOut(
        id=b.id,
        client_msg_id=b.client_msg_id or None,
        author=b.author,
        created_at_iso=b.created_at_iso,
        updated_at_iso=b.updated_at_iso,
        genre=b.genre,
        location=b.location,
        content=b.content,
    )


@app.get("/blogs", response_model=list[BlogOut])
async def list_blogs(
    author: Optional[str] = Query(default=None),
    genre: Optional[str] = Query(default=None),
    location: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[BlogOut]:
    stub = await _grpc_stub()
    try:
        resp = await stub.ListBlogs(blog_pb2.ListBlogsRequest(
            author=author or "",
            genre=genre or "",
            location=location or "",
            limit=limit,
            offset=offset,
        ))
    except grpc.aio.AioRpcError as e:  # type: ignore
        raise HTTPException(status_code=500, detail=e.details())
    out: list[BlogOut] = []
    for b in resp.blogs:
        out.append(BlogOut(
            id=b.id,
            client_msg_id=b.client_msg_id or None,
            author=b.author,
            created_at_iso=b.created_at_iso,
            updated_at_iso=b.updated_at_iso,
            genre=b.genre,
            location=b.location,
            content=b.content,
        ))
    return out


class BlogUpdate(BaseModel):
    content: str = Field(min_length=1)
    updated_at_iso: Optional[str] = None


@app.put("/blogs/{blog_id}", response_model=dict)
async def update_blog(blog_id: int, req: BlogUpdate) -> dict:
    stub = await _grpc_stub()
    try:
        resp = await stub.UpdateBlog(blog_pb2.UpdateBlogRequest(id=blog_id, content=req.content, updated_at_iso=req.updated_at_iso or ""))
    except grpc.aio.AioRpcError as e:  # type: ignore
        if e.code() == grpc.StatusCode.NOT_FOUND:  # type: ignore
            raise HTTPException(status_code=404, detail="Not found")
        raise HTTPException(status_code=500, detail=e.details())
    return {"status": "updated" if resp.updated else "noop", "id": blog_id}


@app.delete("/blogs/{blog_id}", response_model=dict)
async def delete_blog(blog_id: int) -> dict:
    stub = await _grpc_stub()
    try:
        resp = await stub.DeleteBlog(blog_pb2.DeleteBlogRequest(id=blog_id))
    except grpc.aio.AioRpcError as e:  # type: ignore
        if e.code() == grpc.StatusCode.NOT_FOUND:  # type: ignore
            raise HTTPException(status_code=404, detail="Not found")
        raise HTTPException(status_code=500, detail=e.details())
    return {"status": "deleted" if resp.deleted else "noop", "id": blog_id}


@app.post("/blogs/bulk-delete", response_model=dict)
async def bulk_delete(req: BulkDeleteIn) -> dict:
    stub = await _grpc_stub()
    if not req.ids:
        raise HTTPException(status_code=400, detail="ids required")
    try:
        resp = await stub.BulkDelete(blog_pb2.BulkDeleteRequest(ids=req.ids))
    except grpc.aio.AioRpcError as e:  # type: ignore
        raise HTTPException(status_code=500, detail=e.details())
    return {"deleted": resp.deleted}


@app.post("/blogs/bulk-update", response_model=dict)
async def bulk_update(req: BulkUpdateIn) -> dict:
    stub = await _grpc_stub()
    if not req.ids:
        raise HTTPException(status_code=400, detail="ids required")
    set = req.set or BulkUpdateSet()
    if not any([set.genre, set.location, set.content]):
        raise HTTPException(status_code=400, detail="no fields to update")
    try:
        resp = await stub.BulkUpdate(blog_pb2.BulkUpdateRequest(ids=req.ids, genre=set.genre or "", location=set.location or "", content=set.content or ""))
    except grpc.aio.AioRpcError as e:  # type: ignore
        raise HTTPException(status_code=500, detail=e.details())
    return {"updated": resp.updated} 