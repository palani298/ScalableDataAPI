from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy import text


@dataclass
class BlogRow:
    client_msg_id: str | None
    author: str
    created_at: datetime
    updated_at: datetime
    genre: str
    location: str
    content: str


INSERT_SQL = """
INSERT INTO blogs (client_msg_id, author, created_at, updated_at, genre, location, content)
VALUES {values_clause}
ON DUPLICATE KEY UPDATE
  updated_at = VALUES(updated_at)
"""


def _values_clause(num_rows: int) -> str:
    placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s)" for _ in range(num_rows)])
    return placeholders


async def bulk_insert(conn: AsyncConnection, rows: Iterable[BlogRow]) -> int:
    rows_list = list(rows)
    if not rows_list:
        return 0
    values = []
    for r in rows_list:
        values.extend([
            r.client_msg_id,
            r.author,
            r.created_at,
            r.updated_at,
            r.genre,
            r.location,
            r.content,
        ])
    sql = INSERT_SQL.format(values_clause=_values_clause(len(rows_list)))
    # Pass a tuple of scalars so SQLAlchemy treats this as a single paramset, not executemany
    result = await conn.exec_driver_sql(sql, tuple(values))
    return result.rowcount or 0


SELECT_BY_ID_SQL = """
SELECT id, client_msg_id, author, created_at, updated_at, genre, location, content
FROM blogs WHERE id = :id
"""

SELECT_LIST_SQL = """
SELECT id, client_msg_id, author, created_at, updated_at, genre, location, content
FROM blogs
WHERE (:author IS NULL OR author = :author)
  AND (:genre IS NULL OR genre = :genre)
  AND (:location IS NULL OR location = :location)
ORDER BY created_at DESC
LIMIT :limit OFFSET :offset
"""

UPDATE_CONTENT_SQL = """
UPDATE blogs SET content = :content, updated_at = :updated_at WHERE id = :id
"""

DELETE_SQL = """
DELETE FROM blogs WHERE id = :id
""" 