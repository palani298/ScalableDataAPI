from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncConnection
from sqlalchemy import text
from .config import settings


_engine: AsyncEngine | None = None


def get_mysql_dsn() -> str:
    return (
        f"mysql+asyncmy://{settings.mysql_user}:{settings.mysql_password}"
        f"@{settings.mysql_host}:{settings.mysql_port}/{settings.mysql_db}?charset=utf8mb4"
    )


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            get_mysql_dsn(),
            pool_size=settings.mysql_pool_size,
            max_overflow=settings.mysql_max_overflow,
            pool_pre_ping=True,
        )
    return _engine


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS blogs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  client_msg_id CHAR(36) NULL,
  author VARCHAR(128) NOT NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  genre VARCHAR(64) NOT NULL,
  location VARCHAR(128) NOT NULL,
  content MEDIUMTEXT NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_client_msg_id (client_msg_id),
  KEY idx_author_created_at (author, created_at),
  KEY idx_genre_created_at (genre, created_at),
  KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""


async def init_db() -> None:
    engine = get_engine()
    # Retry startup until MySQL is ready
    import asyncio
    for _ in range(30):
        try:
            async with engine.begin() as conn:  # type: AsyncConnection
                await conn.execute(text(CREATE_TABLE_SQL))
            return
        except Exception:  # noqa: BLE001
            await asyncio.sleep(1)
    # last attempt raises
    async with engine.begin() as conn:  # type: AsyncConnection
        await conn.execute(text(CREATE_TABLE_SQL)) 