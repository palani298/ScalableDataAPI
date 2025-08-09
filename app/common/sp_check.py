from __future__ import annotations

import logging
from typing import Iterable

from sqlalchemy import text, bindparam
from sqlalchemy.ext.asyncio import AsyncEngine

from .config import settings
from .db import get_engine

logger = logging.getLogger("sp_check")


async def verify_stored_procedures(required_names: Iterable[str], engine: AsyncEngine | None = None) -> set[str]:
    """Return the set of missing stored procedure names in current database.

    Logs an error for any missing names. Does not raise.
    """
    names = list(required_names)
    if not names:
        return set()
    engine = engine or get_engine()
    stmt = text(
        """
        SELECT ROUTINE_NAME
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_SCHEMA = :db
          AND ROUTINE_TYPE = 'PROCEDURE'
          AND ROUTINE_NAME IN :names
        """
    ).bindparams(bindparam("names", expanding=True))
    present: set[str] = set()
    async with engine.connect() as conn:
        result = await conn.execute(stmt, {"db": settings.mysql_db, "names": names})
        for row in result:  # type: ignore[assignment]
            present.add(row[0])
    missing = set(names) - present
    if missing:
        logger.error("Missing stored procedures in schema '%s': %s", settings.mysql_db, ", ".join(sorted(missing)))
    else:
        logger.info("All required stored procedures are present: %s", ", ".join(sorted(names)))
    return missing 