"""
Database Connection and Engine Management.
Provides SQLAlchemy connection pooling for PostgreSQL with automatic local SQLite fallback.
"""

import os
from pathlib import Path
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from forgestream.config import settings
from forgestream.observability.logging import get_logger

logger = get_logger("postgres.connection")


class DatabaseManager:
    """Manages relational database connection lifecycle and schema initialization."""

    def __init__(
        self,
        dsn: Optional[str] = None,
        sqlite_fallback_path: Optional[str] = None,
        force_sqlite: bool = False,
    ):
        self.dsn = dsn or settings.postgres.dsn
        self.sqlite_path = sqlite_fallback_path or settings.postgres.sqlite_db_path
        self.force_sqlite = force_sqlite
        self._engine: Optional[Engine] = None
        self._is_postgres = False

        self._init_engine()

    def _init_engine(self) -> None:
        """Attempts connection to PostgreSQL, falling back to SQLite if unreachable or forced."""
        if not self.force_sqlite:
            # Try PostgreSQL first
            try:
                pg_engine = create_engine(
                    self.dsn,
                    pool_pre_ping=True,
                    connect_args={"connect_timeout": 2},
                )
                # Test ping
                with pg_engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                self._engine = pg_engine
                self._is_postgres = True
                logger.info("Connected to PostgreSQL operational metadata database")
                return
            except Exception as e:
                logger.warning(f"PostgreSQL connection failed ({e}). Falling back to local SQLite metadata store.")

        # Fallback to local SQLite database
        db_file = Path(self.sqlite_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)
        sqlite_uri = f"sqlite:///{db_file.as_posix()}"
        self._engine = create_engine(sqlite_uri)
        self._is_postgres = False
        logger.info(f"Initialized SQLite operational metadata store at: {sqlite_uri}")

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._init_engine()
        return self._engine

    @property
    def is_postgres(self) -> bool:
        return self._is_postgres

    def init_schema(self) -> None:
        """Executes DDL schema initialization."""
        schema_file = Path(__file__).resolve().parent / "schema.sql"
        if not schema_file.exists():
            schema_file = Path(settings.project_root) / "forgestream" / "postgres" / "schema.sql"

        ddl_content = schema_file.read_text(encoding="utf-8")

        # SQLite compatibility adjustments for DDL if running on SQLite
        if not self._is_postgres:
            # Replace PostgreSQL specific types for SQLite
            ddl_content = ddl_content.replace("DOUBLE PRECISION", "REAL")
            ddl_content = ddl_content.replace("TIMESTAMP WITH TIME ZONE", "TEXT")
            ddl_content = ddl_content.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")

        with self.engine.begin() as conn:
            # Split and execute individual statements
            statements = [s.strip() for s in ddl_content.split(";") if s.strip()]
            for stmt in statements:
                conn.execute(text(stmt))

        logger.info("Database schema initialized successfully")
