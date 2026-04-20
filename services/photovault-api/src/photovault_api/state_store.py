"""Persistence backends for upload dedup and temporary upload content."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from threading import Lock
from typing import Protocol


class UploadStateStore(Protocol):
    def initialize(self) -> None: ...

    def has_sha(self, sha256_hex: str) -> bool: ...

    def has_shas(self, sha256_hex_values: list[str]) -> set[str]: ...

    def upsert_temp_upload(self, sha256_hex: str, size_bytes: int, content: bytes) -> None: ...

    def get_temp_upload(self, sha256_hex: str) -> tuple[int, bytes] | None: ...

    def mark_sha_verified(self, sha256_hex: str) -> None: ...

    def remove_temp_upload(self, sha256_hex: str) -> None: ...


@dataclass
class InMemoryUploadStateStore:
    """In-memory store used for local tests and fallback development."""

    known_sha256: set[str] = field(default_factory=set)
    upload_temp: dict[str, tuple[int, bytes]] = field(default_factory=dict)
    _lock: Lock = field(default_factory=Lock)

    def initialize(self) -> None:
        return

    def has_sha(self, sha256_hex: str) -> bool:
        with self._lock:
            return sha256_hex in self.known_sha256

    def has_shas(self, sha256_hex_values: list[str]) -> set[str]:
        with self._lock:
            return {sha256_hex for sha256_hex in sha256_hex_values if sha256_hex in self.known_sha256}

    def upsert_temp_upload(self, sha256_hex: str, size_bytes: int, content: bytes) -> None:
        with self._lock:
            self.upload_temp[sha256_hex] = (size_bytes, content)

    def get_temp_upload(self, sha256_hex: str) -> tuple[int, bytes] | None:
        with self._lock:
            return self.upload_temp.get(sha256_hex)

    def mark_sha_verified(self, sha256_hex: str) -> None:
        with self._lock:
            self.known_sha256.add(sha256_hex)

    def remove_temp_upload(self, sha256_hex: str) -> None:
        with self._lock:
            self.upload_temp.pop(sha256_hex, None)


@dataclass
class PostgresUploadStateStore:
    """PostgreSQL-backed state store for durable SHA dedup and temp uploads."""

    database_url: str

    def _connect(self):
        import psycopg

        return psycopg.connect(self.database_url)

    def initialize(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_known_sha256 (
                        sha256_hex TEXT PRIMARY KEY,
                        created_at_utc TEXT NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_temp_uploads (
                        sha256_hex TEXT PRIMARY KEY,
                        size_bytes BIGINT NOT NULL,
                        content BYTEA NOT NULL,
                        created_at_utc TEXT NOT NULL
                    );
                    """
                )
            conn.commit()

    def has_sha(self, sha256_hex: str) -> bool:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM api_known_sha256 WHERE sha256_hex = %s LIMIT 1;",
                    (sha256_hex,),
                )
                return cur.fetchone() is not None

    def has_shas(self, sha256_hex_values: list[str]) -> set[str]:
        if not sha256_hex_values:
            return set()

        # Preserve deterministic semantics for callers while reducing round-trips to PostgreSQL.
        unique_values = list(dict.fromkeys(sha256_hex_values))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT sha256_hex
                    FROM api_known_sha256
                    WHERE sha256_hex = ANY(%s);
                    """,
                    (unique_values,),
                )
                return {str(row[0]) for row in cur.fetchall()}

    def upsert_temp_upload(self, sha256_hex: str, size_bytes: int, content: bytes) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_temp_uploads (sha256_hex, size_bytes, content, created_at_utc)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (sha256_hex) DO UPDATE
                    SET size_bytes = EXCLUDED.size_bytes,
                        content = EXCLUDED.content,
                        created_at_utc = EXCLUDED.created_at_utc;
                    """,
                    (sha256_hex, size_bytes, content, now),
                )
            conn.commit()

    def get_temp_upload(self, sha256_hex: str) -> tuple[int, bytes] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT size_bytes, content
                    FROM api_temp_uploads
                    WHERE sha256_hex = %s
                    LIMIT 1;
                    """,
                    (sha256_hex,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                return int(row[0]), bytes(row[1])

    def mark_sha_verified(self, sha256_hex: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_known_sha256 (sha256_hex, created_at_utc)
                    VALUES (%s, %s)
                    ON CONFLICT (sha256_hex) DO NOTHING;
                    """,
                    (sha256_hex, now),
                )
            conn.commit()

    def remove_temp_upload(self, sha256_hex: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM api_temp_uploads WHERE sha256_hex = %s;", (sha256_hex,))
            conn.commit()
