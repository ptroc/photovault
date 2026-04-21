"""Persistence backends for upload dedup and upload file metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from threading import Lock
from typing import Protocol


@dataclass(frozen=True)
class TempUploadRecord:
    sha256_hex: str
    size_bytes: int
    temp_relative_path: str
    job_name: str
    original_filename: str
    received_at_utc: str


@dataclass(frozen=True)
class StoredFileRecord:
    relative_path: str
    sha256_hex: str
    size_bytes: int
    source_kind: str
    first_seen_at_utc: str
    last_seen_at_utc: str


@dataclass(frozen=True)
class StorageSummary:
    total_known_sha256: int
    total_stored_files: int
    indexed_files: int
    uploaded_files: int
    duplicate_file_paths: int
    recent_indexed_files_24h: int
    recent_uploaded_files_24h: int
    last_indexed_at_utc: str | None
    last_uploaded_at_utc: str | None


class UploadStateStore(Protocol):
    def initialize(self) -> None: ...

    def has_sha(self, sha256_hex: str) -> bool: ...

    def has_shas(self, sha256_hex_values: list[str]) -> set[str]: ...

    def upsert_temp_upload(
        self,
        *,
        sha256_hex: str,
        size_bytes: int,
        temp_relative_path: str,
        job_name: str,
        original_filename: str,
        received_at_utc: str,
    ) -> None: ...

    def get_temp_upload(self, sha256_hex: str) -> TempUploadRecord | None: ...

    def mark_sha_verified(self, sha256_hex: str) -> bool: ...

    def upsert_stored_file(
        self,
        *,
        relative_path: str,
        sha256_hex: str,
        size_bytes: int,
        source_kind: str,
        seen_at_utc: str,
    ) -> None: ...

    def get_stored_file_by_path(self, relative_path: str) -> StoredFileRecord | None: ...

    def list_stored_files(self, *, limit: int, offset: int) -> tuple[int, list[StoredFileRecord]]: ...

    def summarize_storage(self) -> StorageSummary: ...

    def remove_temp_upload(self, sha256_hex: str) -> None: ...


@dataclass
class InMemoryUploadStateStore:
    """In-memory store used for local tests and fallback development."""

    known_sha256: set[str] = field(default_factory=set)
    upload_temp: dict[str, TempUploadRecord] = field(default_factory=dict)
    stored_files: dict[str, StoredFileRecord] = field(default_factory=dict)
    _lock: Lock = field(default_factory=Lock)

    def initialize(self) -> None:
        return

    def has_sha(self, sha256_hex: str) -> bool:
        with self._lock:
            return sha256_hex in self.known_sha256

    def has_shas(self, sha256_hex_values: list[str]) -> set[str]:
        with self._lock:
            return {sha256_hex for sha256_hex in sha256_hex_values if sha256_hex in self.known_sha256}

    def upsert_temp_upload(
        self,
        *,
        sha256_hex: str,
        size_bytes: int,
        temp_relative_path: str,
        job_name: str,
        original_filename: str,
        received_at_utc: str,
    ) -> None:
        with self._lock:
            self.upload_temp[sha256_hex] = TempUploadRecord(
                sha256_hex=sha256_hex,
                size_bytes=size_bytes,
                temp_relative_path=temp_relative_path,
                job_name=job_name,
                original_filename=original_filename,
                received_at_utc=received_at_utc,
            )

    def get_temp_upload(self, sha256_hex: str) -> TempUploadRecord | None:
        with self._lock:
            return self.upload_temp.get(sha256_hex)

    def mark_sha_verified(self, sha256_hex: str) -> bool:
        with self._lock:
            is_new = sha256_hex not in self.known_sha256
            self.known_sha256.add(sha256_hex)
            return is_new

    def upsert_stored_file(
        self,
        *,
        relative_path: str,
        sha256_hex: str,
        size_bytes: int,
        source_kind: str,
        seen_at_utc: str,
    ) -> None:
        with self._lock:
            existing = self.stored_files.get(relative_path)
            first_seen = existing.first_seen_at_utc if existing is not None else seen_at_utc
            self.stored_files[relative_path] = StoredFileRecord(
                relative_path=relative_path,
                sha256_hex=sha256_hex,
                size_bytes=size_bytes,
                source_kind=source_kind,
                first_seen_at_utc=first_seen,
                last_seen_at_utc=seen_at_utc,
            )

    def get_stored_file_by_path(self, relative_path: str) -> StoredFileRecord | None:
        with self._lock:
            return self.stored_files.get(relative_path)

    def list_stored_files(self, *, limit: int, offset: int) -> tuple[int, list[StoredFileRecord]]:
        with self._lock:
            ordered = sorted(self.stored_files.values(), key=lambda item: item.relative_path)
            ordered = sorted(ordered, key=lambda item: item.last_seen_at_utc, reverse=True)
            total = len(ordered)
            return total, ordered[offset : offset + limit]

    def summarize_storage(self) -> StorageSummary:
        now = datetime.now(UTC)
        threshold = now - timedelta(hours=24)
        with self._lock:
            records = list(self.stored_files.values())
            duplicate_file_paths = len(records) - len({record.sha256_hex for record in records})
            indexed_records = [record for record in records if record.source_kind == "index_scan"]
            uploaded_records = [record for record in records if record.source_kind == "upload_verify"]
            recent_indexed = 0
            recent_uploaded = 0
            last_indexed: str | None = None
            last_uploaded: str | None = None

            for record in indexed_records:
                try:
                    seen_at = datetime.fromisoformat(record.last_seen_at_utc)
                except ValueError:
                    continue
                if seen_at >= threshold:
                    recent_indexed += 1
                if last_indexed is None or record.last_seen_at_utc > last_indexed:
                    last_indexed = record.last_seen_at_utc

            for record in uploaded_records:
                try:
                    seen_at = datetime.fromisoformat(record.last_seen_at_utc)
                except ValueError:
                    continue
                if seen_at >= threshold:
                    recent_uploaded += 1
                if last_uploaded is None or record.last_seen_at_utc > last_uploaded:
                    last_uploaded = record.last_seen_at_utc

            return StorageSummary(
                total_known_sha256=len(self.known_sha256),
                total_stored_files=len(records),
                indexed_files=len(indexed_records),
                uploaded_files=len(uploaded_records),
                duplicate_file_paths=duplicate_file_paths,
                recent_indexed_files_24h=recent_indexed,
                recent_uploaded_files_24h=recent_uploaded,
                last_indexed_at_utc=last_indexed,
                last_uploaded_at_utc=last_uploaded,
            )

    def remove_temp_upload(self, sha256_hex: str) -> None:
        with self._lock:
            self.upload_temp.pop(sha256_hex, None)


@dataclass
class PostgresUploadStateStore:
    """PostgreSQL-backed state store for durable SHA dedup and file metadata."""

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
                        temp_relative_path TEXT,
                        job_name TEXT,
                        original_filename TEXT,
                        received_at_utc TEXT,
                        created_at_utc TEXT NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_temp_uploads
                    ADD COLUMN IF NOT EXISTS temp_relative_path TEXT;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_temp_uploads
                    ADD COLUMN IF NOT EXISTS job_name TEXT;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_temp_uploads
                    ADD COLUMN IF NOT EXISTS original_filename TEXT;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_temp_uploads
                    ADD COLUMN IF NOT EXISTS received_at_utc TEXT;
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_stored_files (
                        relative_path TEXT PRIMARY KEY,
                        sha256_hex TEXT NOT NULL,
                        size_bytes BIGINT NOT NULL,
                        source_kind TEXT NOT NULL,
                        first_seen_at_utc TEXT NOT NULL,
                        last_seen_at_utc TEXT NOT NULL
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

    def upsert_temp_upload(
        self,
        *,
        sha256_hex: str,
        size_bytes: int,
        temp_relative_path: str,
        job_name: str,
        original_filename: str,
        received_at_utc: str,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_temp_uploads (
                        sha256_hex, size_bytes, temp_relative_path, job_name,
                        original_filename, received_at_utc, created_at_utc
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sha256_hex) DO UPDATE
                    SET size_bytes = EXCLUDED.size_bytes,
                        temp_relative_path = EXCLUDED.temp_relative_path,
                        job_name = EXCLUDED.job_name,
                        original_filename = EXCLUDED.original_filename,
                        received_at_utc = EXCLUDED.received_at_utc,
                        created_at_utc = EXCLUDED.created_at_utc;
                    """,
                    (
                        sha256_hex,
                        size_bytes,
                        temp_relative_path,
                        job_name,
                        original_filename,
                        received_at_utc,
                        now,
                    ),
                )
            conn.commit()

    def get_temp_upload(self, sha256_hex: str) -> TempUploadRecord | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT size_bytes, temp_relative_path, job_name, original_filename, received_at_utc
                    FROM api_temp_uploads
                    WHERE sha256_hex = %s
                    LIMIT 1;
                    """,
                    (sha256_hex,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                temp_relative_path = str(row[1] or "")
                job_name = str(row[2] or "")
                original_filename = str(row[3] or "")
                received_at_utc = str(row[4] or "")
                if not temp_relative_path or not job_name or not original_filename or not received_at_utc:
                    return None
                return TempUploadRecord(
                    sha256_hex=sha256_hex,
                    size_bytes=int(row[0]),
                    temp_relative_path=temp_relative_path,
                    job_name=job_name,
                    original_filename=original_filename,
                    received_at_utc=received_at_utc,
                )

    def mark_sha_verified(self, sha256_hex: str) -> bool:
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
                inserted = cur.rowcount > 0
            conn.commit()
            return inserted

    def upsert_stored_file(
        self,
        *,
        relative_path: str,
        sha256_hex: str,
        size_bytes: int,
        source_kind: str,
        seen_at_utc: str,
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_stored_files (
                        relative_path, sha256_hex, size_bytes, source_kind,
                        first_seen_at_utc, last_seen_at_utc
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (relative_path) DO UPDATE
                    SET sha256_hex = EXCLUDED.sha256_hex,
                        size_bytes = EXCLUDED.size_bytes,
                        source_kind = EXCLUDED.source_kind,
                        last_seen_at_utc = EXCLUDED.last_seen_at_utc;
                    """,
                    (relative_path, sha256_hex, size_bytes, source_kind, seen_at_utc, seen_at_utc),
                )
            conn.commit()

    def get_stored_file_by_path(self, relative_path: str) -> StoredFileRecord | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT sha256_hex, size_bytes, source_kind, first_seen_at_utc, last_seen_at_utc
                    FROM api_stored_files
                    WHERE relative_path = %s
                    LIMIT 1;
                    """,
                    (relative_path,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                return StoredFileRecord(
                    relative_path=relative_path,
                    sha256_hex=str(row[0]),
                    size_bytes=int(row[1]),
                    source_kind=str(row[2]),
                    first_seen_at_utc=str(row[3]),
                    last_seen_at_utc=str(row[4]),
                )

    def list_stored_files(self, *, limit: int, offset: int) -> tuple[int, list[StoredFileRecord]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM api_stored_files;")
                count_row = cur.fetchone()
                total = int(count_row[0]) if count_row is not None else 0
                cur.execute(
                    """
                    SELECT
                        relative_path,
                        sha256_hex,
                        size_bytes,
                        source_kind,
                        first_seen_at_utc,
                        last_seen_at_utc
                    FROM api_stored_files
                    ORDER BY last_seen_at_utc DESC, relative_path ASC
                    LIMIT %s
                    OFFSET %s;
                    """,
                    (limit, offset),
                )
                rows = cur.fetchall()
                records = [
                    StoredFileRecord(
                        relative_path=str(row[0]),
                        sha256_hex=str(row[1]),
                        size_bytes=int(row[2]),
                        source_kind=str(row[3]),
                        first_seen_at_utc=str(row[4]),
                        last_seen_at_utc=str(row[5]),
                    )
                    for row in rows
                ]
                return total, records

    def summarize_storage(self) -> StorageSummary:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM api_known_sha256;")
                known_row = cur.fetchone()
                total_known_sha256 = int(known_row[0]) if known_row is not None else 0

                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total_stored_files,
                        COUNT(*) FILTER (WHERE source_kind = 'index_scan') AS indexed_files,
                        COUNT(*) FILTER (WHERE source_kind = 'upload_verify') AS uploaded_files,
                        COUNT(*) - COUNT(DISTINCT sha256_hex) AS duplicate_file_paths
                    FROM api_stored_files;
                    """
                )
                aggregate_row = cur.fetchone()
                total_stored_files = int(aggregate_row[0]) if aggregate_row is not None else 0
                indexed_files = int(aggregate_row[1]) if aggregate_row is not None else 0
                uploaded_files = int(aggregate_row[2]) if aggregate_row is not None else 0
                duplicate_file_paths = int(aggregate_row[3]) if aggregate_row is not None else 0

                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (
                            WHERE source_kind = 'index_scan'
                            AND last_seen_at_utc >= %s
                        ) AS recent_indexed_files_24h,
                        COUNT(*) FILTER (
                            WHERE source_kind = 'upload_verify'
                            AND last_seen_at_utc >= %s
                        ) AS recent_uploaded_files_24h,
                        MAX(last_seen_at_utc) FILTER (
                            WHERE source_kind = 'index_scan'
                        ) AS last_indexed_at_utc,
                        MAX(last_seen_at_utc) FILTER (
                            WHERE source_kind = 'upload_verify'
                        ) AS last_uploaded_at_utc
                    FROM api_stored_files;
                    """,
                    (
                        (datetime.now(UTC) - timedelta(hours=24)).isoformat(),
                        (datetime.now(UTC) - timedelta(hours=24)).isoformat(),
                    ),
                )
                recent_row = cur.fetchone()
                recent_indexed_files_24h = (
                    int(recent_row[0]) if recent_row and recent_row[0] is not None else 0
                )
                recent_uploaded_files_24h = (
                    int(recent_row[1]) if recent_row and recent_row[1] is not None else 0
                )
                last_indexed_at_utc = (
                    str(recent_row[2]) if recent_row and recent_row[2] is not None else None
                )
                last_uploaded_at_utc = (
                    str(recent_row[3]) if recent_row and recent_row[3] is not None else None
                )

                return StorageSummary(
                    total_known_sha256=total_known_sha256,
                    total_stored_files=total_stored_files,
                    indexed_files=indexed_files,
                    uploaded_files=uploaded_files,
                    duplicate_file_paths=duplicate_file_paths,
                    recent_indexed_files_24h=recent_indexed_files_24h,
                    recent_uploaded_files_24h=recent_uploaded_files_24h,
                    last_indexed_at_utc=last_indexed_at_utc,
                    last_uploaded_at_utc=last_uploaded_at_utc,
                )

    def remove_temp_upload(self, sha256_hex: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM api_temp_uploads WHERE sha256_hex = %s;", (sha256_hex,))
            conn.commit()
