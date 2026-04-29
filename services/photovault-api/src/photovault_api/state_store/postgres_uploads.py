"""Upload/storage helpers for PostgresUploadStateStore."""

from __future__ import annotations

from datetime import UTC, datetime

from .records import StoredFileRecord, TempUploadRecord


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

def delete_stored_file(self, relative_path: str) -> bool:
    """Delete a stored-file row; FK cascade removes dependent catalog rows."""
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM api_stored_files WHERE relative_path = %s;
                """,
                (relative_path,),
            )
            deleted = cur.rowcount > 0
        conn.commit()
    return deleted


def remove_temp_upload(self, sha256_hex: str) -> None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM api_temp_uploads WHERE sha256_hex = %s;", (sha256_hex,))
        conn.commit()
