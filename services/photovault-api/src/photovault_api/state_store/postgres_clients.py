"""Client helpers for PostgresUploadStateStore."""

from __future__ import annotations

from .records import ClientHeartbeatRecord, ClientRecord


def upsert_client_pending(
    self,
    *,
    client_id: str,
    display_name: str,
    enrolled_at_utc: str,
) -> ClientRecord:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO api_clients (
                    client_id,
                    display_name,
                    enrollment_status,
                    first_seen_at_utc,
                    last_enrolled_at_utc,
                    approved_at_utc,
                    revoked_at_utc,
                    auth_token
                )
                VALUES (%s, %s, 'pending', %s, %s, NULL, NULL, NULL)
                ON CONFLICT (client_id) DO UPDATE
                SET display_name = EXCLUDED.display_name,
                    last_enrolled_at_utc = EXCLUDED.last_enrolled_at_utc,
                    enrollment_status = CASE
                        WHEN api_clients.enrollment_status = 'pending' THEN 'pending'
                        ELSE api_clients.enrollment_status
                    END,
                    approved_at_utc = CASE
                        WHEN api_clients.enrollment_status = 'pending' THEN NULL
                        ELSE api_clients.approved_at_utc
                    END,
                    revoked_at_utc = CASE
                        WHEN api_clients.enrollment_status = 'pending' THEN NULL
                        ELSE api_clients.revoked_at_utc
                    END,
                    auth_token = CASE
                        WHEN api_clients.enrollment_status = 'pending' THEN NULL
                        ELSE api_clients.auth_token
                    END
                RETURNING
                    client_id,
                    display_name,
                    enrollment_status,
                    first_seen_at_utc,
                    last_enrolled_at_utc,
                    approved_at_utc,
                    revoked_at_utc,
                    auth_token;
                """,
                (client_id, display_name, enrolled_at_utc, enrolled_at_utc),
            )
            row = cur.fetchone()
        conn.commit()

    if row is None:
        raise RuntimeError("upsert_client_pending must return a row")
    return ClientRecord(
        client_id=str(row[0]),
        display_name=str(row[1]),
        enrollment_status=str(row[2]),
        first_seen_at_utc=str(row[3]),
        last_enrolled_at_utc=str(row[4]),
        approved_at_utc=str(row[5]) if row[5] is not None else None,
        revoked_at_utc=str(row[6]) if row[6] is not None else None,
        auth_token=str(row[7]) if row[7] is not None else None,
    )

def get_client(self, client_id: str) -> ClientRecord | None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    client_id,
                    display_name,
                    enrollment_status,
                    first_seen_at_utc,
                    last_enrolled_at_utc,
                    approved_at_utc,
                    revoked_at_utc,
                    auth_token
                FROM api_clients
                WHERE client_id = %s
                LIMIT 1;
                """,
                (client_id,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return ClientRecord(
        client_id=str(row[0]),
        display_name=str(row[1]),
        enrollment_status=str(row[2]),
        first_seen_at_utc=str(row[3]),
        last_enrolled_at_utc=str(row[4]),
        approved_at_utc=str(row[5]) if row[5] is not None else None,
        revoked_at_utc=str(row[6]) if row[6] is not None else None,
        auth_token=str(row[7]) if row[7] is not None else None,
    )

def list_clients(self, *, limit: int, offset: int) -> tuple[int, list[ClientRecord]]:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM api_clients;")
            count_row = cur.fetchone()
            total = int(count_row[0]) if count_row is not None else 0
            cur.execute(
                """
                SELECT
                    client_id,
                    display_name,
                    enrollment_status,
                    first_seen_at_utc,
                    last_enrolled_at_utc,
                    approved_at_utc,
                    revoked_at_utc,
                    auth_token
                FROM api_clients
                ORDER BY first_seen_at_utc DESC, client_id ASC
                LIMIT %s
                OFFSET %s;
                """,
                (limit, offset),
            )
            rows = cur.fetchall()
    return total, [
        ClientRecord(
            client_id=str(row[0]),
            display_name=str(row[1]),
            enrollment_status=str(row[2]),
            first_seen_at_utc=str(row[3]),
            last_enrolled_at_utc=str(row[4]),
            approved_at_utc=str(row[5]) if row[5] is not None else None,
            revoked_at_utc=str(row[6]) if row[6] is not None else None,
            auth_token=str(row[7]) if row[7] is not None else None,
        )
        for row in rows
    ]

def approve_client(
    self,
    *,
    client_id: str,
    approved_at_utc: str,
    auth_token: str,
) -> ClientRecord | None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE api_clients
                SET enrollment_status = 'approved',
                    approved_at_utc = %s,
                    revoked_at_utc = NULL,
                    auth_token = %s
                WHERE client_id = %s
                RETURNING
                    client_id,
                    display_name,
                    enrollment_status,
                    first_seen_at_utc,
                    last_enrolled_at_utc,
                    approved_at_utc,
                    revoked_at_utc,
                    auth_token;
                """,
                (approved_at_utc, auth_token, client_id),
            )
            row = cur.fetchone()
        conn.commit()
    if row is None:
        return None
    return ClientRecord(
        client_id=str(row[0]),
        display_name=str(row[1]),
        enrollment_status=str(row[2]),
        first_seen_at_utc=str(row[3]),
        last_enrolled_at_utc=str(row[4]),
        approved_at_utc=str(row[5]) if row[5] is not None else None,
        revoked_at_utc=str(row[6]) if row[6] is not None else None,
        auth_token=str(row[7]) if row[7] is not None else None,
    )

def revoke_client(
    self,
    *,
    client_id: str,
    revoked_at_utc: str,
) -> ClientRecord | None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE api_clients
                SET enrollment_status = 'revoked',
                    revoked_at_utc = %s
                WHERE client_id = %s
                RETURNING
                    client_id,
                    display_name,
                    enrollment_status,
                    first_seen_at_utc,
                    last_enrolled_at_utc,
                    approved_at_utc,
                    revoked_at_utc,
                    auth_token;
                """,
                (revoked_at_utc, client_id),
            )
            row = cur.fetchone()
        conn.commit()
    if row is None:
        return None
    return ClientRecord(
        client_id=str(row[0]),
        display_name=str(row[1]),
        enrollment_status=str(row[2]),
        first_seen_at_utc=str(row[3]),
        last_enrolled_at_utc=str(row[4]),
        approved_at_utc=str(row[5]) if row[5] is not None else None,
        revoked_at_utc=str(row[6]) if row[6] is not None else None,
        auth_token=str(row[7]) if row[7] is not None else None,
    )

def upsert_client_heartbeat(
    self,
    *,
    client_id: str,
    last_seen_at_utc: str,
    daemon_state: str,
    workload_status: str,
    active_job_id: int | None,
    active_job_label: str | None,
    active_job_status: str | None,
    active_job_ready_to_upload: int | None,
    active_job_uploaded: int | None,
    active_job_retrying: int | None,
    active_job_total_files: int | None,
    active_job_non_terminal_files: int | None,
    active_job_error_files: int | None,
    active_job_blocking_reason: str | None,
    retry_pending_count: int | None,
    retry_next_at_utc: str | None,
    retry_reason: str | None,
    auth_block_reason: str | None,
    recent_error_category: str | None,
    recent_error_message: str | None,
    recent_error_at_utc: str | None,
    updated_at_utc: str,
) -> ClientHeartbeatRecord:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO api_client_heartbeats (
                    client_id,
                    last_seen_at_utc,
                    daemon_state,
                    workload_status,
                    active_job_id,
                    active_job_label,
                    active_job_status,
                    active_job_ready_to_upload,
                    active_job_uploaded,
                    active_job_retrying,
                    active_job_total_files,
                    active_job_non_terminal_files,
                    active_job_error_files,
                    active_job_blocking_reason,
                    retry_pending_count,
                    retry_next_at_utc,
                    retry_reason,
                    auth_block_reason,
                    recent_error_category,
                    recent_error_message,
                    recent_error_at_utc,
                    updated_at_utc
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (client_id) DO UPDATE
                SET last_seen_at_utc = EXCLUDED.last_seen_at_utc,
                    daemon_state = EXCLUDED.daemon_state,
                    workload_status = EXCLUDED.workload_status,
                    active_job_id = EXCLUDED.active_job_id,
                    active_job_label = EXCLUDED.active_job_label,
                    active_job_status = EXCLUDED.active_job_status,
                    active_job_ready_to_upload = EXCLUDED.active_job_ready_to_upload,
                    active_job_uploaded = EXCLUDED.active_job_uploaded,
                    active_job_retrying = EXCLUDED.active_job_retrying,
                    active_job_total_files = EXCLUDED.active_job_total_files,
                    active_job_non_terminal_files = EXCLUDED.active_job_non_terminal_files,
                    active_job_error_files = EXCLUDED.active_job_error_files,
                    active_job_blocking_reason = EXCLUDED.active_job_blocking_reason,
                    retry_pending_count = EXCLUDED.retry_pending_count,
                    retry_next_at_utc = EXCLUDED.retry_next_at_utc,
                    retry_reason = EXCLUDED.retry_reason,
                    auth_block_reason = EXCLUDED.auth_block_reason,
                    recent_error_category = EXCLUDED.recent_error_category,
                    recent_error_message = EXCLUDED.recent_error_message,
                    recent_error_at_utc = EXCLUDED.recent_error_at_utc,
                    updated_at_utc = EXCLUDED.updated_at_utc
                RETURNING
                    client_id,
                    last_seen_at_utc,
                    daemon_state,
                    workload_status,
                    active_job_id,
                    active_job_label,
                    active_job_status,
                    active_job_ready_to_upload,
                    active_job_uploaded,
                    active_job_retrying,
                    active_job_total_files,
                    active_job_non_terminal_files,
                    active_job_error_files,
                    active_job_blocking_reason,
                    retry_pending_count,
                    retry_next_at_utc,
                    retry_reason,
                    auth_block_reason,
                    recent_error_category,
                    recent_error_message,
                    recent_error_at_utc,
                    updated_at_utc;
                """,
                (
                    client_id,
                    last_seen_at_utc,
                    daemon_state,
                    workload_status,
                    active_job_id,
                    active_job_label,
                    active_job_status,
                    active_job_ready_to_upload,
                    active_job_uploaded,
                    active_job_retrying,
                    active_job_total_files,
                    active_job_non_terminal_files,
                    active_job_error_files,
                    active_job_blocking_reason,
                    retry_pending_count,
                    retry_next_at_utc,
                    retry_reason,
                    auth_block_reason,
                    recent_error_category,
                    recent_error_message,
                    recent_error_at_utc,
                    updated_at_utc,
                ),
            )
            row = cur.fetchone()
        conn.commit()
    if row is None:
        raise RuntimeError("upsert_client_heartbeat must return a row")
    return ClientHeartbeatRecord(
        client_id=str(row[0]),
        last_seen_at_utc=str(row[1]),
        daemon_state=str(row[2]),
        workload_status=str(row[3]),
        active_job_id=int(row[4]) if row[4] is not None else None,
        active_job_label=str(row[5]) if row[5] is not None else None,
        active_job_status=str(row[6]) if row[6] is not None else None,
        active_job_ready_to_upload=int(row[7]) if row[7] is not None else None,
        active_job_uploaded=int(row[8]) if row[8] is not None else None,
        active_job_retrying=int(row[9]) if row[9] is not None else None,
        active_job_total_files=int(row[10]) if row[10] is not None else None,
        active_job_non_terminal_files=int(row[11]) if row[11] is not None else None,
        active_job_error_files=int(row[12]) if row[12] is not None else None,
        active_job_blocking_reason=str(row[13]) if row[13] is not None else None,
        retry_pending_count=int(row[14]) if row[14] is not None else None,
        retry_next_at_utc=str(row[15]) if row[15] is not None else None,
        retry_reason=str(row[16]) if row[16] is not None else None,
        auth_block_reason=str(row[17]) if row[17] is not None else None,
        recent_error_category=str(row[18]) if row[18] is not None else None,
        recent_error_message=str(row[19]) if row[19] is not None else None,
        recent_error_at_utc=str(row[20]) if row[20] is not None else None,
        updated_at_utc=str(row[21]),
    )

def get_client_heartbeat(self, client_id: str) -> ClientHeartbeatRecord | None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    client_id,
                    last_seen_at_utc,
                    daemon_state,
                    workload_status,
                    active_job_id,
                    active_job_label,
                    active_job_status,
                    active_job_ready_to_upload,
                    active_job_uploaded,
                    active_job_retrying,
                    active_job_total_files,
                    active_job_non_terminal_files,
                    active_job_error_files,
                    active_job_blocking_reason,
                    retry_pending_count,
                    retry_next_at_utc,
                    retry_reason,
                    auth_block_reason,
                    recent_error_category,
                    recent_error_message,
                    recent_error_at_utc,
                    updated_at_utc
                FROM api_client_heartbeats
                WHERE client_id = %s
                LIMIT 1;
                """,
                (client_id,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return ClientHeartbeatRecord(
        client_id=str(row[0]),
        last_seen_at_utc=str(row[1]),
        daemon_state=str(row[2]),
        workload_status=str(row[3]),
        active_job_id=int(row[4]) if row[4] is not None else None,
        active_job_label=str(row[5]) if row[5] is not None else None,
        active_job_status=str(row[6]) if row[6] is not None else None,
        active_job_ready_to_upload=int(row[7]) if row[7] is not None else None,
        active_job_uploaded=int(row[8]) if row[8] is not None else None,
        active_job_retrying=int(row[9]) if row[9] is not None else None,
        active_job_total_files=int(row[10]) if row[10] is not None else None,
        active_job_non_terminal_files=int(row[11]) if row[11] is not None else None,
        active_job_error_files=int(row[12]) if row[12] is not None else None,
        active_job_blocking_reason=str(row[13]) if row[13] is not None else None,
        retry_pending_count=int(row[14]) if row[14] is not None else None,
        retry_next_at_utc=str(row[15]) if row[15] is not None else None,
        retry_reason=str(row[16]) if row[16] is not None else None,
        auth_block_reason=str(row[17]) if row[17] is not None else None,
        recent_error_category=str(row[18]) if row[18] is not None else None,
        recent_error_message=str(row[19]) if row[19] is not None else None,
        recent_error_at_utc=str(row[20]) if row[20] is not None else None,
        updated_at_utc=str(row[21]),
    )

def remove_temp_upload(self, sha256_hex: str) -> None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM api_temp_uploads WHERE sha256_hex = %s;", (sha256_hex,))
        conn.commit()
