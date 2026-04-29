"""Detected-media query helpers."""

import sqlite3
from typing import Sequence

from .queries_common import (
    DETECTED_MEDIA_EVENT_INSERTED,
    DETECTED_MEDIA_EVENT_REMOVED,
    DETECTED_MEDIA_STATUS_PRESENT,
    DETECTED_MEDIA_STATUS_REMOVED,
)


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized if normalized else None


def _detected_media_from_row(row: Sequence[object]) -> dict[str, object]:
    return {
        "media_id": int(row[0]),
        "media_key": str(row[1]),
        "filesystem_uuid": row[2],
        "device_path": row[3],
        "mount_path": row[4],
        "filesystem_label": row[5],
        "status": str(row[6]),
        "inserted_at_utc": row[7],
        "removed_at_utc": row[8],
        "last_event_at_utc": row[9],
        "insert_event_count": int(row[10]),
        "remove_event_count": int(row[11]),
        "created_at_utc": row[12],
        "updated_at_utc": row[13],
    }


def fetch_detected_media_by_id(conn: sqlite3.Connection, media_id: int) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT
            id, media_key, filesystem_uuid, device_path, mount_path, filesystem_label,
            status, inserted_at_utc, removed_at_utc, last_event_at_utc,
            insert_event_count, remove_event_count, created_at_utc, updated_at_utc
        FROM detected_media
        WHERE id = ?
        LIMIT 1;
        """,
        (media_id,),
    ).fetchone()
    if row is None:
        return None
    return _detected_media_from_row(row)


def fetch_detected_media_by_key(conn: sqlite3.Connection, media_key: str) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT
            id, media_key, filesystem_uuid, device_path, mount_path, filesystem_label,
            status, inserted_at_utc, removed_at_utc, last_event_at_utc,
            insert_event_count, remove_event_count, created_at_utc, updated_at_utc
        FROM detected_media
        WHERE media_key = ?
        LIMIT 1;
        """,
        (media_key,),
    ).fetchone()
    if row is None:
        return None
    return _detected_media_from_row(row)


def find_detected_media_by_device_or_mount(
    conn: sqlite3.Connection,
    *,
    device_path: str | None,
    mount_path: str | None,
) -> dict[str, object] | None:
    normalized_device = _normalize_optional_text(device_path)
    normalized_mount = _normalize_optional_text(mount_path)
    clauses: list[str] = []
    params: list[object] = []
    if normalized_device is not None:
        clauses.append("device_path = ?")
        params.append(normalized_device)
    if normalized_mount is not None:
        clauses.append("mount_path = ?")
        params.append(normalized_mount)

    if not clauses:
        return None

    where_clause = " OR ".join(clauses)
    row = conn.execute(
        f"""
        SELECT
            id, media_key, filesystem_uuid, device_path, mount_path, filesystem_label,
            status, inserted_at_utc, removed_at_utc, last_event_at_utc,
            insert_event_count, remove_event_count, created_at_utc, updated_at_utc
        FROM detected_media
        WHERE ({where_clause})
        ORDER BY CASE WHEN status = ? THEN 0 ELSE 1 END ASC, last_event_at_utc DESC, id DESC
        LIMIT 1;
        """,
        (*params, DETECTED_MEDIA_STATUS_PRESENT),
    ).fetchone()
    if row is None:
        return None
    return _detected_media_from_row(row)


def list_detected_media(conn: sqlite3.Connection, *, limit: int = 100) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT
            id, media_key, filesystem_uuid, device_path, mount_path, filesystem_label,
            status, inserted_at_utc, removed_at_utc, last_event_at_utc,
            insert_event_count, remove_event_count, created_at_utc, updated_at_utc
        FROM detected_media
        ORDER BY CASE WHEN status = ? THEN 0 ELSE 1 END ASC, last_event_at_utc DESC, id DESC
        LIMIT ?;
        """,
        (DETECTED_MEDIA_STATUS_PRESENT, limit),
    ).fetchall()
    return [_detected_media_from_row(row) for row in rows]


def list_detected_media_events(conn: sqlite3.Connection, *, limit: int = 50) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT
            e.id, e.media_id, m.media_key, e.event_type, e.event_source, e.filesystem_uuid,
            e.device_path, e.mount_path, e.filesystem_label, e.event_at_utc
        FROM detected_media_events e
        JOIN detected_media m ON m.id = e.media_id
        ORDER BY e.id DESC
        LIMIT ?;
        """,
        (limit,),
    ).fetchall()
    return [
        {
            "event_id": int(row[0]),
            "media_id": int(row[1]),
            "media_key": str(row[2]),
            "event_type": str(row[3]),
            "event_source": str(row[4]),
            "filesystem_uuid": row[5],
            "device_path": row[6],
            "mount_path": row[7],
            "filesystem_label": row[8],
            "event_at_utc": row[9],
        }
        for row in rows
    ]


def clear_detected_media(conn: sqlite3.Connection) -> dict[str, int]:
    media_count_row = conn.execute("SELECT COUNT(1) FROM detected_media;").fetchone()
    event_count_row = conn.execute("SELECT COUNT(1) FROM detected_media_events;").fetchone()
    media_count = int(media_count_row[0]) if media_count_row else 0
    event_count = int(event_count_row[0]) if event_count_row else 0

    conn.execute("DELETE FROM detected_media;")
    return {
        "deleted_media_rows": media_count,
        "deleted_event_rows": event_count,
    }


def _append_detected_media_event(
    conn: sqlite3.Connection,
    *,
    media_id: int,
    event_type: str,
    event_source: str,
    filesystem_uuid: str | None,
    device_path: str | None,
    mount_path: str | None,
    filesystem_label: str | None,
    event_at_utc: str,
) -> None:
    conn.execute(
        """
        INSERT INTO detected_media_events (
            media_id, event_type, event_source, filesystem_uuid, device_path,
            mount_path, filesystem_label, event_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            media_id,
            event_type,
            event_source,
            filesystem_uuid,
            device_path,
            mount_path,
            filesystem_label,
            event_at_utc,
        ),
    )


def register_detected_media_inserted(
    conn: sqlite3.Connection,
    *,
    media_key: str,
    filesystem_uuid: str | None,
    device_path: str | None,
    mount_path: str | None,
    filesystem_label: str | None,
    event_source: str,
    now_utc: str,
) -> dict[str, object]:
    normalized_uuid = _normalize_optional_text(filesystem_uuid)
    normalized_device = _normalize_optional_text(device_path)
    normalized_mount = _normalize_optional_text(mount_path)
    normalized_label = _normalize_optional_text(filesystem_label)
    normalized_source = _normalize_optional_text(event_source) or "unknown"

    existing = fetch_detected_media_by_key(conn, media_key)
    if existing is None:
        cursor = conn.execute(
            """
            INSERT INTO detected_media (
                media_key, filesystem_uuid, device_path, mount_path, filesystem_label, status,
                inserted_at_utc, removed_at_utc, last_event_at_utc,
                insert_event_count, remove_event_count, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, 1, 0, ?, ?);
            """,
            (
                media_key,
                normalized_uuid,
                normalized_device,
                normalized_mount,
                normalized_label,
                DETECTED_MEDIA_STATUS_PRESENT,
                now_utc,
                now_utc,
                now_utc,
                now_utc,
            ),
        )
        media_id = int(cursor.lastrowid)
        _append_detected_media_event(
            conn,
            media_id=media_id,
            event_type=DETECTED_MEDIA_EVENT_INSERTED,
            event_source=normalized_source,
            filesystem_uuid=normalized_uuid,
            device_path=normalized_device,
            mount_path=normalized_mount,
            filesystem_label=normalized_label,
            event_at_utc=now_utc,
        )
        created = fetch_detected_media_by_id(conn, media_id)
        if created is None:
            raise RuntimeError(f"failed to create detected_media row for key={media_key}")
        return {
            "media": created,
            "created": True,
            "status_changed": True,
            "deduplicated": False,
            "event_recorded": True,
        }

    next_uuid = normalized_uuid if normalized_uuid is not None else existing["filesystem_uuid"]
    next_device = normalized_device if normalized_device is not None else existing["device_path"]
    next_mount = normalized_mount if normalized_mount is not None else existing["mount_path"]
    next_label = normalized_label if normalized_label is not None else existing["filesystem_label"]
    status_changed = existing["status"] != DETECTED_MEDIA_STATUS_PRESENT
    metadata_changed = (
        next_uuid != existing["filesystem_uuid"]
        or next_device != existing["device_path"]
        or next_mount != existing["mount_path"]
        or next_label != existing["filesystem_label"]
    )
    should_record_event = status_changed or metadata_changed
    if not should_record_event:
        return {
            "media": existing,
            "created": False,
            "status_changed": False,
            "deduplicated": True,
            "event_recorded": False,
        }

    if status_changed:
        conn.execute(
            """
            UPDATE detected_media
            SET filesystem_uuid = ?,
                device_path = ?,
                mount_path = ?,
                filesystem_label = ?,
                status = ?,
                inserted_at_utc = ?,
                removed_at_utc = NULL,
                last_event_at_utc = ?,
                insert_event_count = insert_event_count + 1,
                updated_at_utc = ?
            WHERE id = ?;
            """,
            (
                next_uuid,
                next_device,
                next_mount,
                next_label,
                DETECTED_MEDIA_STATUS_PRESENT,
                now_utc,
                now_utc,
                now_utc,
                existing["media_id"],
            ),
        )
    else:
        conn.execute(
            """
            UPDATE detected_media
            SET filesystem_uuid = ?,
                device_path = ?,
                mount_path = ?,
                filesystem_label = ?,
                last_event_at_utc = ?,
                updated_at_utc = ?
            WHERE id = ?;
            """,
            (
                next_uuid,
                next_device,
                next_mount,
                next_label,
                now_utc,
                now_utc,
                existing["media_id"],
            ),
        )

    _append_detected_media_event(
        conn,
        media_id=int(existing["media_id"]),
        event_type=DETECTED_MEDIA_EVENT_INSERTED,
        event_source=normalized_source,
        filesystem_uuid=next_uuid,
        device_path=next_device,
        mount_path=next_mount,
        filesystem_label=next_label,
        event_at_utc=now_utc,
    )
    updated = fetch_detected_media_by_id(conn, int(existing["media_id"]))
    if updated is None:
        raise RuntimeError(f"detected_media row disappeared for key={media_key}")
    return {
        "media": updated,
        "created": False,
        "status_changed": status_changed,
        "deduplicated": False,
        "event_recorded": True,
    }


def register_detected_media_removed(
    conn: sqlite3.Connection,
    *,
    media_key: str,
    filesystem_uuid: str | None,
    device_path: str | None,
    mount_path: str | None,
    filesystem_label: str | None,
    event_source: str,
    now_utc: str,
) -> dict[str, object]:
    normalized_uuid = _normalize_optional_text(filesystem_uuid)
    normalized_device = _normalize_optional_text(device_path)
    normalized_mount = _normalize_optional_text(mount_path)
    normalized_label = _normalize_optional_text(filesystem_label)
    normalized_source = _normalize_optional_text(event_source) or "unknown"

    existing = fetch_detected_media_by_key(conn, media_key)
    if existing is None:
        cursor = conn.execute(
            """
            INSERT INTO detected_media (
                media_key, filesystem_uuid, device_path, mount_path, filesystem_label, status,
                inserted_at_utc, removed_at_utc, last_event_at_utc,
                insert_event_count, remove_event_count, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, 0, 1, ?, ?);
            """,
            (
                media_key,
                normalized_uuid,
                normalized_device,
                normalized_mount,
                normalized_label,
                DETECTED_MEDIA_STATUS_REMOVED,
                now_utc,
                now_utc,
                now_utc,
                now_utc,
            ),
        )
        media_id = int(cursor.lastrowid)
        _append_detected_media_event(
            conn,
            media_id=media_id,
            event_type=DETECTED_MEDIA_EVENT_REMOVED,
            event_source=normalized_source,
            filesystem_uuid=normalized_uuid,
            device_path=normalized_device,
            mount_path=normalized_mount,
            filesystem_label=normalized_label,
            event_at_utc=now_utc,
        )
        created = fetch_detected_media_by_id(conn, media_id)
        if created is None:
            raise RuntimeError(f"failed to create removed detected_media row for key={media_key}")
        return {
            "media": created,
            "created": True,
            "status_changed": True,
            "deduplicated": False,
            "event_recorded": True,
        }

    next_uuid = normalized_uuid if normalized_uuid is not None else existing["filesystem_uuid"]
    next_device = normalized_device if normalized_device is not None else existing["device_path"]
    next_mount = normalized_mount if normalized_mount is not None else existing["mount_path"]
    next_label = normalized_label if normalized_label is not None else existing["filesystem_label"]
    status_changed = existing["status"] != DETECTED_MEDIA_STATUS_REMOVED
    metadata_changed = (
        next_uuid != existing["filesystem_uuid"]
        or next_device != existing["device_path"]
        or next_mount != existing["mount_path"]
        or next_label != existing["filesystem_label"]
    )
    should_record_event = status_changed or metadata_changed
    if not should_record_event:
        return {
            "media": existing,
            "created": False,
            "status_changed": False,
            "deduplicated": True,
            "event_recorded": False,
        }

    if status_changed:
        conn.execute(
            """
            UPDATE detected_media
            SET filesystem_uuid = ?,
                device_path = ?,
                mount_path = ?,
                filesystem_label = ?,
                status = ?,
                removed_at_utc = ?,
                last_event_at_utc = ?,
                remove_event_count = remove_event_count + 1,
                updated_at_utc = ?
            WHERE id = ?;
            """,
            (
                next_uuid,
                next_device,
                next_mount,
                next_label,
                DETECTED_MEDIA_STATUS_REMOVED,
                now_utc,
                now_utc,
                now_utc,
                existing["media_id"],
            ),
        )
    else:
        conn.execute(
            """
            UPDATE detected_media
            SET filesystem_uuid = ?,
                device_path = ?,
                mount_path = ?,
                filesystem_label = ?,
                last_event_at_utc = ?,
                updated_at_utc = ?
            WHERE id = ?;
            """,
            (
                next_uuid,
                next_device,
                next_mount,
                next_label,
                now_utc,
                now_utc,
                existing["media_id"],
            ),
        )

    _append_detected_media_event(
        conn,
        media_id=int(existing["media_id"]),
        event_type=DETECTED_MEDIA_EVENT_REMOVED,
        event_source=normalized_source,
        filesystem_uuid=next_uuid,
        device_path=next_device,
        mount_path=next_mount,
        filesystem_label=next_label,
        event_at_utc=now_utc,
    )
    updated = fetch_detected_media_by_id(conn, int(existing["media_id"]))
    if updated is None:
        raise RuntimeError(f"detected_media row disappeared for key={media_key}")
    return {
        "media": updated,
        "created": False,
        "status_changed": status_changed,
        "deduplicated": False,
        "event_recorded": True,
    }

