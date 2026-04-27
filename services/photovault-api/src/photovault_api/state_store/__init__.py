"""photovault_api.state_store — public surface re-export.

All names that existed in the original monolithic state_store.py are
re-exported from here so that no caller or test import path needs to change.
"""

from .records import (
    CatalogBackfillRunRecord,
    ClientHeartbeatRecord,
    ClientRecord,
    DuplicateShaGroup,
    MediaAssetRecord,
    MediaExtractionRecord,
    MediaPreviewRecord,
    PathConflictRecord,
    RejectedAssetRecord,
    StorageIndexRunRecord,
    StorageSummary,
    StoredFileRecord,
    TempUploadRecord,
    TombstoneRecord,
    _MEDIA_TYPE_SUFFIXES,
    _PREVIEWABLE_SUFFIXES,
    _media_type_for_path,
    _preview_capability_for_path,
)
from .protocol import UploadStateStore
from .in_memory import InMemoryUploadStateStore
from .postgres import PostgresUploadStateStore

__all__ = [
    # records
    "CatalogBackfillRunRecord",
    "ClientHeartbeatRecord",
    "ClientRecord",
    "DuplicateShaGroup",
    "MediaAssetRecord",
    "MediaExtractionRecord",
    "MediaPreviewRecord",
    "PathConflictRecord",
    "RejectedAssetRecord",
    "StorageIndexRunRecord",
    "StorageSummary",
    "StoredFileRecord",
    "TempUploadRecord",
    "TombstoneRecord",
    "_MEDIA_TYPE_SUFFIXES",
    "_PREVIEWABLE_SUFFIXES",
    "_media_type_for_path",
    "_preview_capability_for_path",
    # protocol
    "UploadStateStore",
    # implementations
    "InMemoryUploadStateStore",
    "PostgresUploadStateStore",
]
