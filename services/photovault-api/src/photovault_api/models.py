"""Pydantic request/response models and StrEnum types for photovault-api."""
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class HandshakeDecision(StrEnum):
    ALREADY_EXISTS = "ALREADY_EXISTS"
    UPLOAD_REQUIRED = "UPLOAD_REQUIRED"


class HandshakeFileRequest(BaseModel):
    client_file_id: int = Field(ge=1)
    sha256_hex: str = Field(min_length=64, max_length=64)
    size_bytes: int = Field(ge=0)


class MetadataHandshakeRequest(BaseModel):
    files: list[HandshakeFileRequest] = Field(min_length=1)


class HandshakeFileResult(BaseModel):
    client_file_id: int
    decision: HandshakeDecision


class MetadataHandshakeResponse(BaseModel):
    results: list[HandshakeFileResult]


class UploadContentResponse(BaseModel):
    status: str


class VerifyRequest(BaseModel):
    sha256_hex: str = Field(min_length=64, max_length=64)
    size_bytes: int = Field(ge=0)


class VerifyResponse(BaseModel):
    status: str


class IndexStorageResponse(BaseModel):
    scanned_files: int
    indexed_files: int
    new_sha_entries: int
    existing_sha_matches: int
    path_conflicts: int
    errors: int


class AdminOverviewResponse(BaseModel):
    total_known_sha256: int
    total_stored_files: int
    indexed_files: int
    uploaded_files: int
    duplicate_file_paths: int
    recent_indexed_files_24h: int
    recent_uploaded_files_24h: int
    last_indexed_at_utc: str | None
    last_uploaded_at_utc: str | None


class AdminFileItem(BaseModel):
    relative_path: str
    sha256_hex: str
    size_bytes: int
    source_kind: str
    first_seen_at_utc: str
    last_seen_at_utc: str


class AdminFileListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[AdminFileItem]


class AdminCatalogItem(BaseModel):
    relative_path: str
    sha256_hex: str
    size_bytes: int
    media_type: str
    preview_capability: str
    origin_kind: str
    last_observed_origin_kind: str
    provenance_job_name: str | None
    provenance_original_filename: str | None
    first_cataloged_at_utc: str
    last_cataloged_at_utc: str
    extraction_status: str
    extraction_last_attempted_at_utc: str | None
    extraction_last_succeeded_at_utc: str | None
    extraction_last_failed_at_utc: str | None
    extraction_failure_detail: str | None
    preview_status: str
    preview_relative_path: str | None
    preview_last_attempted_at_utc: str | None
    preview_last_succeeded_at_utc: str | None
    preview_last_failed_at_utc: str | None
    preview_failure_detail: str | None
    capture_timestamp_utc: str | None
    camera_make: str | None
    camera_model: str | None
    image_width: int | None
    image_height: int | None
    orientation: int | None
    lens_model: str | None
    exposure_time_s: float | None = None
    f_number: float | None = None
    iso_speed: int | None = None
    focal_length_mm: float | None = None
    focal_length_35mm_mm: int | None = None
    is_favorite: bool = False
    is_archived: bool = False
    is_rejected: bool = False


class AdminCatalogListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[AdminCatalogItem]


class AdminRetryExtractionRequest(BaseModel):
    relative_path: str = Field(min_length=1)


class AdminRetryExtractionResponse(BaseModel):
    item: AdminCatalogItem


class AdminRetryPreviewRequest(BaseModel):
    relative_path: str = Field(min_length=1)


class AdminRetryPreviewResponse(BaseModel):
    item: AdminCatalogItem


class AdminCatalogAssetResponse(BaseModel):
    item: AdminCatalogItem


class AdminCatalogFolderItem(BaseModel):
    """One folder in the catalog folder index.

    `path` is the forward-slash-joined directory portion of the asset's
    relative_path, e.g. ``2026/04/Job_A`` for an asset at
    ``2026/04/Job_A/IMG_0001.jpg``. `depth` is the number of path
    segments (``2026`` → 1, ``2026/04`` → 2, ``2026/04/Job_A`` → 3).
    `direct_count` is the number of assets whose folder exactly equals
    ``path``; `total_count` includes assets in subfolders. Clients use
    this to render a folder tree and show counts at every depth.
    """

    path: str
    depth: int
    direct_count: int
    total_count: int


class AdminCatalogFoldersResponse(BaseModel):
    folders: list[AdminCatalogFolderItem]


class AdminCatalogOrganizationRequest(BaseModel):
    relative_path: str = Field(min_length=1)


class AdminCatalogOrganizationResponse(BaseModel):
    item: AdminCatalogItem


# ----- Phase 3.B: reject queue models -------------------------------------
class AdminCatalogRejectRequest(BaseModel):
    relative_path: str = Field(min_length=1)
    marked_reason: str | None = Field(default=None, max_length=500)


class AdminCatalogRejectResponse(BaseModel):
    relative_path: str
    sha256_hex: str
    marked_at_utc: str
    marked_reason: str | None = None
    is_rejected: bool = True


class AdminCatalogRejectUnmarkResponse(BaseModel):
    relative_path: str
    is_rejected: bool = False


class AdminCatalogRejectQueueItem(BaseModel):
    """A reject-queue row plus the matching catalog item.

    The UI needs thumbnails + filenames to let the reviewer restore specific
    items, so we always ship the catalog item in-band. If a catalog row has
    disappeared under the queue (should not happen outside of a Phase 3.C
    delete), the item is ``None`` and the UI falls back to the bare relative
    path rendered from the queue row.
    """

    relative_path: str
    sha256_hex: str
    marked_at_utc: str
    marked_reason: str | None = None
    item: AdminCatalogItem | None = None


class AdminCatalogRejectQueueResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[AdminCatalogRejectQueueItem]


class AdminBackfillCatalogRequest(BaseModel):
    target_statuses: list[str] = Field(default_factory=lambda: ["pending", "failed"], min_length=1)
    limit: int = Field(default=100, ge=1, le=500)
    origin_kind: str | None = None
    media_type: str | None = None
    preview_capability: str | None = None
    cataloged_since_utc: str | None = None
    cataloged_before_utc: str | None = None


class AdminCatalogBackfillRunSummary(BaseModel):
    backfill_kind: str
    requested_statuses: list[str]
    limit: int
    origin_kind: str | None
    media_type: str | None
    preview_capability: str | None
    cataloged_since_utc: str | None
    cataloged_before_utc: str | None
    selected_count: int
    processed_count: int
    succeeded_count: int
    failed_count: int
    remaining_pending_count: int
    remaining_failed_count: int
    completed_at_utc: str


class AdminBackfillCatalogResponse(BaseModel):
    run: AdminCatalogBackfillRunSummary
    items: list[AdminCatalogItem]


class AdminLatestCatalogBackfillRunsResponse(BaseModel):
    extraction_run: AdminCatalogBackfillRunSummary | None
    preview_run: AdminCatalogBackfillRunSummary | None


class LatestIndexRunResponse(BaseModel):
    scanned_files: int
    indexed_files: int
    new_sha_entries: int
    existing_sha_matches: int
    path_conflicts: int
    errors: int
    completed_at_utc: str


class LatestIndexRunEnvelope(BaseModel):
    latest_run: LatestIndexRunResponse | None


class DuplicateShaGroupItem(BaseModel):
    sha256_hex: str
    file_count: int
    first_seen_at_utc: str
    last_seen_at_utc: str
    relative_paths: list[str]


class DuplicateShaGroupListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[DuplicateShaGroupItem]


class PathConflictItem(BaseModel):
    relative_path: str
    previous_sha256_hex: str
    current_sha256_hex: str
    detected_at_utc: str


class PathConflictListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[PathConflictItem]


class ClientEnrollmentStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REVOKED = "revoked"


class ClientWorkloadStatus(StrEnum):
    IDLE = "idle"
    WORKING = "working"
    WAITING = "waiting"
    BLOCKED = "blocked"


class ClientPresenceStatus(StrEnum):
    ONLINE = "online"
    STALE = "stale"
    UNKNOWN = "unknown"


class BootstrapEnrollRequest(BaseModel):
    client_id: str = Field(min_length=1, max_length=200)
    display_name: str = Field(min_length=1, max_length=200)
    bootstrap_token: str = Field(min_length=1)


class BootstrapEnrollResponse(BaseModel):
    client_id: str
    display_name: str
    enrollment_status: ClientEnrollmentStatus
    auth_token: str | None
    first_seen_at_utc: str
    last_enrolled_at_utc: str


class AdminClientItem(BaseModel):
    client_id: str
    display_name: str
    enrollment_status: ClientEnrollmentStatus
    first_seen_at_utc: str
    last_enrolled_at_utc: str
    approved_at_utc: str | None
    revoked_at_utc: str | None
    auth_token: str | None
    heartbeat_last_seen_at_utc: str | None
    heartbeat_presence_status: str
    heartbeat_daemon_state: str | None
    heartbeat_workload_status: ClientWorkloadStatus | None
    heartbeat_active_job_summary: str | None
    heartbeat_retry_backoff_summary: str | None
    heartbeat_auth_block_reason: str | None
    heartbeat_recent_error_summary: str | None


class AdminClientListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[AdminClientItem]


class AdminClientActionResponse(BaseModel):
    item: AdminClientItem


class HeartbeatActiveJobSummary(BaseModel):
    job_id: int = Field(ge=1)
    media_label: str | None = Field(default=None, max_length=255)
    job_status: str = Field(min_length=1, max_length=128)
    ready_to_upload: int = Field(default=0, ge=0)
    uploaded: int = Field(default=0, ge=0)
    retrying: int = Field(default=0, ge=0)
    total_files: int | None = Field(default=None, ge=0)
    non_terminal_files: int | None = Field(default=None, ge=0)
    error_files: int | None = Field(default=None, ge=0)
    blocking_reason: str | None = Field(default=None, max_length=256)


class HeartbeatRetryBackoffSummary(BaseModel):
    pending_count: int = Field(ge=0)
    next_retry_at_utc: datetime | None = None
    reason: str | None = Field(default=None, max_length=512)


class HeartbeatRecentErrorSummary(BaseModel):
    category: str = Field(min_length=1, max_length=128)
    message: str = Field(min_length=1, max_length=512)
    created_at_utc: datetime


class ClientHeartbeatRequest(BaseModel):
    last_seen_at_utc: datetime
    daemon_state: str = Field(min_length=1, max_length=128)
    workload_status: ClientWorkloadStatus
    active_job: HeartbeatActiveJobSummary | None = None
    retry_backoff: HeartbeatRetryBackoffSummary | None = None
    auth_block_reason: str | None = Field(default=None, max_length=128)
    recent_error: HeartbeatRecentErrorSummary | None = None


class ClientHeartbeatResponse(BaseModel):
    status: str
    client_id: str
    last_seen_at_utc: str
    daemon_state: str
    workload_status: ClientWorkloadStatus


