"""Compatibility barrel for client UI view models."""

from .view_models_jobs import (
    _annotate_job_record,
    _derive_file_m2_view,
    _derive_job_m2_view,
    _derive_job_operator_view,
    _filter_jobs,
    _job_filter_key,
    _job_phase_label,
)
from .view_models_overview import (
    _block_partition_ingest_prefill,
    _build_ingest_gate,
    _build_overview_metrics,
    _daemon_health_label,
    _dependency_health_label,
    _derive_client_auth_guidance,
    _derive_daemon_progress_view,
    _derive_state_guidance,
    _format_size_bytes,
    _summarize_recent_events,
)

__all__ = [
    "_annotate_job_record",
    "_block_partition_ingest_prefill",
    "_build_ingest_gate",
    "_build_overview_metrics",
    "_daemon_health_label",
    "_dependency_health_label",
    "_derive_client_auth_guidance",
    "_derive_daemon_progress_view",
    "_derive_file_m2_view",
    "_derive_job_m2_view",
    "_derive_job_operator_view",
    "_derive_state_guidance",
    "_filter_jobs",
    "_format_size_bytes",
    "_job_filter_key",
    "_job_phase_label",
    "_summarize_recent_events",
]
