"""M0 foundation diagnostics for photovault-clientd."""

from photovault_clientd.db import (
    BOOTSTRAP_RESUME_MAP,
    NON_TERMINAL_FILE_STATUSES,
    TERMINAL_FILE_STATUSES,
    run_state_invariant_checks,
)


def run_m0_foundation_checks(conn) -> dict[str, object]:
    mapped_statuses = {status.value for status in BOOTSTRAP_RESUME_MAP}
    missing_non_terminal = sorted(NON_TERMINAL_FILE_STATUSES - mapped_statuses)
    invalid_terminal = sorted(mapped_statuses & TERMINAL_FILE_STATUSES)

    invariant_issues = run_state_invariant_checks(conn)

    pending_bootstrap = conn.execute(
        "SELECT COUNT(1) FROM bootstrap_queue WHERE processed_at_utc IS NULL;"
    ).fetchone()

    return {
        "resume_map_complete": len(missing_non_terminal) == 0,
        "resume_map_terminal_clean": len(invalid_terminal) == 0,
        "missing_non_terminal_statuses": missing_non_terminal,
        "invalid_terminal_statuses": invalid_terminal,
        "invariants_ok": len(invariant_issues) == 0,
        "invariant_issue_count": len(invariant_issues),
        "invariant_issues": invariant_issues,
        "pending_bootstrap_entries": int(pending_bootstrap[0]) if pending_bootstrap else 0,
    }
