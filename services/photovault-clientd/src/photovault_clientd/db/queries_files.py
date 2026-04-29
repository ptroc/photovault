"""Compatibility barrel for clientd file query helpers."""

from . import queries_common as _queries_common
from . import queries_detected_media as _queries_detected_media
from . import queries_file_progress as _queries_file_progress
from . import queries_job_views as _queries_job_views
from . import queries_recovery as _queries_recovery


def _reexport(module) -> list[str]:
    names = [name for name in vars(module) if not name.startswith("_")]
    globals().update({name: getattr(module, name) for name in names})
    return names


__all__ = []
for _module in (
    _queries_common,
    _queries_detected_media,
    _queries_file_progress,
    _queries_job_views,
    _queries_recovery,
):
    __all__.extend(_reexport(_module))

del _module
del _queries_common
del _queries_detected_media
del _queries_file_progress
del _queries_job_views
del _queries_recovery
del _reexport
