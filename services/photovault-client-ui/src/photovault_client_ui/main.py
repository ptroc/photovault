"""Entrypoint for photovault-client-ui."""

import os

from photovault_client_ui.app import create_app


def _ui_port() -> int:
    raw = os.environ.get("PHOTOVAULT_CLIENT_UI_PORT", "8888").strip()
    try:
        return int(raw)
    except ValueError:
        return 8888


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=_ui_port())
