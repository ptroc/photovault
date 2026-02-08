"""Entrypoint for photovault-server-ui."""

from photovault_server_ui.app import create_app

if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=9401)
