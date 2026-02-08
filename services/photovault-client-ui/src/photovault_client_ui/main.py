"""Entrypoint for photovault-client-ui."""

from photovault_client_ui.app import create_app

if __name__ == "__main__":
    create_app().run(host="127.0.0.1", port=9201)
