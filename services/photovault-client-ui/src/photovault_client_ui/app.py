"""SSR control-plane UI for the photovault client."""

from flask import Flask, render_template

DEFAULT_DAEMON_BASE_URL = "http://127.0.0.1:9101"


def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def index() -> str:
        return render_template("index.html", daemon_base_url=DEFAULT_DAEMON_BASE_URL)

    return app
