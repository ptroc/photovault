"""Server-side API skeleton for photovault."""

from fastapi import FastAPI


def create_app() -> FastAPI:
    app = FastAPI(title="photovault-api", version="0.1.0")

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
