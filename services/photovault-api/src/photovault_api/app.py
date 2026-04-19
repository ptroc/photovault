"""Server-side API skeleton for photovault."""

import hashlib
from enum import StrEnum

from fastapi import FastAPI, HTTPException, Request
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


def create_app(initial_known_sha256: set[str] | None = None) -> FastAPI:
    app = FastAPI(title="photovault-api", version="0.1.0")
    app.state.known_sha256 = set(initial_known_sha256 or set())
    app.state.upload_temp: dict[str, bytes] = {}

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/upload/metadata-handshake", response_model=MetadataHandshakeResponse)
    def metadata_handshake(payload: MetadataHandshakeRequest) -> MetadataHandshakeResponse:
        results: list[HandshakeFileResult] = []
        known_sha256: set[str] = app.state.known_sha256

        for file_item in payload.files:
            decision = (
                HandshakeDecision.ALREADY_EXISTS
                if file_item.sha256_hex in known_sha256
                else HandshakeDecision.UPLOAD_REQUIRED
            )
            results.append(
                HandshakeFileResult(
                    client_file_id=file_item.client_file_id,
                    decision=decision,
                )
            )

        return MetadataHandshakeResponse(results=results)

    @app.put("/v1/upload/content/{sha256_hex}", response_model=UploadContentResponse)
    async def upload_content(sha256_hex: str, request: Request) -> UploadContentResponse:
        if len(sha256_hex) != 64:
            raise HTTPException(status_code=400, detail="sha256_hex must be 64 hex characters")

        known_sha256: set[str] = app.state.known_sha256
        if sha256_hex in known_sha256:
            return UploadContentResponse(status="ALREADY_EXISTS")

        raw_size = request.headers.get("x-size-bytes")
        if raw_size is None:
            raise HTTPException(status_code=400, detail="missing x-size-bytes header")
        try:
            expected_size = int(raw_size)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid x-size-bytes header") from exc
        if expected_size < 0:
            raise HTTPException(status_code=400, detail="x-size-bytes must be non-negative")

        content = await request.body()
        if len(content) != expected_size:
            raise HTTPException(status_code=400, detail="payload size does not match x-size-bytes")

        observed_sha = hashlib.sha256(content).hexdigest()
        if observed_sha != sha256_hex:
            raise HTTPException(status_code=400, detail="payload sha256 mismatch")

        app.state.upload_temp[sha256_hex] = content
        return UploadContentResponse(status="STORED_TEMP")

    @app.post("/v1/upload/verify", response_model=VerifyResponse)
    def verify_upload(payload: VerifyRequest) -> VerifyResponse:
        known_sha256: set[str] = app.state.known_sha256
        upload_temp: dict[str, bytes] = app.state.upload_temp

        if payload.sha256_hex in known_sha256:
            return VerifyResponse(status="ALREADY_EXISTS")

        content = upload_temp.get(payload.sha256_hex)
        if content is None:
            return VerifyResponse(status="VERIFY_FAILED")
        if len(content) != payload.size_bytes:
            return VerifyResponse(status="VERIFY_FAILED")
        if hashlib.sha256(content).hexdigest() != payload.sha256_hex:
            return VerifyResponse(status="VERIFY_FAILED")

        known_sha256.add(payload.sha256_hex)
        upload_temp.pop(payload.sha256_hex, None)
        return VerifyResponse(status="VERIFIED")

    return app
