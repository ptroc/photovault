from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient
from photovault_clientd.app import create_app
from photovault_clientd.block_devices import BlockDeviceError, OperatorError
from photovault_clientd.db import create_ingest_job, insert_discovered_files, open_db


class _FakeNetworkManager:
    def ensure_ap_profile(self, *, profile_name: str, ssid: str, password: str) -> dict[str, object]:
        return {
            "created": False,
            "profile_name": profile_name,
            "ap_profile": {
                "profile_name": profile_name,
                "exists": True,
                "active": True,
                "ssid": ssid,
                "autoconnect": "yes",
                "mode": "ap",
                "key_mgmt": "wpa-psk",
            },
        }


class _FakeBlockDeviceAdapter:
    def __init__(self) -> None:
        self.mount_calls: list[str] = []
        self.unmount_calls: list[str] = []
        self.mount_error: BlockDeviceError | None = None
        self.unmount_error: BlockDeviceError | None = None

    def list_external_devices(self) -> list[dict[str, object]]:
        return [
            {
                "name": "sda",
                "path": "/dev/sda",
                "size_bytes": 64_000_000_000,
                "transport": "usb",
                "removable": True,
                "vendor": "Generic",
                "model": "MassStorageClass",
                "partitions": [
                    {
                        "name": "sda1",
                        "path": "/dev/sda1",
                        "size_bytes": 63_000_000_000,
                        "filesystem_type": "exfat",
                        "filesystem_label": "Untitled",
                        "filesystem_uuid": "AAAA-BBBB",
                        "current_mountpoints": [],
                        "target_mount_path": "/mnt/sda_1",
                        "mount_active": False,
                        "can_mount": True,
                        "can_unmount": False,
                    }
                ],
            }
        ]

    def mount_partition(self, device_path: str) -> dict[str, object]:
        if self.mount_error is not None:
            raise self.mount_error
        self.mount_calls.append(device_path)
        return {"device_path": device_path, "mount_path": "/mnt/sda_1", "result": {"ok": True}}

    def unmount_partition(self, device_path: str) -> dict[str, object]:
        if self.unmount_error is not None:
            raise self.unmount_error
        self.unmount_calls.append(device_path)
        return {"device_path": device_path, "mount_path": "/mnt/sda_1", "result": {"ok": True}}


def test_block_devices_inventory_endpoint_returns_external_devices(tmp_path: Path) -> None:
    app = create_app(
        db_path=tmp_path / "state.sqlite3",
        network_manager=_FakeNetworkManager(),
        block_device_adapter=_FakeBlockDeviceAdapter(),
    )
    with TestClient(app) as client:
        response = client.get("/block-devices")
        assert response.status_code == 200
        payload = response.json()
        assert payload["count"] == 1
        assert payload["devices"][0]["path"] == "/dev/sda"
        assert payload["devices"][0]["partitions"][0]["path"] == "/dev/sda1"


def test_block_devices_mount_endpoint_calls_adapter(tmp_path: Path) -> None:
    adapter = _FakeBlockDeviceAdapter()
    app = create_app(
        db_path=tmp_path / "state.sqlite3",
        network_manager=_FakeNetworkManager(),
        block_device_adapter=adapter,
    )
    with TestClient(app) as client:
        response = client.post("/block-devices/mount", json={"device_path": "/dev/sda1"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["device_path"] == "/dev/sda1"
        assert payload["mount_path"] == "/mnt/sda_1"
    assert adapter.mount_calls == ["/dev/sda1"]


def test_block_devices_mount_maps_invalid_input_to_422(tmp_path: Path) -> None:
    adapter = _FakeBlockDeviceAdapter()
    adapter.mount_error = BlockDeviceError(
        OperatorError(
            code="BLOCK_DEVICE_INVALID_INPUT",
            message="invalid",
            detail="bad path",
            suggestion="use /dev/sda1",
        )
    )
    app = create_app(
        db_path=tmp_path / "state.sqlite3",
        network_manager=_FakeNetworkManager(),
        block_device_adapter=adapter,
    )
    with TestClient(app) as client:
        response = client.post("/block-devices/mount", json={"device_path": "/dev/invalid"})
        assert response.status_code == 422
        payload = response.json()
        assert payload["detail"]["code"] == "BLOCK_DEVICE_INVALID_INPUT"


def test_block_devices_unmount_is_blocked_when_non_terminal_source_uses_mount(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    adapter = _FakeBlockDeviceAdapter()
    app = create_app(
        db_path=db_path,
        network_manager=_FakeNetworkManager(),
        block_device_adapter=adapter,
    )
    with TestClient(app) as client:
        conn = open_db(db_path)
        now = datetime.now(UTC).isoformat()
        try:
            job_id = create_ingest_job(conn, "mounted-card", now)
            insert_discovered_files(conn, job_id, ["/mnt/sda_1/DCIM/IMG_0001.JPG"], now)
            conn.commit()
        finally:
            conn.close()

        response = client.post("/block-devices/unmount", json={"device_path": "/dev/sda1"})
        assert response.status_code == 409
        payload = response.json()
        assert payload["detail"]["code"] == "BLOCK_DEVICE_BUSY"
        assert adapter.unmount_calls == []


def test_block_devices_unmount_calls_adapter_when_mount_not_in_use(tmp_path: Path) -> None:
    adapter = _FakeBlockDeviceAdapter()
    app = create_app(
        db_path=tmp_path / "state.sqlite3",
        network_manager=_FakeNetworkManager(),
        block_device_adapter=adapter,
    )
    with TestClient(app) as client:
        response = client.post("/block-devices/unmount", json={"device_path": "/dev/sda1"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["device_path"] == "/dev/sda1"
        assert payload["mount_path"] == "/mnt/sda_1"
    assert adapter.unmount_calls == ["/dev/sda1"]
