import json

from photovault_clientd.block_devices import (
    BlockDeviceAdapter,
    derive_mount_path,
    is_mountpoint_blocked_by_sources,
)


def test_derive_mount_path_for_partition_device() -> None:
    assert derive_mount_path("/dev/sda1") == "/mnt/sda_1"
    assert derive_mount_path("/dev/sdz12") == "/mnt/sdz_12"


def test_list_external_devices_filters_to_removable_sd_disks_and_partitions() -> None:
    payload = {
        "blockdevices": [
            {
                "name": "sda",
                "path": "/dev/sda",
                "type": "disk",
                "size": 64000000000,
                "rm": 1,
                "tran": "usb",
                "model": "MassStorageClass",
                "vendor": "Generic",
                "children": [
                    {
                        "name": "sda1",
                        "path": "/dev/sda1",
                        "type": "part",
                        "size": 63847792640,
                        "fstype": "exfat",
                        "label": "Untitled",
                        "uuid": "AAAA-BBBB",
                        "mountpoint": "/mnt/sda_1",
                        "mountpoints": ["/mnt/sda_1"],
                    }
                ],
            },
            {
                "name": "mmcblk0",
                "path": "/dev/mmcblk0",
                "type": "disk",
                "size": 32000000000,
                "rm": 0,
                "tran": "",
                "children": [],
            },
        ]
    }

    adapter = BlockDeviceAdapter(command_runner=lambda _args: json.dumps(payload))
    devices = adapter.list_external_devices()

    assert len(devices) == 1
    assert devices[0]["path"] == "/dev/sda"
    assert len(devices[0]["partitions"]) == 1
    partition = devices[0]["partitions"][0]
    assert partition["path"] == "/dev/sda1"
    assert partition["target_mount_path"] == "/mnt/sda_1"
    assert partition["mount_active"] is True
    assert partition["can_mount"] is False
    assert partition["can_unmount"] is True


def test_mount_and_unmount_partition_use_deterministic_mountpoint() -> None:
    observed: list[list[str]] = []

    def runner(args: list[str]) -> str:
        observed.append(args)
        return '{"ok": true}'

    adapter = BlockDeviceAdapter(command_runner=runner)
    mount_outcome = adapter.mount_partition("/dev/sdb1")
    unmount_outcome = adapter.unmount_partition("/dev/sdb1")

    assert mount_outcome["mount_path"] == "/mnt/sdb_1"
    assert unmount_outcome["mount_path"] == "/mnt/sdb_1"
    assert observed[0] == ["mount", "/dev/sdb1", "/mnt/sdb_1"]
    assert observed[1] == ["unmount", "/dev/sdb1", "/mnt/sdb_1"]


def test_mountpoint_block_guard_matches_same_or_child_source_paths() -> None:
    assert is_mountpoint_blocked_by_sources(
        "/mnt/sda_1",
        ["/mnt/sda_1/DCIM/IMG_0001.JPG"],
    )
    assert is_mountpoint_blocked_by_sources("/mnt/sda_1", ["/mnt/sda_1"])
    assert not is_mountpoint_blocked_by_sources(
        "/mnt/sda_1",
        ["/mnt/sdb_1/DCIM/IMG_0002.JPG"],
    )
