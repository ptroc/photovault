from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_systemd_units_use_dedicated_user_venv_and_restart_policy() -> None:
    unit_expectations = {
        "photovault-clientd.service": "photovault_clientd.main",
        "photovault-client-ui.service": "photovault_client_ui.main",
        "photovault-api.service": "photovault_api.main",
        "photovault-server-ui.service": "photovault_server_ui.main",
    }

    for unit_name, module in unit_expectations.items():
        content = _read(REPO_ROOT / "deploy" / "systemd" / unit_name)
        assert "User=photovault" in content
        assert "Group=photovault" in content
        assert "ExecStart=/opt/photovault/.venv/bin/python -m " in content
        assert module in content
        assert "Restart=always" in content
        assert "StartLimitBurst=20" in content
        if unit_name == "photovault-clientd.service":
            assert "NoNewPrivileges=false" in content
        else:
            assert "NoNewPrivileges=true" in content


def test_block_device_helper_and_sudoers_are_tightly_scoped() -> None:
    helper = _read(REPO_ROOT / "deploy" / "scripts" / "photovault-blockdev-helper")
    assert "list|mount|unmount" in helper
    assert "mount -o ro,nosuid,nodev,noexec" in helper
    assert "nsenter --mount=\"${HOST_MOUNT_NS}\"" in helper
    assert "PARTITION_RE='^/dev/sd[a-z]+[0-9]+$'" in helper
    assert "MOUNT_RE='^/mnt/sd[a-z]+_[0-9]+$'" in helper

    sudoers = _read(REPO_ROOT / "deploy" / "sudoers" / "photovault-blockdev")
    assert "photovault ALL=(root) NOPASSWD: /usr/local/sbin/photovault-blockdev-helper *" in sudoers


def test_clientd_unit_has_persistent_state_and_env_contract() -> None:
    content = _read(REPO_ROOT / "deploy" / "systemd" / "photovault-clientd.service")
    assert "EnvironmentFile=-/etc/photovault/photovault-clientd.env" in content
    assert "StateDirectory=photovault-clientd" in content
    assert "ReadWritePaths=/var/lib/photovault-clientd /var/log/photovault" in content


def test_bootstrap_playbook_provisions_lifecycle_prerequisites() -> None:
    content = _read(REPO_ROOT / "ansible" / "playbooks" / "bootstrap.yml")
    assert "Ensure photovault group exists" in content
    assert "Ensure photovault user exists" in content
    assert "photovault_env_dir" in content
    assert "clientd_state_dir" in content
    assert "Ensure service environment files exist" in content
    assert "Install systemd unit files" in content
    assert "Remove legacy removable-media systemd hook units" in content
    assert "Remove legacy removable media udev rule" in content
    assert "Install photovault block-device helper" in content
    assert "Install photovault block-device sudoers policy" in content
    assert "udevadm control --reload-rules" in content
    assert "Enable photovault services" in content
    assert "Reload systemd" in content
