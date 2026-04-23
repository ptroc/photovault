#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/deploy_rpi.sh [options]

Deploy the current photovault workspace to a Raspberry Pi using rsync,
refresh the remote virtualenv dependencies, restart photovault services,
wait for post-restart health with retries, and run explicit M4 storage/index smoke
validation.

Options:
  --host <host>            Remote host (default: 10.100.1.95)
  --user <user>            Remote SSH user (default: root)
  --key <path>             SSH private key
                           (default: ~/.ssh/id_rsa_theworlt_bitbucket_key)
  --target <path>          Remote install root (default: /opt/photovault)
  --venv <path>            Remote venv path (default: /opt/photovault/.venv)
  --service <name>         Restart only one service; may be passed multiple times
  --skip-install           Skip remote pip install/upgrade
  --skip-restart           Skip service restart
  --skip-health            Skip post-deploy health checks
  --skip-smoke             Skip post-deploy M4 storage/index smoke checks
  --dry-run                Show rsync changes without applying them
  -h, --help               Show this help

Examples:
  scripts/deploy_rpi.sh
  scripts/deploy_rpi.sh --service photovault-client-ui.service
  scripts/deploy_rpi.sh --dry-run
EOF
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEFAULT_HOST="10.100.1.95"
DEFAULT_USER="root"
DEFAULT_KEY="${HOME}/.ssh/id_rsa_theworlt_bitbucket_key"
DEFAULT_TARGET="/opt/photovault"
DEFAULT_VENV="${DEFAULT_TARGET}/.venv"
DEFAULT_STORAGE_ROOT="/storage/photovault"
DEFAULT_HEALTH_ATTEMPTS=12
DEFAULT_HEALTH_SLEEP_SECONDS=2
DEFAULT_SERVICES=(
  "photovault-clientd.service"
  "photovault-client-ui.service"
  "photovault-api.service"
  "photovault-server-ui.service"
)

HOST="${DEFAULT_HOST}"
USER_NAME="${DEFAULT_USER}"
SSH_KEY="${DEFAULT_KEY}"
TARGET_DIR="${DEFAULT_TARGET}"
VENV_DIR="${DEFAULT_VENV}"
DRY_RUN=0
SKIP_INSTALL=0
SKIP_RESTART=0
SKIP_HEALTH=0
SKIP_SMOKE=0
HEALTH_ATTEMPTS="${DEFAULT_HEALTH_ATTEMPTS}"
HEALTH_SLEEP_SECONDS="${DEFAULT_HEALTH_SLEEP_SECONDS}"
SERVICES=()

while (($# > 0)); do
  case "$1" in
    --host)
      HOST="$2"
      shift 2
      ;;
    --user)
      USER_NAME="$2"
      shift 2
      ;;
    --key)
      SSH_KEY="$2"
      shift 2
      ;;
    --target)
      TARGET_DIR="$2"
      shift 2
      ;;
    --venv)
      VENV_DIR="$2"
      shift 2
      ;;
    --service)
      SERVICES+=("$2")
      shift 2
      ;;
    --skip-install)
      SKIP_INSTALL=1
      shift
      ;;
    --skip-restart)
      SKIP_RESTART=1
      shift
      ;;
    --skip-health)
      SKIP_HEALTH=1
      shift
      ;;
    --skip-smoke)
      SKIP_SMOKE=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ! -f "${SSH_KEY}" ]]; then
  echo "SSH key not found: ${SSH_KEY}" >&2
  exit 1
fi

if [[ ${#SERVICES[@]} -eq 0 ]]; then
  SERVICES=("${DEFAULT_SERVICES[@]}")
fi

SSH_OPTS=(
  -i "${SSH_KEY}"
  -o BatchMode=yes
  -o StrictHostKeyChecking=no
)

RSYNC_OPTS=(
  -az
  --delete
  --exclude .git/
  --exclude .pytest_cache/
  --exclude .ruff_cache/
  --exclude .venv/
  --exclude node_modules/
  --exclude playwright-report/
  --exclude test-results/
  --exclude __pycache__/
  --exclude .DS_Store
)

if ((DRY_RUN)); then
  RSYNC_OPTS+=(--dry-run --itemize-changes)
fi

remote() {
  ssh "${SSH_OPTS[@]}" "${USER_NAME}@${HOST}" "$@"
}

joined_services() {
  local joined=""
  local service
  for service in "${SERVICES[@]}"; do
    if [[ -n "${joined}" ]]; then
      joined+=" "
    fi
    joined+="${service}"
  done
  printf '%s' "${joined}"
}

preflight_remote_storage() {
  remote "set -euo pipefail
API_ENV='/etc/photovault/photovault-api.env'
DEFAULT_STORAGE_ROOT='${DEFAULT_STORAGE_ROOT}'
if [[ ! -f \"\$API_ENV\" ]]; then
  echo 'missing /etc/photovault/photovault-api.env' >&2
  exit 1
fi
mkdir -p \"\$DEFAULT_STORAGE_ROOT\"
chown photovault:photovault \"\$DEFAULT_STORAGE_ROOT\"
chmod 0750 \"\$DEFAULT_STORAGE_ROOT\"
STORAGE_ROOT=\$(grep '^PHOTOVAULT_API_STORAGE_ROOT=' \"\$API_ENV\" | tail -n 1 | cut -d= -f2-)
if [[ -z \"\${STORAGE_ROOT:-}\" ]]; then
  if grep -q '^PHOTOVAULT_API_STORAGE_ROOT=' \"\$API_ENV\"; then
    sed -i \"s|^PHOTOVAULT_API_STORAGE_ROOT=.*|PHOTOVAULT_API_STORAGE_ROOT=\$DEFAULT_STORAGE_ROOT|\" \"\$API_ENV\"
  else
    printf 'PHOTOVAULT_API_STORAGE_ROOT=%s\n' \"\$DEFAULT_STORAGE_ROOT\" >> \"\$API_ENV\"
  fi
  STORAGE_ROOT=\"\$DEFAULT_STORAGE_ROOT\"
fi
if [[ ! -d \"\$STORAGE_ROOT\" ]]; then
  echo \"storage root does not exist: \$STORAGE_ROOT\" >&2
  exit 1
fi
if ! su -s /bin/sh photovault -c \"test -w '\$STORAGE_ROOT'\"; then
  echo \"storage root is not writable by photovault: \$STORAGE_ROOT\" >&2
  exit 1
fi
printf 'storage-root=%s\n' \"\$STORAGE_ROOT\""
}

run_remote_health_checks() {
  local services_joined
  services_joined="$(joined_services)"
  remote "set -euo pipefail
systemctl is-active ${services_joined} >/dev/null
curl -fsS http://127.0.0.1:9101/healthz
echo
curl -fsS http://127.0.0.1:80/ >/dev/null && echo 'client-ui: ok'
curl -fsS http://127.0.0.1:9301/healthz
echo
curl -fsS http://127.0.0.1:9401/ >/dev/null && echo 'server-ui: ok'"
}

wait_for_remote_health_checks() {
  local attempt=1
  local output=""

  while ((attempt <= HEALTH_ATTEMPTS)); do
    if output="$(run_remote_health_checks 2>&1)"; then
      printf '%s\n' "${output}"
      return 0
    fi

    echo "health check attempt ${attempt}/${HEALTH_ATTEMPTS} not ready yet"
    printf '%s\n' "${output}" >&2

    if ((attempt == HEALTH_ATTEMPTS)); then
      echo "health checks did not pass after ${HEALTH_ATTEMPTS} attempts" >&2
      return 1
    fi

    sleep "${HEALTH_SLEEP_SECONDS}"
    attempt=$((attempt + 1))
  done
}

run_remote_m4_smoke() {
  remote "set -euo pipefail
API_ENV='/etc/photovault/photovault-api.env'
STORAGE_ROOT=\$(grep '^PHOTOVAULT_API_STORAGE_ROOT=' \"\$API_ENV\" | tail -n 1 | cut -d= -f2-)
cd '${TARGET_DIR}'
'${VENV_DIR}/bin/python' scripts/m4_smoke_check.py --storage-root \"\$STORAGE_ROOT\""
}

echo "==> Ensuring target directory exists on ${HOST}"
remote "mkdir -p '${TARGET_DIR}'"

echo "==> Syncing workspace to ${USER_NAME}@${HOST}:${TARGET_DIR}"
rsync "${RSYNC_OPTS[@]}" -e "ssh ${SSH_OPTS[*]}" "${ROOT_DIR}/" "${USER_NAME}@${HOST}:${TARGET_DIR}/"

if ((DRY_RUN)); then
  echo "==> Dry run complete; no remote changes applied"
  exit 0
fi

echo "==> Running remote storage/config preflight"
preflight_remote_storage

if ((SKIP_INSTALL == 0)); then
  echo "==> Refreshing remote virtualenv and dependencies"
  remote "cd '${TARGET_DIR}' && python3 -m venv '${VENV_DIR}' && '${VENV_DIR}/bin/pip' install --upgrade pip && '${VENV_DIR}/bin/pip' install -r requirements.txt -r requirements-dev.txt"
fi

if ((SKIP_RESTART == 0)); then
  echo "==> Restarting services"
  remote "systemctl daemon-reload && systemctl restart ${SERVICES[*]}"
  remote "systemctl --no-pager --full status ${SERVICES[*]}"
fi

if ((SKIP_HEALTH == 0)); then
  echo "==> Running remote health checks (up to ${HEALTH_ATTEMPTS} attempts, ${HEALTH_SLEEP_SECONDS}s apart)"
  wait_for_remote_health_checks
fi

if ((SKIP_SMOKE == 0)); then
  echo "==> Running remote M4 smoke checks"
  run_remote_m4_smoke
fi

echo "==> Deploy complete"
