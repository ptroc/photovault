# Scripts

## Raspberry Pi deploy

Use [`deploy_rpi.sh`](./deploy_rpi.sh) to sync the current workspace to the Raspberry Pi,
refresh the remote Python environment, restart photovault services, and run basic health checks.

Examples:

```bash
scripts/deploy_rpi.sh
scripts/deploy_rpi.sh --service photovault-client-ui.service
scripts/deploy_rpi.sh --dry-run
```
