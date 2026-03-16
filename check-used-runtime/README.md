# Check Used Runtime

Scans all Microsoft Fabric workspaces and notebooks to report which Spark runtime version each notebook is using.

## What it does

- Lists all workspaces accessible to the authenticated user
- Retrieves the workspace default Spark runtime from workspace settings
- For each notebook, fetches its definition and checks:
  - If the notebook has an **explicit runtime** set in its `.platform` config
  - If the notebook uses a **custom environment**, resolves the environment's Spark runtime version
  - Otherwise, reports the **workspace default** runtime
- Outputs results to the console and saves them to `fabric_notebook_runtimes.csv`

## Prerequisites

```
pip install requests pandas azure-identity
```

## Configuration

Edit the top of `checkUsedRuntime.py`:

| Variable | Description |
|----------|-------------|
| `RETRY_TIMEOUT` | Override the server's polling retry interval (in seconds). Set to `None` to use the server's `Retry-After` header. |

## Authentication

The script uses `InteractiveBrowserCredential` from `azure-identity`. When you run it:

1. A browser window opens automatically
2. Sign in with your Microsoft account (MFA supported)
3. The script acquires a token and proceeds

No app registration, client IDs, or secrets are needed.

## Usage

```
cd check-used-runtime
python checkUsedRuntime.py
```

## Output

The script prints progress logs and a summary table to the console, and saves a CSV file:

| Column | Description |
|--------|-------------|
| `workspace` | Workspace name |
| `notebook` | Notebook name |
| `runtime` | Runtime version (e.g. `1.2 (env: test)`, `workspace default (1.3)`) |
| `environment` | Environment name, if the notebook uses one |
