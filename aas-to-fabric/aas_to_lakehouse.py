#!/usr/bin/env python3
"""
AAS to Fabric Lakehouse Migration Tool
========================================

Full migration pipeline: reads a model from Azure Analysis Services and
recreates it in Fabric with data flowing through a Lakehouse.

The pipeline creates three artefacts in the target Fabric workspace:
  1. **Lakehouse**         — Delta table storage for imported data
  2. **Dataflow Gen 2**    — M queries that import data from AAS → Lakehouse
  3. **Semantic Model**    — Tabular model on top of the Lakehouse SQL endpoint

Usage:
    # Full migration (creates Lakehouse + Dataflow + Semantic Model)
    python aas_to_lakehouse.py -c config.json

    # Use an existing BIM file (skip AAS read)
    python aas_to_lakehouse.py -c config.json --from-bim model.bim

    # Run only a specific step
    python aas_to_lakehouse.py -c config.json --step lakehouse
    python aas_to_lakehouse.py -c config.json --step dataflow
    python aas_to_lakehouse.py -c config.json --step semantic-model

    # Overwrite existing artefacts
    python aas_to_lakehouse.py -c config.json --overwrite
"""

import json
import logging
import argparse
import sys
import time
import uuid
import base64
import requests
from pathlib import Path
from typing import Optional

# Reuse auth, TOM, and BIM utilities from the existing migration script
from aas_to_fabric import (
    load_config,
    get_access_token,
    SCOPE_AAS,
    SCOPE_POWERBI,
    init_tom,
    read_model_from_aas,
    transform_bim,
    inspect_bim,
    print_model_summary,
    export_bim,
    _resolve_workspace_id,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("aas_to_lakehouse")

FABRIC_API = "https://api.fabric.microsoft.com/v1"


# ---------------------------------------------------------------------------
# Lakehouse Management
# ---------------------------------------------------------------------------

def create_lakehouse(
    workspace_id: str,
    lakehouse_name: str,
    token: str,
) -> dict:
    """
    Create a Lakehouse in the target Fabric workspace.

    Returns the Lakehouse item dict including its ID.
    If a Lakehouse with the same name already exists, returns it.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Check if Lakehouse already exists
    resp = requests.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses",
        headers=headers,
    )
    resp.raise_for_status()
    existing = [
        lh for lh in resp.json().get("value", [])
        if lh["displayName"] == lakehouse_name
    ]
    if existing:
        lh = existing[0]
        log.info(
            "Lakehouse '%s' already exists (ID: %s)",
            lakehouse_name,
            lh["id"],
        )
        return lh

    # Create new Lakehouse
    body = {"displayName": lakehouse_name}
    resp = requests.post(
        f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses",
        headers=headers,
        json=body,
    )

    if resp.status_code == 201:
        lh = resp.json()
        log.info(
            "Created Lakehouse '%s' (ID: %s)",
            lakehouse_name,
            lh["id"],
        )
        return lh

    if resp.status_code == 202:
        # Async — poll for completion
        operation_url = resp.headers.get("Location") or resp.headers.get(
            "Operation-Location"
        )
        log.info("Lakehouse creation initiated (async). Polling...")
        for _ in range(60):
            time.sleep(5)
            if not operation_url:
                break
            status_resp = requests.get(
                operation_url,
                headers={"Authorization": f"Bearer {token}"},
            )
            if status_resp.status_code == 200:
                status = status_resp.json()
                state = status.get("status", "Unknown")
                log.info("  Lakehouse creation state: %s", state)
                if state in ("Succeeded", "Completed"):
                    break
                if state in ("Failed", "Cancelled"):
                    raise RuntimeError(f"Lakehouse creation failed: {status}")

        # Fetch the created Lakehouse by name
        resp2 = requests.get(
            f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses",
            headers=headers,
        )
        resp2.raise_for_status()
        created = [
            lh for lh in resp2.json().get("value", [])
            if lh["displayName"] == lakehouse_name
        ]
        if created:
            log.info(
                "Created Lakehouse '%s' (ID: %s)",
                lakehouse_name,
                created[0]["id"],
            )
            return created[0]
        raise RuntimeError("Lakehouse was created but could not be found")

    resp.raise_for_status()
    return resp.json()


def get_lakehouse_sql_endpoint(
    workspace_id: str,
    lakehouse_id: str,
    token: str,
    poll_timeout: int = 300,
) -> str:
    """
    Retrieve the SQL analytics endpoint connection string for a Lakehouse.

    The SQL endpoint may take some time to provision after Lakehouse creation,
    so this function polls until it is ready.

    Returns the SQL analytics endpoint connection string.
    """
    headers = {"Authorization": f"Bearer {token}"}
    start = time.time()

    while True:
        resp = requests.get(
            f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}",
            headers=headers,
        )
        resp.raise_for_status()
        lh = resp.json()

        props = lh.get("properties") or {}
        sql_props = props.get("sqlEndpointProperties") or {}
        provisioning = sql_props.get("provisioningStatus", "")
        conn_string = sql_props.get("connectionString", "")

        if provisioning == "Success" and conn_string:
            log.info("SQL analytics endpoint ready: %s", conn_string)
            return conn_string

        elapsed = time.time() - start
        if elapsed > poll_timeout:
            raise RuntimeError(
                f"SQL analytics endpoint did not become ready within "
                f"{poll_timeout}s. Last status: {provisioning}"
            )

        log.info(
            "  SQL endpoint provisioning: %s (%.0fs elapsed)...",
            provisioning or "Pending",
            elapsed,
        )
        time.sleep(10)


# ---------------------------------------------------------------------------
# Dataflow Gen 2
# ---------------------------------------------------------------------------

def _build_m_query(table_name: str, aas_server: str, aas_database: str) -> str:
    """Build a Power Query M expression to import a table from AAS.

    Uses a DAX EVALUATE query which reliably works regardless of the
    AAS model navigation hierarchy.
    """
    server = aas_server.replace('"', '""')
    database = aas_database.replace('"', '""')
    tname = table_name.replace("'", "''")

    sq = "'"  # single-quote character for M DAX syntax
    return (
        f'let\n'
        f'    Source = AnalysisServices.Database("{server}", "{database}", '
        f'[Query="EVALUATE {sq}{tname}{sq}", Implementation="2.0"])\n'
        f'in\n'
        f'    Source'
    )


def build_dataflow_definition(
    table_names: list[str],
    aas_server: str,
    aas_database: str,
    workspace_id: str,
    lakehouse_id: str,
    lakehouse_name: str,
) -> dict:
    """
    Build a Dataflow Gen 2 definition using the correct Fabric API parts:
      - mashup.pq           — Power Query M section document
      - queryMetadata.json   — per-query metadata with output destinations

    Returns a dict with 'mashup_pq' and 'query_metadata' keys.
    """
    # Build the Power Query M section document
    queries = []
    query_metadata_entries = {}

    for table_name in table_names:
        m_expr = _build_m_query(table_name, aas_server, aas_database)

        # Sanitize table name for PQ identifier (wrap in #"..." if it has spaces)
        pq_name = f'#"{table_name}"' if " " in table_name else table_name

        queries.append(f"shared {pq_name} = {m_expr};")

        # Lakehouse destination table name (spaces → underscores)
        delta_table_name = table_name.replace(" ", "_")

        query_metadata_entries[table_name] = {
            "queryId": str(uuid.uuid4()),
            "queryName": table_name,
            "loadEnabled": True,
            "destinationSettings": {
                "type": "lakehouse",
                "workspaceId": workspace_id,
                "artifactId": lakehouse_id,
                "lakehouseName": lakehouse_name,
                "schemaName": "dbo",
                "tableName": delta_table_name,
                "updateMethod": "Replace",
            },
        }

    mashup_pq = "section Section1;\n\n" + "\n\n".join(queries)

    query_metadata = {
        "formatVersion": "202502",
        "computeEngineSettings": {
            "allowFastCopy": False,
        },
        "name": None,
        "allowNativeQueries": False,
        "queriesMetadata": query_metadata_entries,
    }

    return {
        "mashup_pq": mashup_pq,
        "query_metadata": query_metadata,
    }


def deploy_dataflow(
    workspace_id: str,
    dataflow_name: str,
    dataflow_content: dict,
    token: str,
    overwrite: bool = False,
) -> dict:
    """
    Deploy a Dataflow Gen 2 to the Fabric workspace.

    Strategy:
      1. Try creating with inline definition via Items API
      2. If that fails, create empty then update definition

    Returns the created/updated Dataflow item dict.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Check if dataflow already exists
    resp = requests.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/items",
        headers=headers,
        params={"type": "Dataflow"},
    )
    resp.raise_for_status()
    existing = [
        item for item in resp.json().get("value", [])
        if item.get("displayName") == dataflow_name
    ]

    if existing and not overwrite:
        raise RuntimeError(
            f"Dataflow '{dataflow_name}' already exists. Use --overwrite."
        )

    # Delete existing if overwriting
    if existing and overwrite:
        item_id = existing[0]["id"]
        log.info("Deleting existing Dataflow '%s' (%s)...", dataflow_name, item_id)
        del_resp = requests.delete(
            f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}",
            headers=headers,
        )
        if del_resp.status_code in (200, 204):
            log.info("Existing Dataflow deleted. Waiting for name to become available...")
            # Fabric may take some time to release the display name
            time.sleep(10)
        else:
            log.warning(
                "Could not delete existing Dataflow: %s %s",
                del_resp.status_code,
                del_resp.text,
            )

    # Encode the two definition parts using correct Fabric paths
    mashup_b64 = base64.b64encode(
        dataflow_content["mashup_pq"].encode("utf-8")
    ).decode("ascii")
    metadata_b64 = base64.b64encode(
        json.dumps(dataflow_content["query_metadata"], indent=2).encode("utf-8")
    ).decode("ascii")

    definition_parts = [
        {
            "path": "mashup.pq",
            "payload": mashup_b64,
            "payloadType": "InlineBase64",
        },
        {
            "path": "queryMetadata.json",
            "payload": metadata_b64,
            "payloadType": "InlineBase64",
        },
    ]

    # --- Attempt 1: Create with inline definition ---
    create_body = {
        "displayName": dataflow_name,
        "type": "Dataflow",
        "definition": {
            "parts": definition_parts,
        },
    }

    log.info("Deploying Dataflow Gen 2 '%s'...", dataflow_name)

    # Retry loop for ItemDisplayNameNotAvailableYet after deletion
    for attempt in range(6):
        resp = requests.post(
            f"{FABRIC_API}/workspaces/{workspace_id}/items",
            headers=headers,
            json=create_body,
        )

        if resp.status_code == 400 and "NotAvailableYet" in (resp.text or ""):
            log.info(
                "  Name not available yet (attempt %d/6). Waiting 10s...",
                attempt + 1,
            )
            time.sleep(10)
            continue
        break

    if resp.status_code in (201, 200):
        result = resp.json()
        log.info(
            "Dataflow '%s' created with definition (ID: %s)",
            dataflow_name,
            result.get("id", "unknown"),
        )
        return result

    if resp.status_code == 202:
        result = _poll_operation(resp, token)
        log.info("Dataflow '%s' created successfully", dataflow_name)
        # Fetch the item to get its ID
        resp_items = requests.get(
            f"{FABRIC_API}/workspaces/{workspace_id}/items",
            headers=headers,
            params={"type": "Dataflow"},
        )
        resp_items.raise_for_status()
        df_items = [
            i for i in resp_items.json().get("value", [])
            if i.get("displayName") == dataflow_name
        ]
        if df_items:
            return df_items[0]
        return result or {"displayName": dataflow_name}

    # --- Attempt 2: Create empty, then update definition ---
    log.info(
        "Inline definition creation returned %s (%s). "
        "Trying create-then-update approach...",
        resp.status_code,
        resp.text[:200] if resp.text else "",
    )

    create_body_simple = {
        "displayName": dataflow_name,
        "type": "Dataflow",
    }

    resp2 = requests.post(
        f"{FABRIC_API}/workspaces/{workspace_id}/items",
        headers=headers,
        json=create_body_simple,
    )

    if resp2.status_code == 202:
        _poll_operation(resp2, token)
        resp_items = requests.get(
            f"{FABRIC_API}/workspaces/{workspace_id}/items",
            headers=headers,
            params={"type": "Dataflow"},
        )
        resp_items.raise_for_status()
        df_items = [
            i for i in resp_items.json().get("value", [])
            if i.get("displayName") == dataflow_name
        ]
        if not df_items:
            raise RuntimeError("Dataflow created but not found")
        item_id = df_items[0]["id"]
    elif resp2.status_code in (200, 201):
        item_id = resp2.json().get("id")
    else:
        log.error(
            "Failed to create Dataflow: %s %s",
            resp2.status_code,
            resp2.text,
        )
        raise RuntimeError(f"Failed to create Dataflow: {resp2.status_code}")

    log.info("Empty Dataflow created (ID: %s). Updating definition...", item_id)

    # Update definition with correct parts
    update_body = {
        "definition": {
            "parts": definition_parts,
        }
    }

    resp3 = requests.post(
        f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/updateDefinition",
        headers=headers,
        json=update_body,
    )

    if resp3.status_code in (200, 204):
        log.info("Dataflow definition updated successfully")
    elif resp3.status_code == 202:
        _poll_operation(resp3, token)
        log.info("Dataflow definition updated (async)")
    else:
        log.warning(
            "Definition update returned %s: %s",
            resp3.status_code,
            resp3.text[:300] if resp3.text else "",
        )

    return {"id": item_id, "displayName": dataflow_name}


def _poll_operation(resp, token: str, timeout: int = 300) -> Optional[dict]:
    """Poll an async Fabric operation until completion."""
    operation_url = resp.headers.get("Location") or resp.headers.get(
        "Operation-Location"
    )
    if not operation_url:
        return None

    start = time.time()
    while time.time() - start < timeout:
        time.sleep(5)
        status_resp = requests.get(
            operation_url,
            headers={"Authorization": f"Bearer {token}"},
        )
        if status_resp.status_code == 200:
            status = status_resp.json()
            state = status.get("status", "Unknown")
            log.info("  Operation state: %s", state)
            if state in ("Succeeded", "Completed"):
                return status
            if state in ("Failed", "Cancelled"):
                raise RuntimeError(f"Operation failed: {status}")
        elif status_resp.status_code == 202:
            log.info("  Still in progress...")
    raise RuntimeError(f"Operation timed out after {timeout}s")


# ---------------------------------------------------------------------------
# Semantic Model (pointing to Lakehouse)
# ---------------------------------------------------------------------------

def repoint_bim_to_lakehouse(
    bim_json: str,
    sql_endpoint: str,
    lakehouse_name: str,
) -> str:
    """
    Transform the BIM JSON so that every partition points to the Lakehouse
    SQL analytics endpoint instead of the original data source.

    Each table's partition becomes an M expression:
        let
            Source = Sql.Database("{sql_endpoint}", "{lakehouse_name}"),
            Data = Source{[Schema="dbo", Item="TableName"]}[Data]
        in
            Data

    Also removes legacy dataSources since M expressions are self-contained.
    """
    bim = json.loads(bim_json)
    model = bim.get("model", {})

    repointed = 0
    for table in model.get("tables", []):
        table_name = table.get("name", "")
        if not table_name:
            continue

        # Delta table name matches what the Dataflow writes (spaces → underscores)
        delta_table_name = table_name.replace(" ", "_")

        m_expr = (
            f'let\n'
            f'    Source = Sql.Database("{sql_endpoint}", "{lakehouse_name}"),\n'
            f'    Data = Source{{[Schema="dbo", Item="{delta_table_name}"]}}[Data]\n'
            f'in\n'
            f'    Data'
        )

        table["partitions"] = [
            {
                "name": table_name,
                "mode": "import",
                "source": {
                    "type": "m",
                    "expression": m_expr,
                },
            }
        ]
        repointed += 1

    # Remove legacy dataSources
    if "dataSources" in model:
        del model["dataSources"]

    log.info(
        "Repointed %d table(s) to Lakehouse SQL endpoint: %s / %s",
        repointed,
        sql_endpoint,
        lakehouse_name,
    )

    return json.dumps(bim, indent=2, ensure_ascii=False)


def deploy_semantic_model(
    workspace_id: str,
    workspace_name: str,
    model_name: str,
    bim_json: str,
    token: str,
    overwrite: bool = False,
):
    """
    Deploy the semantic model to Fabric via the Items API.

    Reuses the same approach as deploy_to_fabric_rest from aas_to_fabric.py.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Check for existing
    pbi_api = "https://api.powerbi.com/v1.0/myorg"
    resp = requests.get(
        f"{pbi_api}/groups/{workspace_id}/datasets",
        headers=headers,
    )
    resp.raise_for_status()
    existing = [
        d for d in resp.json().get("value", [])
        if d["name"] == model_name
    ]

    if existing and not overwrite:
        raise RuntimeError(
            f"Semantic model '{model_name}' already exists. Use --overwrite."
        )

    if existing and overwrite:
        ds_id = existing[0]["id"]
        log.info(
            "Deleting existing semantic model '%s' (%s)...",
            model_name,
            ds_id,
        )
        del_resp = requests.delete(
            f"{pbi_api}/groups/{workspace_id}/datasets/{ds_id}",
            headers=headers,
        )
        if del_resp.status_code in (200, 204):
            log.info("Existing semantic model deleted")
            time.sleep(2)

    # Deploy via Fabric Items API
    bim_b64 = base64.b64encode(bim_json.encode("utf-8")).decode("ascii")
    pbism_content = json.dumps({"version": "1.0", "settings": {}})
    pbism_b64 = base64.b64encode(pbism_content.encode("utf-8")).decode("ascii")

    create_body = {
        "displayName": model_name,
        "type": "SemanticModel",
        "definition": {
            "parts": [
                {
                    "path": "definition.pbism",
                    "payload": pbism_b64,
                    "payloadType": "InlineBase64",
                },
                {
                    "path": "model.bim",
                    "payload": bim_b64,
                    "payloadType": "InlineBase64",
                },
            ]
        },
    }

    log.info("Deploying semantic model '%s'...", model_name)
    resp = requests.post(
        f"{FABRIC_API}/workspaces/{workspace_id}/items",
        headers=headers,
        json=create_body,
    )

    if resp.status_code == 201:
        result = resp.json()
        log.info(
            "Semantic model '%s' deployed (ID: %s)",
            model_name,
            result.get("id", "unknown"),
        )
        return

    if resp.status_code == 202:
        _poll_operation(resp, token)
        log.info(
            "Semantic model '%s' deployed to '%s'",
            model_name,
            workspace_name,
        )
        return

    log.error("Fabric API error: %s %s", resp.status_code, resp.text)
    raise RuntimeError(f"Deployment failed: {resp.status_code} - {resp.text}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Migrate an AAS model to Fabric via Lakehouse: "
            "creates Lakehouse, Dataflow Gen 2, and Semantic Model."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--config",
        "-c",
        default="config.json",
        help="Path to configuration JSON file (default: config.json)",
    )
    parser.add_argument(
        "--from-bim",
        metavar="BIM_FILE",
        help="Use an existing BIM file instead of reading from AAS.",
    )
    parser.add_argument(
        "--step",
        choices=["lakehouse", "dataflow", "semantic-model", "all"],
        default="all",
        help=(
            "Run only a specific step: "
            "'lakehouse' (create Lakehouse), "
            "'dataflow' (deploy Dataflow Gen 2), "
            "'semantic-model' (deploy Semantic Model pointing to Lakehouse), "
            "'all' (default — run all steps)."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing Dataflow and Semantic Model.",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        metavar="TABLE",
        help="Only include these tables.",
    )
    parser.add_argument(
        "--exclude-tables",
        nargs="+",
        metavar="TABLE",
        help="Exclude these tables.",
    )
    parser.add_argument(
        "--auth",
        choices=["interactive", "service_principal", "default"],
        help="Override the authentication method.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging.",
    )

    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # ── Load config ────────────────────────────────────────────────
    config_path = Path(args.config)
    if not config_path.exists():
        log.error("Config file not found: %s", config_path)
        sys.exit(1)

    config = load_config(str(config_path))

    if args.auth:
        config.setdefault("auth", {})["method"] = args.auth

    source = config.get("source", {})
    options = config.get("options", {})
    lh_cfg = config.get("lakehouse_migration", {})

    # Validate lakehouse_migration section
    workspace_name = lh_cfg.get("workspace_name")
    if not workspace_name:
        # Fall back to target workspace from the direct-migration config
        target = config.get("target", {})
        xmla = target.get("xmla_endpoint", "")
        if "/myorg/" in xmla:
            workspace_name = xmla.split("/myorg/")[-1].strip("/")
        workspace_name = workspace_name or target.get("workspace_name")
    if not workspace_name:
        log.error(
            "lakehouse_migration.workspace_name (or target.xmla_endpoint) "
            "is required."
        )
        sys.exit(1)

    lakehouse_name = lh_cfg.get("lakehouse_name", "AAS_Lakehouse")
    dataflow_name = lh_cfg.get("dataflow_name", "AAS_Import")
    model_name = lh_cfg.get("semantic_model_name", "AAS_Model_Lakehouse")
    aas_server = lh_cfg.get("aas_server") or source.get("server", "")
    aas_database = lh_cfg.get("aas_database") or source.get("database", "")

    # Strip :rw/:ro from AAS server for M queries
    import re
    aas_server_clean = re.sub(r":(rw|ro)$", "", aas_server)

    # Apply table filter overrides
    if args.tables:
        options["include_tables"] = args.tables
    if args.exclude_tables:
        options["exclude_tables"] = args.exclude_tables

    run_all = args.step == "all"

    # ── Get BIM JSON (needed for table list and semantic model) ────
    bim_json: Optional[str] = None

    if args.from_bim:
        bim_path = Path(args.from_bim)
        if not bim_path.exists():
            log.error("BIM file not found: %s", bim_path)
            sys.exit(1)
        bim_json = bim_path.read_text(encoding="utf-8")
        log.info("Loaded BIM from %s (%d chars)", bim_path, len(bim_json))
    else:
        if not aas_server or not aas_database:
            log.error("source.server and source.database are required in config")
            sys.exit(1)
        token_aas = get_access_token(config, scope=SCOPE_AAS)
        bim_json = read_model_from_aas(aas_server, aas_database, token_aas)

    # Apply transformations (table filtering, compat upgrade, etc.)
    bim_json = transform_bim(bim_json, options)

    # Extract table names from the (filtered) BIM
    bim_data = json.loads(bim_json)
    table_names = [
        t["name"]
        for t in bim_data.get("model", {}).get("tables", [])
        if t.get("name")
    ]

    summary = inspect_bim(bim_json)
    print_model_summary(summary)

    if not table_names:
        log.error("No tables found in the model. Nothing to migrate.")
        sys.exit(1)

    log.info("Tables to migrate: %s", ", ".join(table_names))

    # ── Acquire Power BI / Fabric token ────────────────────────────
    token = get_access_token(config, scope=SCOPE_POWERBI)

    # ── Resolve workspace ──────────────────────────────────────────
    workspace_id = _resolve_workspace_id(workspace_name, token)

    # ==================================================================
    # STEP 1: Create Lakehouse
    # ==================================================================
    lakehouse_id = lh_cfg.get("lakehouse_id")  # allow pre-set ID
    sql_endpoint = lh_cfg.get("sql_endpoint")   # allow pre-set endpoint

    if run_all or args.step == "lakehouse":
        log.info("=" * 60)
        log.info("STEP 1: Create Lakehouse '%s'", lakehouse_name)
        log.info("=" * 60)

        lh = create_lakehouse(workspace_id, lakehouse_name, token)
        lakehouse_id = lh["id"]

        # Get SQL analytics endpoint
        sql_endpoint = get_lakehouse_sql_endpoint(
            workspace_id, lakehouse_id, token
        )
        log.info(
            "Lakehouse ready: %s (SQL: %s)",
            lakehouse_name,
            sql_endpoint,
        )

    if not lakehouse_id:
        log.error(
            "Lakehouse ID is required. Run with --step lakehouse first, "
            "or set lakehouse_migration.lakehouse_id in config."
        )
        sys.exit(1)

    # ==================================================================
    # STEP 2: Deploy Dataflow Gen 2
    # ==================================================================
    if run_all or args.step == "dataflow":
        log.info("=" * 60)
        log.info("STEP 2: Deploy Dataflow Gen 2 '%s'", dataflow_name)
        log.info("=" * 60)

        if not aas_server_clean or not aas_database:
            log.error(
                "AAS server and database are required for the Dataflow. "
                "Set source.server and source.database in config."
            )
            sys.exit(1)

        df_content = build_dataflow_definition(
            table_names=table_names,
            aas_server=aas_server_clean,
            aas_database=aas_database,
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            lakehouse_name=lakehouse_name,
        )

        log.info("Dataflow M document preview:")
        log.info(df_content["mashup_pq"][:500])

        deploy_dataflow(
            workspace_id=workspace_id,
            dataflow_name=dataflow_name,
            dataflow_content=df_content,
            token=token,
            overwrite=args.overwrite,
        )

    # ==================================================================
    # STEP 3: Deploy Semantic Model pointing to Lakehouse
    # ==================================================================
    if run_all or args.step == "semantic-model":
        log.info("=" * 60)
        log.info("STEP 3: Deploy Semantic Model '%s'", model_name)
        log.info("=" * 60)

        if not sql_endpoint:
            if not lakehouse_id:
                log.error("Cannot deploy semantic model without Lakehouse info.")
                sys.exit(1)
            sql_endpoint = get_lakehouse_sql_endpoint(
                workspace_id, lakehouse_id, token
            )

        # Repoint partitions to Lakehouse SQL endpoint
        bim_lakehouse = repoint_bim_to_lakehouse(
            bim_json, sql_endpoint, lakehouse_name
        )

        # Show final model summary
        final_summary = inspect_bim(bim_lakehouse)
        print_model_summary(final_summary)

        deploy_semantic_model(
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            model_name=model_name,
            bim_json=bim_lakehouse,
            token=token,
            overwrite=args.overwrite,
        )

    # ==================================================================
    # Done
    # ==================================================================
    log.info("=" * 60)
    log.info("Migration complete!")
    log.info("=" * 60)
    log.info("")
    log.info("Next steps:")
    log.info(
        "  1. Configure AAS data source credentials in the Dataflow settings"
    )
    log.info(
        "  2. Run the Dataflow '%s' to load data into Lakehouse '%s'",
        dataflow_name,
        lakehouse_name,
    )
    log.info(
        "  3. Configure Lakehouse credentials in Semantic Model '%s' settings",
        model_name,
    )
    log.info(
        "  4. Refresh the Semantic Model to pick up Lakehouse data"
    )
    log.info(
        "  5. (Optional) Set up a Fabric pipeline to orchestrate "
        "Dataflow → Semantic Model refresh"
    )


if __name__ == "__main__":
    main()
