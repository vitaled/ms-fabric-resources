#!/usr/bin/env python3
"""
AAS to Fabric Semantic Model Migration Tool
============================================

Reads a tabular model from Azure Analysis Services (AAS) and recreates it
as a semantic model in Microsoft Fabric / Power BI Premium via the XMLA endpoint.

Two deployment strategies:
  1. TOM-based (pythonnet) — full-fidelity model migration via .NET TOM libraries.
     Works locally on Windows where the .NET AMO/TOM packages are installed.
  2. BIM export only — exports the model definition to a .bim JSON file that can
     be imported into Power BI Desktop or deployed with other tooling.

Usage:
    # Full migration (read AAS → deploy to Fabric)
    python aas_to_fabric.py --config config.json

    # Export model to BIM file only (no Fabric deployment)
    python aas_to_fabric.py --config config.json --export-only

    # Deploy an existing BIM file to Fabric
    python aas_to_fabric.py --config config.json --from-bim model.bim

    # Interactive authentication (default)
    python aas_to_fabric.py --config config.json --auth interactive

    # Service principal authentication
    python aas_to_fabric.py --config config.json --auth service_principal
"""

import json
import logging
import argparse
import sys
import os
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("aas_to_fabric")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_config(path: str) -> dict:
    """Load configuration from a JSON file."""
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    log.info("Loaded configuration from %s", path)
    return cfg


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

# AAS and Fabric/Power BI require different token audiences
SCOPE_AAS = "https://*.asazure.windows.net/.default"
SCOPE_POWERBI = "https://analysis.windows.net/powerbi/api/.default"

# Cache the credential object so interactive login only happens once
_credential_cache = None


def _get_credential(config: dict):
    """Build and cache an Azure credential based on config."""
    global _credential_cache
    if _credential_cache is not None:
        return _credential_cache

    from azure.identity import (
        InteractiveBrowserCredential,
        ClientSecretCredential,
        DefaultAzureCredential,
    )

    auth_cfg = config.get("auth", {})
    method = auth_cfg.get("method", "interactive")

    if method == "service_principal":
        tenant_id = auth_cfg["tenant_id"]
        client_id = auth_cfg["client_id"]
        client_secret = auth_cfg["client_secret"]
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
        log.info("Authenticating with service principal (tenant=%s)", tenant_id)

    elif method == "interactive":
        tenant_id = auth_cfg.get("tenant_id") or None
        credential = InteractiveBrowserCredential(tenant_id=tenant_id)
        log.info("Authenticating interactively (browser)")

    elif method == "default":
        credential = DefaultAzureCredential()
        log.info("Authenticating with DefaultAzureCredential")

    else:
        raise ValueError(f"Unknown auth method: {method}")

    _credential_cache = credential
    return credential


def get_access_token(config: dict, scope: str = SCOPE_AAS) -> str:
    """
    Obtain an Azure AD access token.

    Args:
        config: Configuration dict with auth settings.
        scope:  Token audience/scope. Use SCOPE_AAS for AAS connections,
                SCOPE_POWERBI for Fabric/Power BI XMLA connections.
    """
    credential = _get_credential(config)
    token = credential.get_token(scope)
    log.info("Access token acquired for scope %s", scope)
    return token.token


# ---------------------------------------------------------------------------
# TOM Helpers (pythonnet / .NET)
# ---------------------------------------------------------------------------

_tom_module = None


def init_tom():
    """
    Load the Microsoft.AnalysisServices.Tabular .NET assembly via pythonnet.

    Returns the TOM namespace module so callers can use TOM.Server(), etc.

    Prerequisites:
      - pythonnet installed  (pip install pythonnet)
      - AMO/TOM NuGet packages extracted  (see setup_dotnet.py)
    """
    global _tom_module
    if _tom_module is not None:
        return _tom_module

    # Configure pythonnet to use .NET Framework (netfx) runtime on Windows.
    # This must happen BEFORE importing clr.
    from clr_loader import get_netfx
    import pythonnet
    try:
        runtime = get_netfx()
        pythonnet.set_runtime(runtime)
    except Exception:
        pass  # runtime may already be set

    try:
        import clr  # pythonnet
    except ImportError:
        log.error(
            "pythonnet is not installed. Run:  pip install pythonnet\n"
            "Then run setup_dotnet.py to download the AMO/TOM .NET libraries."
        )
        raise

    # Try to find the AMO/TOM DLLs.  The setup_dotnet.py script places them
    # in a  dotnet_libs/  subfolder next to this script.
    lib_dir = Path(__file__).parent / "dotnet_libs"
    if lib_dir.exists():
        sys.path.insert(0, str(lib_dir))
        for dll in lib_dir.glob("*.dll"):
            try:
                clr.AddReference(str(dll))
            except Exception:
                pass  # skip non-.NET DLLs

    # Add explicit references
    try:
        clr.AddReference("Microsoft.AnalysisServices.Tabular")
        clr.AddReference("Microsoft.AnalysisServices")
    except Exception as exc:
        log.error(
            "Could not load AMO/TOM assemblies. Make sure you have run:\n"
            "  python setup_dotnet.py\n"
            "Error: %s",
            exc,
        )
        raise

    import Microsoft.AnalysisServices.Tabular as TOM  # type: ignore

    _tom_module = TOM
    log.info("TOM .NET libraries loaded successfully")
    return TOM


def _build_connection_string(endpoint: str, token: str, is_powerbi: bool = False) -> str:
    """Build an MSOLAP connection string with AAD token auth.

    Args:
        endpoint:   Server address (AAS or Fabric XMLA)
        token:      Azure AD access token
        is_powerbi: True for Fabric/Power BI XMLA endpoints
    """
    if is_powerbi:
        # Power BI / Fabric XMLA requires Integrated Security=ClaimsToken
        # for the .NET Framework TOM library to accept an external bearer token.
        return (
            f"Provider=MSOLAP;"
            f"Data Source={endpoint};"
            f"Password={token};"
            f"Integrated Security=ClaimsToken;"
            f"Persist Security Info=True;"
            f"Impersonation Level=Impersonate"
        )
    # AAS endpoints work with Password= directly
    return (
        f"Provider=MSOLAP;"
        f"Data Source={endpoint};"
        f"Password={token};"
        f"Persist Security Info=True;"
        f"Impersonation Level=Impersonate"
    )


# ---------------------------------------------------------------------------
# Read model from AAS
# ---------------------------------------------------------------------------

def read_model_from_aas(server: str, database: str, token: str) -> str:
    """
    Connect to Azure Analysis Services and extract the full model definition
    as a BIM JSON string (Tabular Model definition).

    Args:
        server:   AAS server address (e.g. asazure://westeurope.asazure.windows.net/myserver)
        database: Name of the AAS database / model
        token:    Azure AD access token

    Returns:
        BIM JSON string representing the full model definition.
    """
    TOM = init_tom()

    conn_str = _build_connection_string(server, token)
    srv = TOM.Server()

    log.info("Connecting to AAS: %s", server)
    srv.Connect(conn_str)

    try:
        db = srv.Databases.FindByName(database)
        if db is None:
            available = [srv.Databases[i].Name for i in range(srv.Databases.Count)]
            raise ValueError(
                f"Database '{database}' not found on {server}. "
                f"Available databases: {available}"
            )

        log.info("Found database '%s' (compat level %s)", db.Name, db.CompatibilityLevel)
        log.info(
            "  Tables: %d | Relationships: %d | Data Sources: %d",
            db.Model.Tables.Count,
            db.Model.Relationships.Count,
            db.Model.DataSources.Count,
        )

        # Serialize the database to BIM JSON (full TMSL model definition)
        bim_json = TOM.JsonSerializer.SerializeDatabase(db)

        log.info("Model serialized to BIM JSON (%d chars)", len(bim_json))
        return bim_json

    finally:
        srv.Disconnect()
        log.info("Disconnected from AAS")


# ---------------------------------------------------------------------------
# Post-process the BIM (optional transformations)
# ---------------------------------------------------------------------------

def _repoint_tables_to_aas(model: dict, aas_server: str, aas_database: str):
    """
    Replace partitions in every non-calculated table with an M expression
    that imports data from an AAS server via the AnalysisServices.Database
    Power Query connector.
    """
    import re

    # Strip :rw / :ro suffix — not needed for M import queries
    aas_server_clean = re.sub(r":(rw|ro)$", "", aas_server)

    repointed = 0
    for table in model.get("tables", []):
        table_name = table.get("name", "")
        if not table_name:
            continue

        # Skip calculated tables — they don't import data
        if _is_calculated_table(table):
            continue

        m_expr = _build_aas_m_expression(
            aas_server_clean, aas_database, table_name
        )

        # Replace all partitions with a single M partition
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

    # Remove legacy dataSources — no longer needed with M partitions
    if "dataSources" in model:
        del model["dataSources"]

    log.info(
        "Repointed %d table(s) to AAS via M expressions: %s / %s",
        repointed,
        aas_server_clean,
        aas_database,
    )


def _is_calculated_table(table: dict) -> bool:
    """Return True if the table is a DAX calculated table."""
    partitions = table.get("partitions", [])
    if not partitions:
        return False
    # A calculated table has exactly one partition of type "calculated"
    return any(
        p.get("source", {}).get("type") == "calculated" for p in partitions
    )


def _build_aas_m_expression(
    server: str, database: str, table_name: str
) -> str:
    """
    Build a Power Query M expression that imports a single table from an
    Analysis Services model using table navigation (not DAX EVALUATE).

    Navigation-based import preserves column names exactly as defined in
    the model, avoiding the column-name prefix issue of DAX EVALUATE.
    """
    # Escape double-quotes for M string literals
    srv = server.replace('"', '""')
    db = database.replace('"', '""')
    tbl = table_name.replace('"', '""')

    return (
        f'let\n'
        f'    Source = AnalysisServices.Database("{srv}", "{db}", '
        f'[Implementation="2.0"]),\n'
        f'    Table = Source{{[Id="{tbl}", Kind="Table"]}}[Data]\n'
        f'in\n'
        f'    Table'
    )


def _convert_legacy_partitions(model: dict, options: dict):
    """
    Detect legacy "query" type partitions (with a dataSource reference)
    and convert them to empty M partitions so Fabric can at least accept
    the model definition.  This is a safety net — if repoint_to_aas has
    already converted the partitions, this is a no-op.
    """
    converted = 0
    for table in model.get("tables", []):
        if _is_calculated_table(table):
            continue

        new_parts = []
        needs_conversion = False
        for part in table.get("partitions", []):
            src = part.get("source", {})
            if src.get("type") == "query" and src.get("dataSource"):
                needs_conversion = True
                break  # will replace all partitions for this table
            new_parts.append(part)

        if needs_conversion:
            table_name = table.get("name", "partition")
            # Create a placeholder M partition (returns empty table)
            # The user can later configure a real data source in Fabric.
            table["partitions"] = [
                {
                    "name": table_name,
                    "mode": "import",
                    "source": {
                        "type": "m",
                        "expression": (
                            'let\n'
                            '    Source = #table({}, {})\n'
                            'in\n'
                            '    Source'
                        ),
                    },
                }
            ]
            converted += 1

    if converted:
        # Remove legacy dataSources — they belong to the old partitions
        if "dataSources" in model:
            del model["dataSources"]
        log.info(
            "Converted %d table(s) with legacy 'query' partitions to "
            "empty M partitions (configure data source in Fabric, or "
            "re-run with --repoint-to-aas to import from AAS)",
            converted,
        )


def transform_bim(bim_json: str, options: dict) -> str:
    """
    Apply optional transformations to the BIM JSON before deployment.

    Transformations:
      - include_tables:    Only keep these tables (list of names)
      - exclude_tables:    Remove these tables (list of names)
      - strip_partitions:  Remove partition definitions (Fabric will use its own)
      - strip_data_sources: Remove legacy data source objects
      - update_compatibility_level: Override the compat level (e.g. 1604 for Fabric)

    When tables are filtered, relationships that reference removed tables are
    also removed automatically.
    """
    bim = json.loads(bim_json)
    model = bim.get("model", {})

    # ── Table filtering ────────────────────────────────────────────
    include = options.get("include_tables")
    exclude = options.get("exclude_tables")

    if include and exclude:
        log.warning(
            "Both include_tables and exclude_tables are set. "
            "include_tables takes precedence."
        )
        exclude = None

    tables = model.get("tables", [])

    if include:
        include_set = {t.lower() for t in include}
        kept = [t for t in tables if t.get("name", "").lower() in include_set]
        removed = [t.get("name") for t in tables if t.get("name", "").lower() not in include_set]
        model["tables"] = kept
        if removed:
            log.info(
                "Table filter (include): kept %d of %d tables. "
                "Removed: %s",
                len(kept), len(tables), ", ".join(removed),
            )

    elif exclude:
        exclude_set = {t.lower() for t in exclude}
        kept = [t for t in tables if t.get("name", "").lower() not in exclude_set]
        removed = [t.get("name") for t in tables if t.get("name", "").lower() in exclude_set]
        model["tables"] = kept
        if removed:
            log.info(
                "Table filter (exclude): kept %d of %d tables. "
                "Removed: %s",
                len(kept), len(tables), ", ".join(removed),
            )

    # If tables were filtered, clean up orphaned relationships
    if include or exclude:
        remaining_table_names = {t.get("name") for t in model.get("tables", [])}
        relationships = model.get("relationships", [])
        valid_rels = []
        for rel in relationships:
            from_table = rel.get("fromTable", "")
            to_table = rel.get("toTable", "")
            if from_table in remaining_table_names and to_table in remaining_table_names:
                valid_rels.append(rel)
            else:
                log.info(
                    "Removed orphaned relationship: %s → %s",
                    from_table, to_table,
                )
        model["relationships"] = valid_rels

        # Also clean up RLS role table permissions for removed tables
        for role in model.get("roles", []):
            if "tablePermissions" in role:
                role["tablePermissions"] = [
                    tp for tp in role["tablePermissions"]
                    if tp.get("name", "") in remaining_table_names
                ]

    # ── Repoint partitions to AAS (M expressions) ──────────────
    repoint = options.get("repoint_to_aas")
    if repoint:
        aas_server = repoint.get("server", "")
        aas_database = repoint.get("database", "")
        if not aas_server or not aas_database:
            log.warning(
                "repoint_to_aas requires 'server' and 'database'. Skipping."
            )
        else:
            _repoint_tables_to_aas(model, aas_server, aas_database)

    # ── Auto-convert legacy partitions for Fabric ──────────────
    # Fabric V3 semantic models do NOT support legacy "query" type
    # partitions with provider data sources.  If any remain after the
    # repoint stage, convert them to M partitions now.
    _convert_legacy_partitions(model, options)

    if options.get("strip_partitions"):
        for table in model.get("tables", []):
            if "partitions" in table:
                del table["partitions"]
        log.info("Stripped partition definitions from all tables")

    if options.get("strip_data_sources"):
        if "dataSources" in model:
            del model["dataSources"]
            log.info("Stripped data source definitions")

    compat = options.get("update_compatibility_level")
    if compat:
        bim["compatibilityLevel"] = int(compat)
        log.info("Updated compatibility level to %s", compat)

    # Fabric requires V3 models (compatibility level >= 1604).
    # Auto-upgrade if the model is older.
    current_compat = bim.get("compatibilityLevel", 0)
    if current_compat < 1604:
        log.info(
            "Upgrading compatibility level from %s to 1604 (required by Fabric)",
            current_compat,
        )
        bim["compatibilityLevel"] = 1604

    # Fabric requires defaultPowerBIDataSourceVersion in the model
    if "defaultPowerBIDataSourceVersion" not in model:
        model["defaultPowerBIDataSourceVersion"] = "powerBI_V3"
        log.info("Set defaultPowerBIDataSourceVersion = powerBI_V3")

    # Power BI / Fabric only allows RLS roles with "read" permission.
    # AAS models may have roles with other permissions (e.g. "administrator").
    # Fix them to "read" to avoid deployment errors.
    for role in model.get("roles", []):
        if role.get("modelPermission", "").lower() != "read":
            old_perm = role.get("modelPermission", "none")
            role["modelPermission"] = "read"
            log.info(
                "Changed role '%s' permission from '%s' to 'read' "
                "(Fabric RLS requirement)",
                role.get("name", "unknown"),
                old_perm,
            )

    return json.dumps(bim, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Export BIM to file
# ---------------------------------------------------------------------------

def export_bim(bim_json: str, output_path: str):
    """Save the BIM JSON to a local file."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(bim_json, encoding="utf-8")
    log.info("BIM model exported to %s", path.resolve())


# ---------------------------------------------------------------------------
# Deploy to Fabric / Power BI via XMLA (TOM)
# ---------------------------------------------------------------------------

def deploy_to_fabric_xmla(
    xmla_endpoint: str,
    dataset_name: str,
    bim_json: str,
    token: str,
    overwrite: bool = False,
):
    """
    Deploy a BIM model to Fabric / Power BI Premium via the XMLA read/write endpoint.

    This creates (or replaces) a semantic model in the target workspace.

    Args:
        xmla_endpoint: Fabric XMLA endpoint
                       (e.g. powerbi://api.powerbi.com/v1.0/myorg/WorkspaceName)
        dataset_name:  Name for the new semantic model
        bim_json:      BIM JSON string
        token:         Azure AD access token
        overwrite:     If True, replace an existing model with the same name
    """
    TOM = init_tom()

    conn_str = _build_connection_string(xmla_endpoint, token, is_powerbi=True)
    srv = TOM.Server()

    log.info("Connecting to Fabric XMLA endpoint: %s", xmla_endpoint)
    srv.Connect(conn_str)

    try:
        # Check if a dataset with this name already exists
        existing_db = srv.Databases.FindByName(dataset_name)

        if existing_db and not overwrite:
            raise RuntimeError(
                f"Semantic model '{dataset_name}' already exists in the workspace. "
                f"Use --overwrite to replace it."
            )

        # Deserialize BIM JSON into a TOM Database object
        db = TOM.JsonSerializer.DeserializeDatabase(bim_json)
        db.Name = dataset_name
        db.ID = dataset_name.replace(" ", "_").replace("-", "_")

        if existing_db:
            log.info("Replacing existing semantic model '%s'...", dataset_name)
            existing_db.Drop()

        # Add the new database and update
        srv.Databases.Add(db)
        db.Update(TOM.UpdateOptions.ExpandFull)

        log.info(
            "Successfully deployed semantic model '%s' to Fabric\n"
            "  Tables: %d | Relationships: %d",
            dataset_name,
            db.Model.Tables.Count,
            db.Model.Relationships.Count,
        )

    finally:
        srv.Disconnect()
        log.info("Disconnected from Fabric XMLA endpoint")


# ---------------------------------------------------------------------------
# Deploy via Power BI REST API (primary deployment method)
# ---------------------------------------------------------------------------

PBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"


def _resolve_workspace_id(workspace_name: str, token: str) -> str:
    """Resolve a workspace name to its GUID using the Power BI REST API."""
    import requests

    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(
        f"{PBI_API_BASE}/groups",
        headers=headers,
        params={"$filter": f"name eq '{workspace_name}'"},
    )
    resp.raise_for_status()
    groups = resp.json().get("value", [])
    if not groups:
        raise RuntimeError(
            f"Workspace '{workspace_name}' not found. "
            f"Check the workspace name and your permissions."
        )
    workspace_id = groups[0]["id"]
    log.info("Resolved workspace '%s' → %s", workspace_name, workspace_id)
    return workspace_id


def deploy_to_fabric_rest(
    workspace_name: str,
    dataset_name: str,
    bim_json: str,
    token: str,
    overwrite: bool = False,
):
    """
    Deploy a model to Fabric using the Power BI REST API.

    Strategy:
      1. Resolve workspace ID from name
      2. Use TMSL createOrReplace via the XMLA Execute REST endpoint
      3. Falls back to Import API if XMLA execute is not available

    Args:
        workspace_name: Fabric workspace name
        dataset_name:   Name for the semantic model
        bim_json:       BIM JSON string
        token:          Azure AD access token (Power BI scope)
        overwrite:      If True, replace an existing model
    """
    import requests
    import time

    headers = {"Authorization": f"Bearer {token}"}

    # 1. Resolve workspace ID
    workspace_id = _resolve_workspace_id(workspace_name, token)

    # 2. Check if model already exists
    resp = requests.get(
        f"{PBI_API_BASE}/groups/{workspace_id}/datasets",
        headers=headers,
    )
    resp.raise_for_status()
    existing_datasets = resp.json().get("value", [])
    existing = [d for d in existing_datasets if d["name"] == dataset_name]

    if existing and not overwrite:
        raise RuntimeError(
            f"Semantic model '{dataset_name}' already exists in workspace "
            f"'{workspace_name}'. Use --overwrite to replace it."
        )

    # If overwriting, delete the existing dataset first
    if existing and overwrite:
        ds_id = existing[0]["id"]
        log.info("Deleting existing semantic model '%s' (%s)...", dataset_name, ds_id)
        del_resp = requests.delete(
            f"{PBI_API_BASE}/groups/{workspace_id}/datasets/{ds_id}",
            headers=headers,
        )
        if del_resp.status_code not in (200, 204):
            log.warning(
                "Could not delete existing model: %s %s",
                del_resp.status_code,
                del_resp.text,
            )
        else:
            log.info("Existing model deleted")
            time.sleep(2)  # Brief pause for service to process deletion

    # 3. Deploy via Fabric Items API — create semantic model from definition
    log.info("Deploying '%s' via Fabric Items API...", dataset_name)

    import base64

    # Encode the BIM JSON as base64 for the Fabric API
    bim_b64 = base64.b64encode(bim_json.encode("utf-8")).decode("ascii")

    # The Fabric Items API for SemanticModel requires two parts:
    #   - definition.pbism  (a JSON manifest pointing to the model)
    #   - model.bim         (the actual tabular model definition)
    pbism_content = json.dumps(
        {
            "version": "1.0",
            "settings": {},
        }
    )
    pbism_b64 = base64.b64encode(pbism_content.encode("utf-8")).decode("ascii")

    fabric_api = "https://api.fabric.microsoft.com/v1"

    # Create semantic model item with inline definition
    create_body = {
        "displayName": dataset_name,
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

    resp = requests.post(
        f"{fabric_api}/workspaces/{workspace_id}/items",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=create_body,
    )

    if resp.status_code == 201:
        # Synchronous success
        result = resp.json()
        log.info(
            "Successfully deployed semantic model '%s' (ID: %s) "
            "to workspace '%s'",
            result.get("displayName", dataset_name),
            result.get("id", "unknown"),
            workspace_name,
        )
        return

    if resp.status_code == 202:
        # Async operation — poll for completion
        operation_url = resp.headers.get("Location") or resp.headers.get("Operation-Location")
        log.info("Creation initiated (async). Polling for completion...")

        for _ in range(60):  # Up to 5 minutes
            time.sleep(5)
            if operation_url:
                status_resp = requests.get(
                    operation_url,
                    headers={"Authorization": f"Bearer {token}"},
                )
            else:
                break

            if status_resp.status_code == 200:
                status = status_resp.json()
                state = status.get("status", "Unknown")
                log.info("  Operation state: %s", state)
                if state in ("Succeeded", "Completed"):
                    log.info(
                        "Successfully deployed semantic model '%s' "
                        "to workspace '%s'",
                        dataset_name,
                        workspace_name,
                    )
                    return
                if state in ("Failed", "Cancelled"):
                    log.error("Operation failed: %s", json.dumps(status, indent=2))
                    raise RuntimeError(f"Deployment failed: {status}")
            elif status_resp.status_code == 202:
                log.info("  Still in progress...")
                continue

        raise RuntimeError("Deployment timed out after 5 minutes")

    # Error
    log.error("Fabric API failed: %s %s", resp.status_code, resp.text)
    raise RuntimeError(f"Deployment failed: {resp.status_code} - {resp.text}")


# ---------------------------------------------------------------------------
# Model Inspection Utilities
# ---------------------------------------------------------------------------

def inspect_bim(bim_json: str) -> dict:
    """Parse a BIM JSON and return a summary of the model contents."""
    bim = json.loads(bim_json)
    model = bim.get("model", {})
    tables = model.get("tables", [])

    summary = {
        "name": bim.get("name", "unknown"),
        "compatibilityLevel": bim.get("compatibilityLevel"),
        "tables": [],
        "relationships": len(model.get("relationships", [])),
        "dataSources": len(model.get("dataSources", [])),
        "roles": len(model.get("roles", [])),
        "perspectives": len(model.get("perspectives", [])),
        "expressions": len(model.get("expressions", [])),
    }

    for table in tables:
        t_info = {
            "name": table.get("name"),
            "columns": len(table.get("columns", [])),
            "measures": len(table.get("measures", [])),
            "hierarchies": len(table.get("hierarchies", [])),
            "partitions": len(table.get("partitions", [])),
            "calculatedColumns": sum(
                1 for c in table.get("columns", [])
                if c.get("type") == "calculated"
            ),
        }
        summary["tables"].append(t_info)

    return summary


def print_model_summary(summary: dict):
    """Pretty-print a model summary."""
    print("\n" + "=" * 60)
    print(f"  Model: {summary['name']}")
    print(f"  Compatibility Level: {summary['compatibilityLevel']}")
    print("=" * 60)
    print(f"  Tables:        {len(summary['tables'])}")
    print(f"  Relationships: {summary['relationships']}")
    print(f"  Data Sources:  {summary['dataSources']}")
    print(f"  Roles:         {summary['roles']}")
    print(f"  Perspectives:  {summary['perspectives']}")
    print(f"  Expressions:   {summary['expressions']}")
    print("-" * 60)

    for t in summary["tables"]:
        cols = t["columns"]
        calc = t["calculatedColumns"]
        meas = t["measures"]
        hier = t["hierarchies"]
        parts = t["partitions"]
        print(
            f"  {t['name']:40s}  "
            f"cols={cols:<3d} calc={calc:<3d} measures={meas:<3d} "
            f"hier={hier:<2d} partitions={parts}"
        )

    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migrate an AAS tabular model to Fabric as a semantic model.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--config", "-c",
        default="config.json",
        help="Path to configuration JSON file (default: config.json)",
    )

    # Mode flags
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--export-only",
        action="store_true",
        help="Only export the AAS model to a .bim file; do not deploy to Fabric.",
    )
    mode.add_argument(
        "--from-bim",
        metavar="BIM_FILE",
        help="Deploy an existing .bim file to Fabric (skip AAS read).",
    )
    mode.add_argument(
        "--inspect",
        metavar="BIM_FILE",
        help="Inspect a .bim file and print a summary (no connections made).",
    )

    # Overrides
    parser.add_argument(
        "--auth",
        choices=["interactive", "service_principal", "default"],
        help="Override the authentication method from config.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite an existing semantic model in Fabric.",
    )
    parser.add_argument(
        "--deploy-method",
        choices=["rest", "xmla"],
        default="rest",
        help="Deployment method: 'rest' (REST API, default) or 'xmla' (TOM via XMLA).",
    )
    parser.add_argument(
        "--output", "-o",
        metavar="FILE",
        help="Override BIM output path (used with --export-only).",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        metavar="TABLE",
        help="Only include these tables (by name). Overrides config include_tables.",
    )
    parser.add_argument(
        "--exclude-tables",
        nargs="+",
        metavar="TABLE",
        help="Exclude these tables (by name). Overrides config exclude_tables.",
    )
    parser.add_argument(
        "--repoint-to-aas",
        action="store_true",
        help=(
            "Convert partitions to M expressions that import data from the "
            "original AAS server (source.server / source.database in config). "
            "On refresh in Fabric the data will be pulled from AAS."
        ),
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging.",
    )

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # ── Inspect mode (no auth needed) ──────────────────────────────
    if args.inspect:
        bim_path = Path(args.inspect)
        if not bim_path.exists():
            log.error("BIM file not found: %s", bim_path)
            sys.exit(1)
        bim_json = bim_path.read_text(encoding="utf-8")
        summary = inspect_bim(bim_json)
        print_model_summary(summary)
        return

    # ── Load config ────────────────────────────────────────────────
    config_path = Path(args.config)
    if not config_path.exists():
        log.error("Config file not found: %s", config_path)
        sys.exit(1)

    config = load_config(str(config_path))

    # Apply CLI overrides
    if args.auth:
        config.setdefault("auth", {})["method"] = args.auth

    options = config.get("options", {})

    # Apply table filter CLI overrides
    if args.tables:
        options["include_tables"] = args.tables
    if args.exclude_tables:
        options["exclude_tables"] = args.exclude_tables

    # Apply repoint-to-aas CLI override
    if args.repoint_to_aas:
        source = config.get("source", {})
        options["repoint_to_aas"] = {
            "server": source.get("server", ""),
            "database": source.get("database", ""),
        }

    # Auto-enable repoint_to_aas when deploying to Fabric (not export-only)
    # so that legacy SQL partitions are automatically converted to M
    # expressions that import data from the AAS source.
    if (
        not args.export_only
        and not options.get("repoint_to_aas")
        and not options.get("strip_partitions")
    ):
        source = config.get("source", {})
        if source.get("server") and source.get("database"):
            options["repoint_to_aas"] = {
                "server": source["server"],
                "database": source["database"],
            }
            log.info(
                "Auto-enabling repoint_to_aas for Fabric import mode "
                "(source: %s / %s)",
                source["server"],
                source["database"],
            )

    # ── Get BIM JSON ───────────────────────────────────────────────
    bim_json: Optional[str] = None

    if args.from_bim:
        # Load from existing BIM file
        bim_path = Path(args.from_bim)
        if not bim_path.exists():
            log.error("BIM file not found: %s", bim_path)
            sys.exit(1)
        bim_json = bim_path.read_text(encoding="utf-8")
        log.info("Loaded BIM from %s (%d chars)", bim_path, len(bim_json))

    else:
        # Read from AAS
        source = config.get("source", {})
        server = source.get("server")
        database = source.get("database")
        if not server or not database:
            log.error("source.server and source.database are required in config")
            sys.exit(1)

        token = get_access_token(config, scope=SCOPE_AAS)
        bim_json = read_model_from_aas(server, database, token)

    # ── Transform BIM ──────────────────────────────────────────────
    bim_json = transform_bim(bim_json, options)

    # ── Print summary ──────────────────────────────────────────────
    summary = inspect_bim(bim_json)
    print_model_summary(summary)

    # ── Export BIM to file ─────────────────────────────────────────
    bim_output = args.output or options.get("bim_output_path")
    if options.get("export_bim") or args.export_only:
        if bim_output:
            export_bim(bim_json, bim_output)
        else:
            export_bim(bim_json, "model.bim")

    if args.export_only:
        log.info("Export-only mode — skipping Fabric deployment.")
        return

    # ── Deploy to Fabric ───────────────────────────────────────────
    target = config.get("target", {})
    dataset_name = target.get("dataset_name")
    if not dataset_name:
        log.error("target.dataset_name is required in config")
        sys.exit(1)

    # Acquire a Power BI-scoped token for Fabric
    token = get_access_token(config, scope=SCOPE_POWERBI)

    if args.deploy_method == "xmla":
        xmla_endpoint = target.get("xmla_endpoint")
        if not xmla_endpoint:
            log.error("target.xmla_endpoint is required for XMLA deployment")
            sys.exit(1)
        deploy_to_fabric_xmla(
            xmla_endpoint=xmla_endpoint,
            dataset_name=dataset_name,
            bim_json=bim_json,
            token=token,
            overwrite=args.overwrite,
        )
    else:
        # REST API deployment (default) — extract workspace name from XMLA endpoint
        workspace_name = target.get("workspace_name")
        if not workspace_name:
            # Try to extract from xmla_endpoint:
            # powerbi://api.powerbi.com/v1.0/myorg/WorkspaceName
            xmla_endpoint = target.get("xmla_endpoint", "")
            if "/myorg/" in xmla_endpoint:
                workspace_name = xmla_endpoint.split("/myorg/")[-1].strip("/")
        if not workspace_name:
            log.error(
                "target.workspace_name (or target.xmla_endpoint) is required. "
                "Set workspace_name in config or provide the XMLA endpoint URL."
            )
            sys.exit(1)

        deploy_to_fabric_rest(
            workspace_name=workspace_name,
            dataset_name=dataset_name,
            bim_json=bim_json,
            token=token,
            overwrite=args.overwrite,
        )

    log.info("Migration complete!")


if __name__ == "__main__":
    main()
