# AAS to Fabric — Semantic Model Migration

Migrates an Azure Analysis Services (AAS) tabular model to a **Microsoft Fabric / Power BI** semantic model.

## How It Works

```
┌──────────────────┐      BIM JSON       ┌──────────────────────┐
│  Azure Analysis  │ ──────────────────►  │  Microsoft Fabric    │
│  Services (AAS)  │   (full model def)   │  Semantic Model      │
└──────────────────┘                      └──────────────────────┘
        │                                          ▲
        │  TOM / pythonnet                         │  XMLA endpoint
        ▼                                          │  (TOM deploy)
   ┌──────────┐         optionally
   │ model.bim│ ◄──── export to file
   └──────────┘
```

1. **Read** — Connects to AAS via the Tabular Object Model (TOM) using `pythonnet` and serialises the full database definition to **BIM JSON** (the standard Tabular Model format).
2. **Transform** *(optional)* — Strips partitions, data sources, or adjusts the compatibility level to match Fabric requirements.
3. **Deploy** — Connects to the Fabric workspace XMLA read/write endpoint and creates the semantic model.

## Prerequisites

| Requirement | Notes |
|---|---|
| **Python 3.10+** | 3.11 or 3.12 recommended |
| **pip packages** | `pip install -r requirements.txt` |
| **.NET 6+ runtime** | Required by `pythonnet`. Usually pre-installed on Windows. |
| **AMO/TOM DLLs** | Run `python setup_dotnet.py` to download automatically. |
| **AAS access** | The authenticated user/SP needs at least *Read* permission on the AAS model. |
| **Fabric XMLA** | XMLA read/write must be enabled on the target Fabric/Premium workspace. |

## Quick Start

```powershell
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Download the AMO/TOM .NET libraries
python setup_dotnet.py

# 3. Copy the sample config and fill in your details
copy config.json.sample config.json
notepad config.json

# 4. Run the migration (interactive browser login)
python aas_to_fabric.py --config config.json
```

## Usage Examples

### Full migration (AAS → Fabric)

```powershell
python aas_to_fabric.py -c config.json
```

### Export model to .bim file only

```powershell
python aas_to_fabric.py -c config.json --export-only -o mymodel.bim
```

### Deploy an existing .bim file to Fabric

```powershell
python aas_to_fabric.py -c config.json --from-bim mymodel.bim
```

### Inspect a .bim file (no connections)

```powershell
python aas_to_fabric.py --inspect mymodel.bim
```

### Overwrite an existing semantic model

```powershell
python aas_to_fabric.py -c config.json --overwrite
```

### Service principal authentication

```powershell
python aas_to_fabric.py -c config.json --auth service_principal
```

### Lakehouse migration (AAS → Lakehouse → Semantic Model)

A full pipeline that creates a Lakehouse, a Dataflow Gen 2 (to import data from AAS), and a semantic model on top of the Lakehouse SQL endpoint:

```powershell
# Full pipeline
python aas_to_lakehouse.py -c config.json

# Run a single step
python aas_to_lakehouse.py -c config.json --step lakehouse
python aas_to_lakehouse.py -c config.json --step dataflow
python aas_to_lakehouse.py -c config.json --step semantic-model

# Use an existing BIM file
python aas_to_lakehouse.py -c config.json --from-bim model.bim

# Overwrite existing artefacts
python aas_to_lakehouse.py -c config.json --overwrite
```

## Configuration Reference

Copy `config.json.sample` to `config.json` and edit:

```jsonc
{
    "source": {
        "server": "asazure://westeurope.asazure.windows.net/yourserver",
        "database": "YourModelName"
    },
    "target": {
        // Fabric XMLA endpoint — find in Workspace Settings → Premium
        "xmla_endpoint": "powerbi://api.powerbi.com/v1.0/myorg/YourWorkspaceName",
        "dataset_name": "YourNewSemanticModelName"
    },
    "auth": {
        "method": "interactive",       // interactive | service_principal | default
        "tenant_id": "",               // required for service_principal
        "client_id": "",               // required for service_principal
        "client_secret": ""            // required for service_principal
    },
    "options": {
        "export_bim": true,            // also save a local .bim copy
        "bim_output_path": "model.bim",
        "strip_partitions": false,     // remove partition defs (Fabric uses its own)
        "strip_data_sources": false,   // remove legacy data source objects
        "update_compatibility_level": null  // e.g. 1604 for Fabric
    },
    "lakehouse_migration": {
        "workspace_name": "YourFabricWorkspace",
        "lakehouse_name": "YourLakehouseName",
        "dataflow_name": "AAS_Import_YourModel",
        "semantic_model_name": "YourModel_Lakehouse",
        "aas_server": null,              // defaults to source.server
        "aas_database": null,            // defaults to source.database
        "lakehouse_id": null,            // auto-resolved if omitted
        "sql_endpoint": null             // auto-resolved after Lakehouse creation
    }
}
```

The `lakehouse_migration` section is only required when using `aas_to_lakehouse.py`.

## Deployment Methods

### XMLA Endpoint (recommended)

Uses TOM via `pythonnet` to deploy directly to the Fabric XMLA read/write endpoint. Preserves:

- Tables, columns, measures, calculated columns/tables
- Relationships, hierarchies, display folders
- Perspectives, roles (RLS/OLS)
- KPIs, annotations, translations
- M expressions / partitions

### REST API (alternative)

Uses the Power BI Import REST API. Simpler (no .NET needed for deploy) but has more limitations. Add `--deploy-method rest` and set `target.workspace_id` in config.

## Architecture & Design Choices

### Why TOM/pythonnet instead of `semantic-link`?

| | TOM (pythonnet) | semantic-link (sempy) |
|---|---|---|
| **Model read from AAS** | Full fidelity via `JsonSerializer.SerializeDatabase()` | Not supported (sempy targets Fabric only) |
| **Model deploy** | Full fidelity via XMLA | Limited (no full BIM deploy from outside Fabric) |
| **Runs locally** | Yes | Primarily designed for Fabric notebooks |
| **Dependencies** | .NET runtime + AMO DLLs | `pip install semantic-link` |

`semantic-link` is excellent for _working with_ Fabric models from inside Fabric notebooks (reading data, managing refreshes, using `sempy.tom`). However, for **extracting a full model from AAS** and **deploying it externally**, TOM via pythonnet is the industry-standard approach.

### Using this with semantic-link inside Fabric

If you prefer to run the deployment step inside a Fabric notebook using `sempy`:

1. Run `aas_to_fabric.py --export-only` locally to produce `model.bim`
2. Upload `model.bim` to a Fabric Lakehouse
3. In a Fabric notebook:

```python
import sempy.fabric as fabric
from sempy import tom

# Read the BIM
with open("/lakehouse/default/Files/model.bim") as f:
    bim = f.read()

# Deploy using TOM (sempy wraps it inside Fabric)
# ... or use fabric.create_semantic_model() for simpler models
```

## Troubleshooting

| Issue | Solution |
|---|---|
| `pythonnet` import error | Make sure .NET 6+ runtime is installed. On Windows, it's usually pre-installed. |
| AMO/TOM DLLs not found | Run `python setup_dotnet.py` to download them. |
| AAS connection refused | Verify AAS server name, firewall rules, and user permissions. |
| XMLA endpoint error | Ensure XMLA read/write is enabled in Fabric workspace settings. |
| Auth token error | Try `--auth interactive` to rule out credential issues. |

## File Structure

```
aas-to-fabric/
├── aas_to_fabric.py    # Main migration script (AAS → Fabric semantic model)
├── aas_to_lakehouse.py # Full pipeline: AAS → Lakehouse → Dataflow → Semantic Model
├── fetch_df_def.py     # Utility: download a deployed Dataflow Gen 2 definition
├── test_transform.py   # Quick test for BIM transform logic
├── setup_dotnet.py     # Downloads AMO/TOM .NET libraries
├── config.json.sample  # Template config (copy to config.json)
├── config.json         # Your local config (gitignored)
├── requirements.txt    # Python dependencies
├── model.bim           # Exported model definition (gitignored)
├── README.md           # This file
└── dotnet_libs/        # Created by setup_dotnet.py (gitignored)
    ├── Microsoft.AnalysisServices.dll
    ├── Microsoft.AnalysisServices.Core.dll
    ├── Microsoft.AnalysisServices.Tabular.dll
    └── Microsoft.AnalysisServices.Tabular.Json.dll
```
