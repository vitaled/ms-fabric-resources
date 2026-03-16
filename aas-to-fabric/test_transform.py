"""Quick test to verify transform_bim fixes."""
import json
import sys

sys.path.insert(0, ".")
from aas_to_fabric import transform_bim

bim_json = open("model.bim", "r", encoding="utf-8").read()

# Test with repoint_to_aas enabled
options = {
    "include_tables": ["Customer"],
    "repoint_to_aas": {
        "server": "asazure://westeurope.asazure.windows.net/aastest0042:rw",
        "database": "AdventureWorks",
    },
}

result = transform_bim(bim_json, options)
bim = json.loads(result)

for t in bim["model"]["tables"]:
    name = t["name"]
    print(f"Table: {name}")
    for p in t.get("partitions", []):
        pname = p["name"]
        mode = p.get("mode")
        ptype = p["source"]["type"]
        print(f"  Partition: {pname}  mode={mode}  type={ptype}")
        if ptype == "m":
            print(f"  Expression:\n{p['source']['expression']}")

has_ds = "dataSources" in bim["model"]
print(f"\nHas dataSources: {has_ds}")
print(f"Compat level: {bim.get('compatibilityLevel')}")
print(f"defaultPowerBIDataSourceVersion: {bim['model'].get('defaultPowerBIDataSourceVersion')}")
