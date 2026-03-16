"""Fetch and save the definition of the deployed Dataflow Gen 2."""
import requests, json, base64, time, logging
logging.disable(logging.CRITICAL)

from aas_to_fabric import load_config, get_access_token, SCOPE_POWERBI, _resolve_workspace_id

cfg = load_config("config.json")
token = get_access_token(cfg, SCOPE_POWERBI)
ws_id = _resolve_workspace_id("ferragamo-demo", token)
headers = {"Authorization": f"Bearer {token}"}

resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items?type=Dataflow",
    headers=headers,
)
dfs = [i for i in resp.json().get("value", []) if i["displayName"] == "AAS_Import_AdventureWorks"]
if not dfs:
    print("Dataflow not found")
    exit(1)
df_id = dfs[0]["id"]
print(f"Dataflow ID: {df_id}")

resp = requests.post(
    f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/dataflows/{df_id}/getDefinition",
    headers=headers,
)
print(f"getDefinition status: {resp.status_code}")

parts = []
if resp.status_code == 200:
    parts = resp.json().get("definition", {}).get("parts", [])
elif resp.status_code == 202:
    loc = resp.headers.get("Location", "")
    ra = resp.headers.get("Retry-After", "5")
    print(f"Polling async operation (retry-after {ra}s)...")
    for i in range(20):
        time.sleep(int(ra) if ra.isdigit() else 5)
        p = requests.get(loc, headers=headers)
        b = p.json()
        s = b.get("status", "")
        print(f"  Poll {i+1}: {s}")
        if s == "Succeeded":
            ru = b.get("resultUrl", "") or b.get("resourceLocation", "")
            r2 = requests.get(ru, headers=headers)
            parts = r2.json().get("definition", {}).get("parts", [])
            break
        elif s == "Failed":
            print(json.dumps(b, indent=2))
            break
else:
    print(resp.text[:2000])

for part in parts:
    path = part["path"]
    data = base64.b64decode(part["payload"]).decode("utf-8")
    fname = path.replace("/", "_")
    with open(f"df_def_{fname}", "w", encoding="utf-8") as f:
        f.write(data)
    print(f"\nSaved: df_def_{fname} ({len(data)} chars)")
    print(data[:5000])

print("\nDone.")
