import requests
import pandas as pd
import base64
import json
import re
import time
from azure.identity import InteractiveBrowserCredential

BASE_URL = "https://api.fabric.microsoft.com/v1"
RETRY_TIMEOUT = 5  # Set to a number (in seconds) to override the server's Retry-After, e.g. 5

# Opens a browser for login (supports MFA)
print("Authenticating...")
credential = InteractiveBrowserCredential()
token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
access_token = token.token
print("Authentication successful.")

headers = {
    "Authorization": f"Bearer {access_token}"
}

results = []
env_cache = {}  # Cache environment runtime lookups


def get_notebook_definition(ws_id, nb_id, nb_name):
    """Get notebook definition, trying GET first, then POST with polling."""
    # Try simple GET first
    resp = requests.get(
        f"{BASE_URL}/workspaces/{ws_id}/items/{nb_id}/definition",
        headers=headers
    )
    if resp.status_code == 200:
        print(f"    Got definition via GET")
        return resp.json()

    # Fall back to POST + polling
    resp = requests.post(
        f"{BASE_URL}/workspaces/{ws_id}/notebooks/{nb_id}/getDefinition",
        headers=headers,
        json={"format": "fabricItem"}
    )
    if resp.status_code == 200:
        print(f"    Got definition via POST (immediate)")
        return resp.json()
    elif resp.status_code == 202:
        location = resp.headers.get("Location")
        retry_after = RETRY_TIMEOUT if RETRY_TIMEOUT is not None else int(resp.headers.get("Retry-After", 5))
        print(f"    Polling for definition (retry every {retry_after}s)...")
        while location:
            time.sleep(retry_after)
            poll = requests.get(location, headers=headers)
            if poll.status_code == 200:
                poll_body = poll.json()
                status = poll_body.get("status")
                print(f"    Poll status: {status}")
                if status == "Succeeded":
                    definition_url = poll_body.get("resourceLocation")
                    if not definition_url:
                        definition_url = location.rstrip("/") + "/result"
                    def_resp = requests.get(definition_url, headers=headers)
                    if def_resp.status_code == 200:
                        return def_resp.json()
                    else:
                        print(f"    Failed to fetch result: {def_resp.status_code}")
                    break
                elif status in ("Failed", "Cancelled"):
                    print(f"    Operation {status}")
                    break
            elif poll.status_code == 202:
                retry_after = RETRY_TIMEOUT if RETRY_TIMEOUT is not None else int(poll.headers.get("Retry-After", 5))
            else:
                print(f"    Unexpected poll status: {poll.status_code}")
                break
    else:
        print(f"    Failed to get definition: {resp.status_code} {resp.text[:200]}")
    return None

#List Workspaces
print("\nFetching workspaces...")
ws_resp = requests.get(f"{BASE_URL}/workspaces", headers=headers)
if ws_resp.status_code != 200:
    raise SystemExit(f"Failed to list workspaces: {ws_resp.status_code} {ws_resp.text}")
workspaces = ws_resp.json().get("value", [])
print(f"Found {len(workspaces)} workspaces.")

for ws in workspaces:

    ws_id = ws["id"]
    ws_name = ws["displayName"]
    print(f"\n--- Workspace: {ws_name} ---")

    # Get workspace default Spark runtime
    ws_default_runtime = None
    spark_settings = requests.get(
        f"{BASE_URL}/workspaces/{ws_id}/spark/settings",
        headers=headers
    )
    if spark_settings.status_code == 200:
        spark_body = spark_settings.json()
        ws_default_runtime = spark_body.get("environment", {}).get("runtimeVersion")
        print(f"  Default runtime: {ws_default_runtime}")
    else:
        print(f"  Could not get Spark settings: {spark_settings.status_code}")

    #List Items
    items_resp = requests.get(
        f"{BASE_URL}/workspaces/{ws_id}/items",
        headers=headers
    )
    if items_resp.status_code != 200:
        print(f"  Skipping: cannot list items ({items_resp.status_code})")
        continue
    items = items_resp.json().get("value", [])
    notebooks = [i for i in items if i["type"] == "Notebook"]
    print(f"  Found {len(notebooks)} notebooks.")

    for item in notebooks:

        nb_id = item["id"]
        nb_name = item["displayName"]
        print(f"  Processing: {nb_name}")

        definition = get_notebook_definition(ws_id, nb_id, nb_name)

        runtime_version = None
        env_name = None

        if definition:
            try:
                parts = definition.get("definition", {}).get("parts", [])
                print(f"    Parts: {[p.get('path') for p in parts]}")
                for part in parts:
                    decoded = base64.b64decode(part["payload"]).decode("utf-8")
                    if part.get("path", "").endswith(".platform"):
                        platform = json.loads(decoded)
                        platform_runtime = platform.get("config", {}).get("runtime", {}).get("version")
                        if platform_runtime:
                            runtime_version = platform_runtime
                    elif "notebook-content" in part.get("path", ""):
                        # Extract environment dependency from notebook metadata
                        meta_match = re.search(r'# META \{.*?# META \}', decoded, re.DOTALL)
                        if meta_match:
                            meta_lines = meta_match.group()
                            clean = re.sub(r'^# META\s?', '', meta_lines, flags=re.MULTILINE)
                            try:
                                meta = json.loads(clean)
                                env_dep = meta.get("dependencies", {}).get("environment", {})
                                if env_dep:
                                    env_id = env_dep.get("environmentId")
                                    env_ws_id = env_dep.get("workspaceId", ws_id)
                                    cache_key = f"{env_ws_id}/{env_id}"
                                    if cache_key not in env_cache:
                                        env_resp = requests.get(
                                            f"{BASE_URL}/workspaces/{env_ws_id}/environments/{env_id}",
                                            headers=headers
                                        )
                                        if env_resp.status_code == 200:
                                            env_data = env_resp.json()
                                            env_display = env_data.get("displayName", env_id)
                                            # Get environment Spark compute settings for runtime
                                            env_runtime = None
                                            spark_resp = requests.get(
                                                f"{BASE_URL}/workspaces/{env_ws_id}/environments/{env_id}/sparkcompute",
                                                headers=headers
                                            )
                                            if spark_resp.status_code == 200:
                                                spark_data = spark_resp.json()
                                                env_runtime = spark_data.get("runtimeVersion")
                                            env_cache[cache_key] = {
                                                "name": env_display,
                                                "runtime": env_runtime
                                            }
                                        else:
                                            env_cache[cache_key] = {"name": env_id, "runtime": None}
                                    cached = env_cache[cache_key]
                                    env_name = cached["name"]
                                    if cached["runtime"]:
                                        runtime_version = cached["runtime"]
                                    print(f"    Environment: {env_name}, runtime: {cached['runtime']}")
                            except json.JSONDecodeError:
                                pass
            except Exception as e:
                print(f"Error parsing definition for {nb_name}: {e}")

        if runtime_version is None:
            runtime_version = f"workspace default ({ws_default_runtime})" if ws_default_runtime else "workspace default"
        elif env_name:
            runtime_version = f"{runtime_version} (env: {env_name})"

        print(f"    => Runtime: {runtime_version}")

        results.append({
            "workspace": ws_name,
            "notebook": nb_name,
            "runtime": runtime_version,
            "environment": env_name or ""
        })

df = pd.DataFrame(results)

print(df)

df.to_csv("fabric_notebook_runtimes.csv", index=False)