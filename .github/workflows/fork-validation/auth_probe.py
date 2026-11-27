"""Authenticate to the selected workspace only, with no allocation or changes."""
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db_smoke import CLUSTERS, workspace_api


def check():
    call = workspace_api(os.environ["DATABRICKS_HOST"], os.environ["DATABRICKS_TOKEN"])
    me = call("GET", "/api/2.0/preview/scim/v2/Me", attributes="userName")
    if not me.get("userName"):
        raise RuntimeError("No authenticated identity returned")
    print("AWS2: authenticated identity confirmed; no identity data logged.")
    versions = {item["key"] for item in call("GET", CLUSTERS + "/spark-versions")["versions"]}
    nodes = {item["node_type_id"] for item in call("GET", CLUSTERS + "/list-node-types")["node_types"]
             if item.get("num_cores", 0) in (2, 4) and 0 < item.get("memory_mb", 0) <= 32768}
    runtime = "17.3.x-scala2.13"
    node = next((n for n in ("m5d.large", "m5d.xlarge", "i3.xlarge") if n in nodes), None)
    print(f"AWS2: runtime and node type API readable. Compatible small CPU smoke: {runtime in versions and bool(node)}")
    if runtime in versions and node:
        print(f"Available smoke configuration: {runtime}, {node}; no cluster created.")


try:
    check()
except RuntimeError as exc:
    print(f"AWS2 authorization probe: {exc}; no allocation.")
