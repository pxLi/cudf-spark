# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Run one fixed Spark command on a temporary single-node Databricks cluster."""

import http.client
import json
import os
import re
import signal
import sys
import time
from urllib.parse import urlencode


CLUSTERS = "/api/2.1/clusters"
COMMANDS = "/api/1.2"


def workspace_api(host, token):
    origin = re.fullmatch(r"https://([a-z0-9.-]+\.(?:cloud\.databricks\.com|azuredatabricks\.net|gcp\.databricks\.com))/?", host)
    if not origin or not token or not token.isascii() or not token.isprintable():
        raise RuntimeError("Expected one HTTPS Databricks workspace and a token")

    def call(method, path, **fields):
        # Connect directly to the configured origin; never follow a redirect with the token.
        conn = http.client.HTTPSConnection(origin[1], timeout=15)
        try:
            query = "?" + urlencode(fields) if fields and method == "GET" else ""
            body = json.dumps(fields) if method == "POST" else None
            conn.request(method, path + query, body=body, headers={
                "Authorization": "Bearer " + token, "Content-Type": "application/json"})
            reply = conn.getresponse()
            if reply.status != 200:
                diagnostic = reply.read(16384).lower()
                network_denial = any(word in diagnostic for word in (
                    b"ip access list", b"ip_access_list", b"network access", b"source ip",
                    b"ip acl", b"private link", b"privatelink", b"public access is not allowed"))
                reason = " (network policy denied)" if network_denial else ""
                try:
                    code = json.loads(diagnostic).get("error_code", "")
                except (ValueError, AttributeError):
                    code = ""
                if isinstance(code, str) and re.fullmatch(r"[a-z_]{3,64}", code):
                    reason += " " + code.upper()
                raise RuntimeError(f"{method} {path}: HTTP {reply.status}{reason}")
            response = json.loads(reply.read(1024 * 1024))
            if not isinstance(response, dict):
                raise ValueError
            return response
        except (OSError, http.client.HTTPException, ValueError) as exc:
            raise RuntimeError(f"{method} {path}: {type(exc).__name__}") from None
        finally:
            conn.close()

    return call


def wait(label, read, key, target, pending, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = read()
        state = result.get(key)
        if state == target:
            return result
        if state not in pending:
            raise RuntimeError(f"{label}: stopped before {target}")
        time.sleep(min(8, max(0, deadline - time.monotonic())))
    raise RuntimeError(f"{label}: did not reach {target} within {seconds}s")


def identifier(record, key):
    value = record.get(key)
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", value):
        raise RuntimeError(f"Missing or invalid {key} in API response")
    return value


def smoke(call, name, runtime, node):
    if not runtime.strip() or not node.strip():
        raise RuntimeError("Runtime and CPU node type must be explicitly configured")
    principal = call("GET", "/api/2.0/preview/scim/v2/Me", attributes="userName").get("userName")
    if not isinstance(principal, str) or not principal.strip():
        raise RuntimeError("Workspace did not identify the smoke principal")
    print(f"Requesting {name} (one node; 10-minute idle termination)", flush=True)
    try:
        created = call(
            "POST", CLUSTERS + "/create", cluster_name=name,
            spark_version=runtime, node_type_id=node, num_workers=0, autotermination_minutes=10,
            aws_attributes={"availability": "ON_DEMAND", "first_on_demand": 1},
            data_security_mode="SINGLE_USER", single_user_name=principal,
            spark_conf={"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*, 4]"},
            custom_tags={"ResourceClass": "SingleNode", "SmokeRun": name},
        )
        cluster = identifier(created, "cluster_id")
    except RuntimeError as exc:
        raise RuntimeError(f"{exc}; no confirmed cluster ID. Do not retry creation; reconcile {name}.") from None

    def owned():
        record = call("GET", CLUSTERS + "/get", cluster_id=cluster)
        if (record.get("cluster_id") != cluster or record.get("cluster_name") != name
                or (record.get("custom_tags") or {}).get("SmokeRun") != name):
            raise RuntimeError(f"{name}: cluster ownership not confirmed; refusing operation")
        return record

    try:
        ready = wait("Cluster startup", owned, "state", "RUNNING", ("PENDING", "RESTARTING", "RESIZING"), 18 * 60)
        print("Confirmed owned AWS2 cluster RUNNING", flush=True)
        if ready.get("num_workers") != 0 or ready.get("node_type_id") != node:
            raise RuntimeError("Workspace did not provision the requested single-node compute")
        context = identifier(call("POST", COMMANDS + "/contexts/create", clusterId=cluster, language="python"), "id")
        ids = {"clusterId": cluster, "contextId": context}
        ready = wait("Context startup", lambda: call("GET", COMMANDS + "/contexts/status", **ids),
                     "status", "Running", ("Pending",), 180)
        if ready.get("id") != context:
            raise RuntimeError("Unexpected execution context in status response")
        command = identifier(call("POST", COMMANDS + "/commands/execute", **ids, language="python",
                                  command="print(spark.range(1).count())"), "id")
        result = wait("Spark command", lambda: call("GET", COMMANDS + "/commands/status", **ids, commandId=command),
                      "status", "Finished", ("Queued", "Running"), 180)
        output = result.get("results") or {}
        if (result.get("id") != command or output.get("resultType") != "text"
                or not isinstance(output.get("data"), str) or output["data"].strip() != "1"):
            raise RuntimeError("Spark command did not return the expected count")
        print("Verified spark.range(1).count() == 1", flush=True)
    finally:
        state = owned().get("state")
        if state not in ("TERMINATING", "TERMINATED"):
            try:
                call("POST", CLUSTERS + "/delete", cluster_id=cluster)
            except RuntimeError as exc:
                # A lost response is not evidence that termination failed. Observe rather than resubmit.
                print(f"{exc}; checking for termination", file=sys.stderr, flush=True)
        wait("Cluster cleanup", owned, "state", "TERMINATED",
             ("PENDING", "RUNNING", "RESTARTING", "RESIZING", "ERROR", "UNKNOWN", "TERMINATING"), 8 * 60)
        print(f"Confirmed {name} TERMINATED", flush=True)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    run, attempt = os.environ["GITHUB_RUN_ID"], os.environ["GITHUB_RUN_ATTEMPT"]
    if not all(re.fullmatch(r"[1-9][0-9]{0,19}", value) for value in (run, attempt)):
        sys.exit("Invalid workflow run identity")
    api = workspace_api(os.environ["DATABRICKS_HOST"], os.environ["DATABRICKS_TOKEN"])
    smoke(api, f"fork-db-smoke-{run}-{attempt}", os.environ["DATABRICKS_SPARK_VERSION"],
          os.environ["DATABRICKS_NODE_TYPE_ID"])
