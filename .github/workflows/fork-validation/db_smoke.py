# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Run one fixed Spark command on a temporary single-node Databricks cluster."""

import http.client
import json
import os
from pathlib import Path
import re
import signal
import sys
import time
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db_console import TransientRequest, observe, run_build, verify_failure_console


CLUSTERS = "/api/2.1/clusters"
COMMANDS = "/api/1.2"


def workspace_api(host, token):
    origin = re.fullmatch(r"https://([a-z0-9.-]+\.(?:cloud\.databricks\.com|azuredatabricks\.net|gcp\.databricks\.com))/?", host)
    if not origin or not token or not token.isascii() or not token.isprintable():
        raise RuntimeError("Expected one HTTPS Databricks workspace and a token")

    def request(method, path, **fields):
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
                error = TransientRequest if reply.status in (408, 429, 500, 502, 503, 504) else RuntimeError
                raise error(f"{method} {path}: HTTP {reply.status}{reason}")
            response = json.loads(reply.read(1024 * 1024))
            if not isinstance(response, dict):
                raise ValueError
            return response
        except (OSError, http.client.HTTPException) as exc:
            raise TransientRequest(f"{method} {path}: {type(exc).__name__}") from None
        except ValueError as exc:
            raise RuntimeError(f"{method} {path}: {type(exc).__name__}") from None
        finally:
            conn.close()

    def call(method, path, **fields):
        # Never repeat cluster creation or a command submission after an ambiguous response.
        if method == "GET":
            return observe(lambda: request(method, path, **fields))
        return request(method, path, **fields)
    return call


def wait(label, read, key, target, pending, seconds, interval=8):
    deadline = time.monotonic() + seconds
    notice = 0
    while time.monotonic() < deadline:
        result = read()
        state = result.get(key)
        if state == target:
            return result
        if state not in pending:
            observed = state if isinstance(state, str) and re.fullmatch(r"[A-Za-z_]{1,32}", state) else "unrecognized"
            code = (result.get("termination_reason") or {}).get("code")
            reason = f", reason={code}" if isinstance(code, str) and re.fullmatch(r"[A-Z_]{2,80}", code) else ""
            raise RuntimeError(f"{label}: expected {target}, observed {observed}{reason}")
        if time.monotonic() >= notice:
            print(f"{label}: {state}; {int(time.monotonic() - (deadline - seconds))}s elapsed", flush=True)
            notice = time.monotonic() + 60
        time.sleep(min(interval, max(0, deadline - time.monotonic())))
    raise RuntimeError(f"{label}: did not reach {target} within {seconds}s")


def identifier(record, key):
    value = record.get(key)
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", value):
        raise RuntimeError(f"Missing or invalid {key} in API response")
    return value


def report_build(output):
    if not isinstance(output, str) or len(output) > 32000:
        raise RuntimeError("Missing bounded driver build report")
    report = json.loads(output.strip())
    if (not isinstance(report, dict) or type(report.get("ok")) is not bool
            or not isinstance(report.get("lines"), list) or len(report["lines"]) > 120):
        raise RuntimeError("Invalid driver build report")
    print("Databricks driver output (post-run):", flush=True)
    for line in report["lines"]:
        if (not isinstance(line, str) or len(line) > 700 or not line.isascii()
                or not line.isprintable() or re.search(
                    r"(?i)dapi[a-f0-9]{16}|bearer |://|gh[pousr]_|github_pat_|@|"
                    r"\b[a-z0-9-]+\.(?!(?:py|sh|test_simple_range)\b)[a-z]{2,}\b", line)):
            raise RuntimeError("Driver output not suitable for public logs")
        print(f"  driver | {line}", flush=True)
    if os.environ.get("RUNNER_TEMP"):
        evidence = Path(os.environ["RUNNER_TEMP"]) / "db-evidence"
        evidence.mkdir(exist_ok=True)
        (evidence / "driver.log").write_text("\n".join(report["lines"]) + "\n")
        junit = report.get("junit")
        if junit is not None:
            if (not isinstance(junit, dict) or set(junit) != {"failure", "error", "skipped"}
                    or any(type(x) is not int or x not in (0, 1) for x in junit.values())):
                raise RuntimeError("Unexpected driver JUnit summary")
            (evidence / "junit-summary.json").write_text(json.dumps({
                "source": "driver pytest XML", "case": "range_test.py::test_simple_range",
                "tests": 1, **junit}, indent=2) + "\n")
    if not report["ok"]:
        raise RuntimeError("Driver reported an unsuccessful repository build or test; see stage output above")


def smoke(call, name, runtime, node, build=False):
    if not runtime.strip() or not node.strip():
        raise RuntimeError("Runtime and CPU node type must be explicitly configured")
    principal = call("GET", "/api/2.0/preview/scim/v2/Me", attributes="userName").get("userName")
    if not isinstance(principal, str) or not principal.strip():
        raise RuntimeError("Workspace did not identify the smoke principal")
    payload = None
    if build:
        versions = call("GET", CLUSTERS + "/spark-versions")["versions"]
        nodes = call("GET", CLUSTERS + "/list-node-types")["node_types"]
        if (runtime != "17.3.x-gpu-ml-scala2.13" or node not in {"g4dn.4xlarge", "g5.2xlarge"}
                or runtime not in {v["key"] for v in versions}
                or not any(n["node_type_id"] == node and n.get("num_gpus", 0) == 1
                           and not (n.get("node_info") or {}).get("status") for n in nodes)):
            raise RuntimeError("AWS2 does not offer the expected single-GPU node/runtime; no cluster requested")
        helpers = Path(__file__).resolve().parents[1] / "databricks-ci"
        # Only reviewed code and public archive pins cross to the disposable driver.
        tool = (helpers / "driver_archive.py").read_text()
        catalog = json.loads((helpers / "archives.json").read_text())
        payload = (f"ARCHIVE_TOOL = {tool!r}\nARCHIVE_CATALOG = {catalog!r}\n"
                   + Path(__file__).with_name("db_build_driver.py").read_text())
    print(f"Requesting {name} (one node; 10-minute idle termination)", flush=True)
    try:
        created = call(
            "POST", CLUSTERS + "/create", cluster_name=name,
            spark_version=runtime, node_type_id=node, num_workers=0, autotermination_minutes=10,
            aws_attributes={"availability": "ON_DEMAND", "first_on_demand": 1, "zone_id": "auto"},
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
        if build:
            def query(code):
                command = identifier(call("POST", COMMANDS + "/commands/execute", **ids, language="python",
                                          command=code), "id")
                result = wait("Driver console command", lambda: call("GET", COMMANDS + "/commands/status",
                              **ids, commandId=command), "status", "Finished", ("Queued", "Running"), 100, interval=2)
                output = result.get("results") or {}
                if result.get("id") != command or output.get("resultType") != "text":
                    raise RuntimeError("No text response from the Databricks console command")
                return json.loads(output["data"])

            excerpts = {}
            report_build(json.dumps(run_build(query, payload, excerpts=excerpts)))
            if not excerpts.get("stdout") or not excerpts.get("stderr"):
                raise RuntimeError("Product test passed, but fork console validation did not receive both streams")
            print(f"Console validation: {len(excerpts['stdout'])} stdout and {len(excerpts['stderr'])} stderr lines",
                  flush=True)
            verify_failure_console(query)
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
    build = os.environ.get("FORK_DB_BUILD") == "1"
    smoke(api, f"fork-db-{'build' if build else 'smoke'}-{run}-{attempt}", os.environ["DATABRICKS_SPARK_VERSION"],
          os.environ["DATABRICKS_NODE_TYPE_ID"], build=build)
