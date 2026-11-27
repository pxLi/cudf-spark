# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Fork-only pilot of the real v2 cell. No cloud operation in offline or preflight."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / ".github/workflows/databricks-ci"))
import attempts
import workload

REPO = "pxLi/cudf-spark"
REF = "refs/heads/validation/gpu-ci-17162-20261127"
WORKFLOW = ".github/workflows/fork-v2-validation.yml"
SOURCE = {
    "revision": "ce575b3891f0233983f3a924ca73f83cceb5c1cb",
    "submodules": {
        "thirdparty/parquet-testing": "d79a0101d90dfa3bbb10337626f57a3e8c4b5363",
        "thirdparty/spark-rapids-common": "5b570402a63ce8feeb97494c9998076a1908c0ae",
    },
}

TEST = """set -eu
export TESTS=range_test.py::test_simple_range TEST_PARALLEL=0
bash "$(dirname "$0")/product-test.sh" "$@"
/usr/bin/python3 -I "$(dirname "$0")/check-case.py" /home/ubuntu/spark-rapids/scala2.13/integration_tests/target
"""
CHECK_CASE = """import sys
from pathlib import Path
from xml.etree.ElementTree import parse
cases = []
for report in Path(sys.argv[1]).glob("run_dir*/TEST-pytest-*.xml"):
    if report.is_symlink() or not report.is_file() or report.stat().st_size > 64 * 1024 * 1024:
        sys.exit("Invalid fork pytest report")
    for case in parse(report).getroot().iter("testcase"):
        cases.append((case.get("classname", "").split(".")[-1], case.get("name"),
                      any(case.find(k) is not None for k in ("skipped", "error", "failure"))))
if cases != [("range_test", "test_simple_range", False)]:
    sys.exit("Expected exactly one passing range_test::test_simple_range")
print("Confirmed one passing range_test.py::test_simple_range")
"""

BUILD = """set -eu
/usr/bin/python3 -I - <<'PY'
import hashlib, os, pwd
from pathlib import Path
jar = Path(pwd.getpwuid(os.geteuid()).pw_dir) / ".m2/repository" / %r
if not jar.is_file() or jar.is_symlink() or hashlib.sha256(jar.read_bytes()).hexdigest() != %r:
    raise SystemExit("Product Maven repository did not receive the verified fork plugin")
PY
exec bash "$(dirname "$0")/product-build.sh" "$@"
"""


def gate():
    sha = os.environ.get("FORK_DB_V2_SHA", "")
    if (not re.fullmatch(r"[a-f0-9]{40}", sha) or os.environ.get("GITHUB_SHA") != sha
            or os.environ.get("GITHUB_WORKFLOW_SHA") != sha or os.environ.get("GITHUB_REPOSITORY") != REPO
            or os.environ.get("GITHUB_REF") != REF or os.environ.get("GITHUB_EVENT_NAME") != "push"
            or os.environ.get("GITHUB_WORKFLOW_REF") != f"{REPO}/{WORKFLOW}@{REF}"
            or os.environ.get("GITHUB_RUN_ATTEMPT") != "1"
            or subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip() != sha):
        raise ValueError("Expected the reviewed controller SHA on the first attempt of the fork validation push")


class Cell:
    attempt_env = staticmethod(workload.attempt_env)
    creator_name = staticmethod(workload.creator_name)
    cleanup_env = staticmethod(workload.cleanup_env)

    @staticmethod
    def source_manifest(candidate, sha):
        result = workload.source_manifest(candidate, sha)
        if result != SOURCE:
            raise ValueError("Fixed project revision or pinned submodule mismatch")
        return result

    @staticmethod
    def run_workload(phase, env, cluster_id, directory, source):
        if source != SOURCE:
            raise ValueError("Workload did not receive the reviewed source manifest")
        if phase == "test":
            original = workload.driver_payload

            def bounded_test(plan, files):
                if plan["phase"] != "test" or plan["mode"] != "CI_PART1":
                    raise ValueError("Fork case only covers CI_PART1")
                return original(plan, {**files, "product-test.sh": files["test.sh"], "test.sh": TEST,
                                       "check-case.py": CHECK_CASE})

            with patch.object(workload, "driver_payload", bounded_test):
                return workload.run_workload(phase, env, cluster_id, directory, source)
        if phase != "build":
            raise ValueError("Unexpected workload phase")
        digest = env.get("DB_MAVEN_SEED_SHA256", "")
        if not re.fullmatch("[0-9a-f]{64}", digest) or env["DATABRICKS_CLUSTER_ID"] != cluster_id:
            raise ValueError("Missing fork seed or cluster identity")
        helper = (workload.DB / "maven_seed_v2.py").read_text()
        settings = (workload.DB / "maven-settings.xml").read_text()
        payload = (Path(__file__).with_name("v2_seed_probe.py").read_text() + "\nimport json\nseed = {'__name__':'seed'}\n"
                   + f"exec({helper!r}, seed)\nprint(json.dumps(prove(seed['restore'], {digest!r}, {settings!r})))\n")
        call = workload.workspace_api(env["DATABRICKS_HOST"], env["DATABRICKS_TOKEN"])
        with workload.command_context(call, cluster_id) as query:
            report, _ = workload.run_driver(query, payload, hashlib.sha256(payload.encode()).hexdigest(),
                                            (workload.HERE / "console_filter.py").read_text(),
                                            deadline_seconds=420, label="fork Maven release seed")
        artifact = report.pop("artifact", "")
        if not isinstance(artifact, str) or not re.fullmatch("[0-9a-f]{64}", artifact) or report != {
                      "ok": True, "plugin": "org.apache.maven.plugins:maven-clean-plugin:3.2.0:clean",
                      "cold": "miss", "seeded": "offline-clean", "private": "absent"}:
            raise ValueError("Fresh-driver offline Maven seed proof missing")
        print("Maven clean 3.2.0: empty repository missed; fork seed executed offline, no JNI/private/SNAPSHOT.", flush=True)
        original = workload.driver_payload

        def bounded_build(plan, files):
            if plan.get("seed") != digest or files.get("maven_seed_v2.py") != helper:
                raise ValueError("Expected the normal product Maven seed restore")
            isolated = "/dbfs/cached_jars/cudf-spark-ci/fork-validation/maven-releases-v1.tar"
            jar = "org/apache/maven/plugins/maven-clean-plugin/3.2.0/maven-clean-plugin-3.2.0.jar"
            return original(plan, {**files, "product-build.sh": files["build.sh"], "build.sh": BUILD % (jar, artifact),
                                   "maven_seed_v2.py": f"import sys\nsys.argv.extend(['--archive', {isolated!r}])\n" + helper})

        with patch.object(workload, "driver_payload", bounded_build):
            return workload.run_workload(phase, env, cluster_id, directory, source)


def run(operation, candidate):
    node = "g4dn.4xlarge"
    # The fork allows one sandbox identity. Resolve it from the same authenticated workspace, never PR input.
    host = os.environ["DATABRICKS_HOST"]
    call = workload.workspace_api(host, os.environ["DATABRICKS_AWS2_TOKEN"])
    me = call("GET", "/api/2.0/preview/scim/v2/Me", attributes="userName")
    if not isinstance(me.get("userName"), str) or not me["userName"].strip():
        raise ValueError("Sandbox workspace did not confirm the current identity")
    profile = dict(host=host, creator_user_name=me["userName"], driver=node, worker=node)
    os.environ["DATABRICKS_PROFILES_JSON"] = json.dumps({"aws2": profile})
    args = argparse.Namespace(operation=operation, runtime="17.3", mode="CI_PART1", reduced=False,
                              candidate=candidate, merge_sha=SOURCE["revision"],
                              state_dir=Path(os.environ["RUNNER_TEMP"]) / "fork-v2-cell")
    attempts.run_row(args, Cell, repository=REPO, workflow=WORKFLOW, profiles=("aws2",), attempts_per_profile=1)
    if operation == "cleanup" and not (args.state_dir / "cleanup-complete").is_file():
        raise ValueError("Missing cleanup confirmation for the fork cell")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("offline", "preflight", "run", "cleanup"))
    op = parser.parse_args().operation
    candidate = Path(os.environ["GITHUB_WORKSPACE"]).resolve() / "candidate"
    if op == "offline":
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from v2_fixture import exercise
        exercise(candidate)
        return
    gate()
    if op == "preflight":
        Cell.source_manifest(candidate, SOURCE["revision"])
        if attempts.discover(candidate, SOURCE["revision"]).get("17.3") != "4.0.0":
            raise ValueError("Pilot requires the pinned DBR 17.3 / Spark 4.0.0 profile")
        return
    if op == "run" and not re.fullmatch("[0-9a-f]{64}", os.environ.get("DB_MAVEN_SEED_SHA256", "")):
        raise ValueError("No digest from the same-job fork seed publisher; no allocation")

    def interrupted(sig, _frame):
        raise attempts.Failure(f"Interrupted by signal {sig}; no replacement")

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, interrupted)
    run(op, candidate)
    print("Fork pilot: " + ("exact-owned termination and cleanup confirmed" if op == "cleanup"
                            else "fixed-source build and targeted test passed"))


if __name__ == "__main__":
    main()
