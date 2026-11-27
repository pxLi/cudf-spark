# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Offline fork acceptance: only the workspace API and execution of the remote payload are fake."""

import ast
from contextlib import ExitStack
import hashlib
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import v2_cell as cell
sys.path.insert(0, str(cell.ROOT / "jenkins/databricks/tests"))
from test_gha_attempts import Workspace


def cycle(candidate, outcome):
    ws = Workspace()
    contexts = []
    phases = []
    token = "synthetic-not-a-provider-credential"
    host = "https://example.cloud.databricks.com"

    def commands(method, path, **args):
        if (method, path) == ("GET", "/api/2.0/preview/scim/v2/Me"):
            assert args == {"attributes": "userName"}
            return {"userName": ws.creator}
        assert path.startswith("/api/1.2/contexts/") and args["clusterId"] in ws.records
        action = (method, path.rsplit("/", 1)[1])
        contexts.append(action)
        if action == ("POST", "create"):
            return {"id": "command-context"}
        assert args["contextId"] == "command-context"
        if action == ("GET", "status"):
            return {"id": "command-context", "status": "Running"}
        assert action == ("POST", "destroy")
        return {}

    def execute(_query, payload, digest, formatter, *, deadline_seconds, label):
        assert hashlib.sha256(payload.encode()).hexdigest() == digest
        assert all(s not in payload for s in (token, host, ws.creator)) and formatter and deadline_seconds > 0
        phases.append(label)
        if label == "fork Maven release seed":
            assert "/fork-validation/maven-releases-v1.tar" in payload and "f" * 64 in payload
            if outcome == "seed failure":
                return {"ok": False, "exit": 1}, {}
            return ({"ok": True, "plugin": "org.apache.maven.plugins:maven-clean-plugin:3.2.0:clean",
                     "cold": "miss", "seeded": "offline-clean", "private": "absent", "artifact": "d" * 64}, {})
        call, = (x for x in ast.walk(ast.parse(payload).body[-1]) if isinstance(x, ast.Call)
                 and isinstance(x.func, ast.Name) and x.func.id == "run_phase")
        plan, launchers = map(ast.literal_eval, call.args[:2])
        assert plan["source"] == cell.SOURCE and plan["runtime"] == "17.3" and plan["spark"] == "4.0.0"
        assert plan["phase"] == label and plan["mode"] == "CI_PART1"
        launcher = label + ".sh"
        wrapper = "product-" + launcher
        assert launchers[wrapper] == (cell.ROOT / "jenkins/databricks" / launcher).read_text()
        if label == "build":
            assert plan["seed"] == "f" * 64 and "d" * 64 in launchers["build.sh"]
            assert "/fork-validation/maven-releases-v1.tar" in launchers["maven_seed_v2.py"].splitlines()[1]
        rc, counts = 0, [1, 0, 0]
        if label == "test":
            assert launchers["test.sh"] == cell.TEST
            name = "test_other" if outcome == "wrong case" else "test_simple_range"
            child = "<skipped/>" if outcome == "skipped" else ""
            root = Path(tmp) / "run_dir"
            root.mkdir()
            (root / "TEST-pytest-ci.xml").write_text(f'<testsuite><testcase classname="range_test" name="{name}">'
                                                    f'{child}</testcase></testsuite>')
            check = subprocess.run([sys.executable, "-IB", "-c", launchers["check-case.py"], tmp],
                                   capture_output=True, timeout=10)
            rc = check.returncode
            assert (rc == 0) == (outcome == "pass")
            counts = [1, int(outcome == "skipped"), 0]
        return ({"ok": rc == 0, "exit": rc, "stage": "test" if rc else "complete", "junit": counts,
                 "ready": ["databricks", "upstream"], "seed": 1 if label == "build" else None},
                {"stdout": [], "stderr": [], "diagnostics": []})

    with TemporaryDirectory() as tmp, ExitStack() as stack:
        env = dict(PATH=os.defpath, RUNNER_TEMP=tmp, GITHUB_RUN_ID="7", GITHUB_RUN_ATTEMPT="1",
                   GITHUB_REPOSITORY=cell.REPO, GITHUB_WORKFLOW_REF=f"{cell.REPO}/{cell.WORKFLOW}@{cell.REF}",
                   DATABRICKS_HOST=host, DATABRICKS_AWS2_TOKEN=token, FORK_DB_CREATOR_USER_NAME=ws.creator,
                   DB_MAVEN_SEED_SHA256="f" * 64)
        stack.enter_context(patch.dict(os.environ, env, clear=True))
        stack.enter_context(patch("socket.socket.connect", side_effect=AssertionError("Offline network access")))
        stack.enter_context(patch.dict(sys.modules, {"requests": Mock()}))  # No legacy cluster HTTP calls.
        stack.enter_context(patch.object(cell.attempts.https, "workspace_api", return_value=ws.api))
        stack.enter_context(patch.object(cell.workload, "workspace_api", return_value=commands))
        stack.enter_context(patch.object(cell.workload, "run_driver", side_effect=execute))
        stack.enter_context(patch("time.sleep"))
        try:
            print(f"Mock workspace/driver fixture: {outcome}", flush=True)
            cell.run("run", candidate)
        except (cell.attempts.Failure, ValueError) as exc:
            expected = "seed proof" if outcome == "seed failure" else "Test failed"
            if outcome == "pass" or expected.casefold() not in str(exc).casefold():
                raise
        else:
            assert outcome == "pass", "A skipped GPU case passed"
        assert not ws.records and ws.writes() == ["create", "delete", "permanent-delete"]
        cfg, = ws.specs
        assert cfg["num_workers"] == 0 and cfg["driver_node_type_id"] == "g4dn.4xlarge"
        assert 0 < int(cfg["autotermination_minutes"]) <= 30 and cfg["single_user_name"] == ws.creator
        tags = cfg["custom_tags"]
        assert tags["CI_REPOSITORY"] == cell.REPO and tags["CI_WORKFLOW"] == cell.WORKFLOW
        assert tags["CI_MANAGED_BY"].endswith("-fork") and tags["CI_PROFILE"] == "aws2"
        assert phases == ["fork Maven release seed"] + ([] if outcome == "seed failure" else ["build", "test"])
        assert contexts == [("POST", "create"), ("GET", "status"), ("POST", "destroy")] * len(phases)
        before = ws.calls.copy()
        cell.run("cleanup", candidate)
        cell.run("cleanup", candidate)
        assert before == ws.calls, "Confirmed post-cleanup repeated a provider operation"
        print(f"Offline {outcome}: {len(phases)} expected driver phases; exact-owned cleanup verified.")


def exercise(candidate):
    for outcome in ("pass", "skipped", "wrong case", "seed failure"):
        cycle(candidate, outcome)
