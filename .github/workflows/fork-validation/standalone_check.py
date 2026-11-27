# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise the checkout's standalone shard and JUnit contracts without a GPU."""

import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from xml.etree import ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parent))
from plan import CASE, I, ROOT, SCALA, U, WORKFLOWS

GPU = WORKFLOWS / "gpu-ci"


def run(script, *args, cwd=ROOT, env=None, rc=0, message=""):
    p = subprocess.run([sys.executable, "-IB", GPU / script, *map(str, args)], cwd=cwd,
                       env={"PATH": os.defpath, **(env or {})}, capture_output=True, text=True, timeout=30)
    if p.returncode != rc or message not in p.stdout + p.stderr:
        raise AssertionError(f"{script}: expected exit {rc} and {message!r}; got {p.returncode}: {p.stdout}{p.stderr}")
    return p.stdout


def junit(path, classname, case, outcome):
    path.parent.mkdir(parents=True, exist_ok=True)
    suite = ET.Element("testsuite", tests="1", errors="0", failures="0")
    test = ET.SubElement(suite, "testcase", classname=classname, name=case)
    if outcome:
        ET.SubElement(test, outcome)
        if outcome == "failure":
            suite.set("failures", "1")
    ET.ElementTree(suite).write(path, encoding="utf-8")


def check(tmp):
    rows = json.loads(run("groups.py", '["unit","integration"]'))["include"]
    filename = Path(I).name
    node = f"{filename}::{CASE}"
    tracked = subprocess.check_output(["git", "ls-files", "--", "integration_tests/src/main/python"],
                                      cwd=ROOT, text=True).splitlines()
    expected = {str(Path(p).relative_to("integration_tests/src/main/python")) for p in tracked
                if Path(p).match("test_*.py") or Path(p).match("*_test.py")}
    shards = {i: set(run("it.py", "files", i).split()) for i in (1, 2)}
    assert shards[1].isdisjoint(shards[2]) and shards[1] | shards[2] == expected
    shard, = [i for i, files in shards.items() if filename in files]
    assert (ROOT / U).is_file() and f"def {CASE}(" in (ROOT / I).read_text()
    env = {"IT_SHARD": str(shard), "TARGETS_JSON": json.dumps([node])}
    assert run("it.py", "files", shard, env=env).strip() == node
    run("it.py", "files", 3 - shard, env=env, rc=1, message="wrong-shard target")

    # Use the worker's per-Scala report paths and both fallback exit codes. A
    # passing stale pytest report must not rescue the current wrong-case report.
    for scala in ("2.12", "2.13"):
        module = tmp if scala == "2.12" else tmp / "scala2.13"
        unit = next(r for r in rows if r["kind"] == "unit" and r["scala"] == scala)["shims"][0]
        it, = [r for r in rows if (r["kind"], r["scala"], r.get("shard")) == ("integration", scala, shard)]
        assert it["arch"] == "amd64"
        ut_xml = module / "tests/target" / ("spark" + unit["shim"]) / "surefire-reports/TEST-suite.xml"
        target = module / "integration_tests/target"
        old = target / "run_dir-old/TEST-pytest-old.xml"
        junit(old, Path(I).stem, CASE, "")
        before = module / "before"
        before.write_text(str(old) + "\n")
        out = module / "reports"
        out.mkdir()
        for outcome, urc, irc in (("", 0, 0), ("skipped", 3, 42), ("failure", 1, 1), ("wrong", 1, 1)):
            tag = "" if outcome == "wrong" else outcome
            junit(ut_xml, "unselected.Suite" if outcome == "wrong" else SCALA, "test", tag)
            junit(target / "run_dir-new/TEST-pytest-new.xml", Path(I).stem,
                  ("test_unselected" if outcome == "wrong" else CASE) + "[param]", tag)
            run("check-reports.py", scala, unit["shim"], unit["unit-group"], SCALA, cwd=tmp,
                env={"RUNNER_TEMP": str(tmp / "runner")}, rc=urc,
                message="suite mismatch" if outcome == "wrong" else "")
            run("it.py", "reports", target, out, "targeted", before, env=env, rc=irc,
                message="missing targets" if outcome == "wrong" else "")
        print(f"Scala {scala}: target passed; skip requested full fallback; failures and wrong targets rejected.")
    print(f"IT shards: {len(shards[1])} + {len(shards[2])} tracked files, no gaps or duplicates.")


if __name__ == "__main__":
    with TemporaryDirectory(prefix="fork-standalone-") as scratch:
        check(Path(scratch).resolve())
