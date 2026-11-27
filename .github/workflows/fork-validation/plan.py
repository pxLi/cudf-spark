# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise the production planners offline, then emit one representative row per lane kind."""

import ast
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory


WORKFLOWS = Path(__file__).resolve().parents[1]
ROOT = WORKFLOWS.parents[1]
sys.path.insert(0, str(WORKFLOWS / "databricks-ci"))
from source import db_rows, standalone_rows

U = "tests/src/test/scala/com/nvidia/spark/rapids/OrcScanRetrySuite.scala"
I = "integration_tests/src/main/python/parquet_test.py"
UT = ".github/workflows/gpu-ci/check-reports.py"
IT = ".github/workflows/gpu-ci/it.sh"
DB = "jenkins/databricks/test.sh"
RULES = ".github/workflows/gpu-ci/impact-rules.json"
SCALA = "com.nvidia.spark.rapids.OrcScanRetrySuite"
CASE = "test_parquet_decimal_scale_change_boundaries"


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def seed(repo):
    # A small independent repo keeps pilot code out of reference scans. Copy the
    # real fixtures, policy, permissions, catalog and runtime discovery inputs.
    paths = [U, I, UT, IT, DB, RULES, "pom.xml", "scala2.13/pom.xml", ".github/gpu-ci-suites.json",
             ".github/actions/spark-archive-cache/releases.json", ".github/workflows/databricks-ci/paths.json"]
    providers = ROOT.glob("sql-plugin/src/main/spark*db*/**/SparkShimServiceProvider.scala")
    for name in [*paths, *(p.relative_to(ROOT) for p in providers)]:
        target = repo / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, target)
    git(repo, "init", "-q")
    git(repo, "add", ".")
    return git(repo, "commit-tree", git(repo, "write-tree"), "-m", "fixture base")


def test_edits(repo):
    scala, count = re.subn(r'(\n  test\([^\n]*\{\n)', r'\1    assert(true)\n', (repo / U).read_text(), count=1)
    if count != 1:
        raise ValueError(f"No test body found in {U}")
    python = (repo / I).read_text()
    fn = next(n for n in ast.parse(python).body if isinstance(n, ast.FunctionDef) and n.name == CASE)
    lines = python.splitlines(keepends=True)
    lines.insert(fn.body[0].lineno - 1, "    assert True  # offline selection fixture\n")
    return {U: scala, I: "".join(lines)}


def plan(repo, base, changes, title="Probe"):
    # Switch only this disposable fixture. A two-parent commit exercises the
    # real DB base/head/merge check, including when DB would be skipped.
    git(repo, "read-tree", "--reset", "-u", base)
    for path, contents in changes.items():
        (repo / path).write_text(contents)
    git(repo, "add", "-A")
    tree = git(repo, "write-tree")
    head = git(repo, "commit-tree", tree, "-p", base, "-m", "probe head")
    merge = git(repo, "commit-tree", tree, "-p", base, "-p", head, "-m", "probe merge")
    git(repo, "update-ref", "--no-deref", "HEAD", merge)
    rev = dict(base=base, head=head, merge=merge, policy="0100" if "[databricks]" in title else "0000")
    return standalone_rows(repo, rev) + db_rows(repo, rev, title)


def probe(repo):
    base = seed(repo)
    edit = test_edits(repo)
    full_ut, full_it, db, doc, product = [
        {path: (repo / path).read_text() + "\n" if (repo / path).exists() else "fixture\n"}
        for path in (UT, IT, DB, "notes.md", "pom.xml")]
    ut, it = {U: edit[U]}, {I: edit[I]}
    # Counts are for this fork pilot's exact catalog. Do not change production
    # selectors to meet them; recheck the catalog if the pilot is reused.
    scenarios = [
        ("docs", doc, (0, 0, 0), set()),
        ("docs [databricks]", doc, (0, 0, 4), set()),
        ("UT helper", full_ut, (4, 0, 0), set()),
        ("IT helper", full_it, (0, 4, 0), set()),
        ("DB executable", db, (0, 0, 4), set()),
        ("two targeted", ut | it, (4, 2, 0), {"unit", "integration"}),
        ("full UT, targeted IT", full_ut | it, (4, 2, 0), {"integration"}),
        ("targeted UT, full IT", ut | full_it, (4, 4, 0), {"unit"}),
        ("product and DB", product | db, (4, 4, 4), set()),
        ("two targeted and DB", ut | it | db, (4, 2, 4), {"unit", "integration"}),
    ]
    sample = []
    for label, changes, counts, targeted in scenarios:
        rows = plan(repo, base, changes, label)
        kinds = ("unit", "integration", "databricks")
        actual = tuple(sum(row["kind"] == kind for row in rows) for kind in kinds)
        if actual != counts or len({row["group"] for row in rows}) != sum(counts):
            raise ValueError(f"{label}: got {actual} lanes, expected {counts} distinct UT/IT/DB lanes")
        if "integration" in targeted:
            shards = {(r["scala"], r["shard"]) for r in rows if r["kind"] == "integration"}
            if shards != {("2.12", 2), ("2.13", 2)}:
                raise ValueError(f"{label}: wrong targeted IT shards: {shards}")
        cells = {(r["runtime"], r["mode"]) for r in rows if r["kind"] == "databricks"}
        if counts[2] and (len(cells) != counts[2] or
                          cells != {(runtime, part) for runtime, _ in cells for part in ("CI_PART1", "CI_PART2")}):
            raise ValueError(f"{label}: missing or unexpected Databricks cells: {cells}")
        for row in rows:
            kind = row["kind"]
            expected = [SCALA] if kind == "unit" else [f"parquet_test.py::{CASE}"]
            if row.get("targets") != (expected if kind in targeted else None):
                raise ValueError(f"{label}: wrong targets for {row['group']}: {row.get('targets')}")
        print(f"{label}: UT={actual[0]} IT={actual[1]} DB={actual[2]}", flush=True)
        if label == "two targeted and DB":
            sample = [next(row for row in rows if row["kind"] == kind) for kind in kinds]
    if len(sample) != 3:
        raise ValueError("Missing UT, IT or DB pilot sample")
    return {"include": sample}


def main():
    output = Path(os.environ["GITHUB_OUTPUT"])
    workspace = Path(os.environ["GITHUB_WORKSPACE"])
    if not (workspace / "common/workflow-suite-selector/index.js").is_file():
        raise SystemExit("Checkout spark-rapids-common/workflow-suite-selector under GITHUB_WORKSPACE/common")
    os.environ.update(GIT_AUTHOR_NAME="Fork validation", GIT_COMMITTER_NAME="Fork validation",
                      GIT_AUTHOR_EMAIL="fixture@example.invalid", GIT_COMMITTER_EMAIL="fixture@example.invalid",
                      GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1",
                      DB_ENABLED="true", DB_RUNNER_GROUP="offline-fixture")
    with TemporaryDirectory(prefix="fork-plan-") as tmp:
        matrix = probe(Path(tmp))
    with output.open("a", encoding="utf-8") as stream:
        print("matrix=" + json.dumps(matrix, separators=(",", ":")), file=stream)


if __name__ == "__main__":
    main()
