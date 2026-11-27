"""Read sanitized driver output over the same HTTPS command context as the build."""

from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import secrets
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "jenkins/databricks"))
from https_transport_v2 import CHANNELS, run_driver


@contextmanager
def driver_output():
    # Guard the whole channel, not each page. Keep Actions groups outside.
    marker = secrets.token_hex(16) if os.getenv("GITHUB_ACTIONS") == "true" else None
    if marker:
        print(f"::stop-commands::{marker}", flush=True)
    try:
        yield
    finally:
        if marker:
            print(f"::{marker}::", flush=True)


def print_snapshot(snapshot):
    for channel in CHANNELS:
        lines = snapshot.get(channel, [])
        if (not isinstance(lines, list) or len(lines) > 5000 or
                any(not isinstance(line, str) or len(line) > 8000 or
                    any(not c.isprintable() and c != "\t" for c in line) for line in lines)):
            raise ValueError("Invalid driver console response")
        stream = sys.stderr if channel == "stderr" else sys.stdout
        for line in lines:
            print(f"driver {channel} | {line}", file=stream, flush=True)


def run_build(query, payload, deadline_seconds=7200, label="pinned build and GPU test", excerpts=None):
    formatter = Path(__file__).with_name("console_evidence.py").read_text()
    result, snapshot = run_driver(query, payload, hashlib.sha256(payload.encode("utf-8")).hexdigest(),
                                  formatter, deadline_seconds=deadline_seconds, label=label)
    for channel, lines in snapshot.items():
        if not lines:
            continue
        if result["ok"]:
            print(f"::group::Final driver {channel}", flush=True)
        try:
            with driver_output():
                print_snapshot({channel: lines})
                if excerpts is not None:
                    excerpts.setdefault(channel, []).extend(lines)
        except (OSError, ValueError):
            print("Driver console rendering incomplete; preserving workload result", flush=True)
        finally:
            if result["ok"]:
                print("::endgroup::", flush=True)
    return result


def verify_failure_console(query):
    # Run after a successful product test on the same driver. This deliberate
    # assertion failure exercises the error path without changing any project tests.
    probe = """
import json, subprocess, sys
from pathlib import Path
test = Path("test_console_probe.py")
test.write_text("import unittest\\nclass ConsoleProbe(unittest.TestCase):\\n    def test_expected_failure(self): self.assertEqual(1, 2)\\n")
with open("stdout.log", "wb") as stdout, open("stderr.log", "wb") as stderr:
    p = subprocess.run([sys.executable, "-m", "unittest", "-v", test.name], stdout=stdout, stderr=stderr)
Path("diagnostics.log").write_bytes(Path("stderr.log").read_bytes())
print(json.dumps({"ok": p.returncode == 0, "fixtureExit": p.returncode}))
"""
    excerpts = {}
    result = run_build(query, probe, deadline_seconds=180, label="deliberately failing console validation",
                       excerpts=excerpts)
    if (result != {"ok": False, "fixtureExit": 1} or
            not any("AssertionError: 1 != 2" in line for line in excerpts.get("diagnostics", []))):
        raise RuntimeError("Console fixture did not preserve the expected failure and its assertion diagnostic")
    print("Console validation: intentional assertion failure observed; product test result unchanged", flush=True)
