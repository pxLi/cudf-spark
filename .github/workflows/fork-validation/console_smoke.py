# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check the fork console path with a passing child and an expected exit 42, offline."""

from contextlib import redirect_stdout
from io import StringIO
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch


HERE = Path(__file__).resolve().parent
CANARY = "not-a-real-credential-ConsoleSmokeCanary"


def emit(ok):
    count = 1008 if ok else 24
    if not ok:
        print("[ERROR] java.lang.OutOfMemoryError: Java heap space", file=sys.stderr)
        Path("diagnostics.log").write_text(f"[driver]\nbuild: exit 42\nOutOfMemoryError\nAuthorization: Bearer {CANARY}\n")
    for i in range(count):
        for stream, offset in ((sys.stdout, 10000), (sys.stderr, 20000)):
            if i == count - 2:
                print(f"Authorization: Bearer {CANARY}", file=stream)
            print(f"[INFO] Compiling {i + offset} Scala sources", file=stream)
    return 0 if ok else 42


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def check_view(view, count, limit):
    check(isinstance(view, dict) and set(view) == {"stdout", "stderr", "diagnostics"},
          "Invalid snapshot fields")
    for lines in view.values():
        check(isinstance(lines, list) and all(isinstance(line, str) and CANARY not in line for line in lines),
              "Non-text or unredacted snapshot")
    for name, offset in (("stdout", 10000), ("stderr", 20000)):
        expected = [f"[INFO] Compiling {offset + i} Scala sources" for i in range(count - limit, count)]
        check(view[name] == expected, f"{name}: expected the last {limit} lines in order")


def exercise(directory, ok):
    from console_evidence import snapshot
    from db_console import print_snapshot, run_build

    count = 1008 if ok else 24
    command = [sys.executable, "-I", str(HERE / "console_smoke.py"), "--emit", "pass" if ok else "fail"]
    with (directory / "stdout.log").open("wb") as out, (directory / "stderr.log").open("wb") as err:
        rc = subprocess.run(command, cwd=directory, env={"PATH": os.defpath}, stdin=subprocess.DEVNULL,
                            stdout=out, stderr=err, timeout=15).returncode
    check(rc == (0 if ok else 42), "Unexpected fixture exit status")

    label = "Passing fixture" if ok else "Expected failure (exit 42)"
    live = snapshot(directory, finished=False)
    check_view(live, count, 2)
    final = snapshot(directory, finished=True, ok=ok)
    check_view(final, count, 1000 if ok else 10)
    if not ok:
        error = "OutOfMemoryError"
        check(not any(error in line for line in final["stderr"]), "Diagnostic was not outside the stderr tail")
        check(any(error in line for line in final["diagnostics"]), "Earlier failure diagnostic was lost")
    # The child already ran locally. Simulate only launch and process liveness;
    # execute the controller's read-only queries against the same files across pages.
    (directory / "result.json").write_text(json.dumps({"ok": True, "lines": []}))
    process = Mock()
    process.poll.side_effect = (None, rc)
    context = {}

    def query(code):
        if not context:
            context.update(json=json, _ci_dir=directory, _ci_proc=process, _ci_view={"snapshot": snapshot})
            return {"started": True}
        reply = StringIO()
        with redirect_stdout(reply):
            exec(code, context)
        data = reply.getvalue().strip()
        check(len(data.encode("utf-8")) <= 8000, "Console reply exceeds the page budget")
        return json.loads(data)

    excerpts = {name: [] for name in final}
    with patch("db_console.time.sleep"), patch("db_console.print_snapshot", wraps=print_snapshot) as rendered:
        report = run_build(query, payload="", deadline_seconds=10, label=label, excerpts=excerpts)
        rendered.assert_any_call(live)
    check(excerpts == final, "Final console pages lost, duplicated, or reordered lines")
    check(report.get("ok") is ok, "Wrong final console result")
    if not ok:
        check(any("exit 42" in line for line in report["lines"]), "Original failure exit code was lost")


if __name__ == "__main__":
    if sys.argv[1:] in (["--emit", "pass"], ["--emit", "fail"]):
        sys.exit(emit(sys.argv[2] == "pass"))
    if sys.argv[1:]:
        sys.exit("Unexpected arguments")
    sys.path.insert(0, str(HERE))  # Also works with python -I.
    for ok in (True, False):
        with TemporaryDirectory(prefix="fork-console-") as tmp:
            exercise(Path(tmp), ok)
