# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check the fork console path with a passing child and an expected exit 42, offline."""

from contextlib import redirect_stderr, redirect_stdout
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
OUT_OF_SCOPE = "[INFO] This sibling log is not part of the driver console"
PUBLIC = {
    "stdout": (
        "[INFO] Resolving org.apache.parquet:parquet-hadoop:jar:1.15.2",
        "[INFO] Downloaded from central: https://repo.maven.apache.org/maven2/org/apache/parquet/"
        "parquet-hadoop/1.15.2/parquet-hadoop-1.15.2.jar (1.2 MB at 3.1 MB/s)",
    ),
    "stderr": (
        "/workspace/spark/sql/core/src/test/scala/example/ParquetReplaySuite.scala:73: error: type mismatch;",
        "\tat org.apache.spark.sql.example.ParquetReplaySuite$.$anonfun$checkSchema$2(ParquetReplaySuite.scala:73)",
    ),
}


def emit(ok):
    count = 1008 if ok else 24
    if not ok:
        print("[ERROR] java.lang.OutOfMemoryError: Java heap space", file=sys.stderr)
        Path("diagnostics.log").write_text(f"[driver]\nbuild: exit 42\nOutOfMemoryError\nAuthorization: Bearer {CANARY}\n")
    for i in range(count):
        for stream, offset in ((sys.stdout, 10000), (sys.stderr, 20000)):
            if i == count - 2:
                print(f"Authorization: Bearer {CANARY}", file=stream)
                print(f"::notice::{CANARY}", f"--token {CANARY}",
                      f"https://repo.maven.apache.org/example.jar?signature={CANARY}", sep="\n", file=stream)
            print(f"[INFO] Compiling {i + offset} Scala sources", file=stream)
    for name, stream in (("stdout", sys.stdout), ("stderr", sys.stderr)):
        print(*PUBLIC[name], sep="\n", file=stream)
    return 0 if ok else 42


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def check_view(view, count, limit):
    check(isinstance(view, dict) and set(view) == {"stdout", "stderr", "diagnostics"},
          "Invalid snapshot fields")
    for lines in view.values():
        check(isinstance(lines, list) and all(isinstance(line, str) and CANARY not in line
                                              and OUT_OF_SCOPE not in line for line in lines),
              "Non-text, sensitive or out-of-scope console output")
    for name, offset in (("stdout", 10000), ("stderr", 20000)):
        markers = [f"[INFO] Compiling {offset + i} Scala sources" for i in range(count)]
        check(view[name] == (markers + list(PUBLIC[name]))[-limit:],
              f"{name}: missing, modified, or reordered public output")


def check_render(live):
    from db_console import driver_output, print_snapshot

    for in_actions in ("true", "false"):
        out, err = StringIO(), StringIO()
        with patch.dict(os.environ, GITHUB_ACTIONS=in_actions), redirect_stdout(out), redirect_stderr(err):
            with driver_output():
                for name in ("stdout", "stderr"):
                    for line in live[name]:
                        print_snapshot({name: [line]})
        # Compare bytes including stream prefixes, original indentation and newlines.
        stdout = out.getvalue().splitlines(keepends=True)
        if in_actions == "true":
            marker = stdout[0].removeprefix("::stop-commands::").rstrip("\n")
            check(stdout[0] == f"::stop-commands::{marker}\n" and marker and stdout[-1] == f"::{marker}::\n",
                  "Missing or unbalanced GitHub Actions command guard")
            stdout = stdout[1:-1]
        expected_out = "".join(f"driver stdout | {line}\n" for line in PUBLIC["stdout"])
        expected_err = "".join(f"driver stderr | {line}\n" for line in PUBLIC["stderr"])
        check("".join(stdout).encode() == expected_out.encode() and err.getvalue().encode() == expected_err.encode(),
              "Stock build output was changed during rendering")


def exercise(directory, ok):
    from console_evidence import snapshot
    from db_console import driver_output, print_snapshot, run_build

    count = 1008 if ok else 24
    (directory / "setup.log").write_text(OUT_OF_SCOPE + "\n", encoding="utf-8")
    if not ok:
        # The live window contains neither PEM delimiter; payload alone is not public console text.
        log = directory / "stderr.log"
        log.write_bytes(b"-----BEGIN PRIVATE KEY-----\n" + b"Q29uc29sZVNtb2tlQ2FuYXJ5\n" * 12000)
        check(not any(snapshot(directory).values()), "Credential block or sibling output escaped the live window")
        with log.open("ab") as stream:
            stream.write(b"-----END PRIVATE KEY-----\nordinary text after the block\n")
        check(snapshot(directory, finished=True)["stderr"] == ["ordinary text after the block"],
              "Closing a clipped credential block lost the next public line or leaked its body")
    command = [sys.executable, "-I", str(HERE / "console_smoke.py"), "--emit", "pass" if ok else "fail"]
    with (directory / "stdout.log").open("wb") as out, (directory / "stderr.log").open("wb") as err:
        rc = subprocess.run(command, cwd=directory, env={"PATH": os.defpath}, stdin=subprocess.DEVNULL,
                            stdout=out, stderr=err, timeout=15).returncode
    check(rc == (0 if ok else 42), "Unexpected fixture exit status")

    label = "Passing fixture" if ok else "Expected failure (exit 42)"
    live = snapshot(directory, finished=False)
    check_view(live, count, 2)
    if ok:  # No diagnostic channel in the passing fixture.
        check_render(live)
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
    process.poll.side_effect = (None, None, rc)
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
    with (patch("db_console.time.sleep"), patch("db_console.print_snapshot", wraps=print_snapshot) as rendered,
          patch("db_console.driver_output", wraps=driver_output) as guard):
        report = run_build(query, payload="", deadline_seconds=10, label=label, excerpts=excerpts)
        rendered.assert_any_call(live)
        check(guard.call_count == 1 + sum(bool(lines) for lines in final.values()),
              "Command markers must not repeat for each live poll or final page")
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
