"""Read sanitized driver output over the same HTTPS command context as the build."""

import json
import os
from pathlib import Path
import secrets
import sys
import time


CHANNELS = ("stdout", "stderr", "diagnostics")

class TransientRequest(RuntimeError):
    """A temporary API or network failure, not a workload failure."""


def observe(call):
    for delay in (2, 5, None):
        try:
            return call()
        except TransientRequest:
            if delay is None:
                raise
            time.sleep(delay)


def print_snapshot(snapshot):
    # Suspend workflow commands while printing external text. Pytest node IDs,
    # Maven output and stack traces keep ordinary punctuation, including "::".
    marker = secrets.token_hex(16) if os.getenv("GITHUB_ACTIONS") == "true" and any(snapshot.values()) else None
    if marker:
        print(f"::stop-commands::{marker}", flush=True)
    try:
        for channel in CHANNELS:
            lines = snapshot.get(channel, [])
            if (not isinstance(lines, list) or len(lines) > 5000 or
                    any(not isinstance(line, str) or len(line) > 8000 or
                        any(not c.isprintable() and c != "\t" for c in line) for line in lines)):
                raise ValueError("Invalid driver console response")
            stream = sys.stderr if channel == "stderr" else sys.stdout
            for line in lines:
                print(f"driver {channel} | {line}", file=stream, flush=True)
    finally:
        if marker:
            print(f"::{marker}::", flush=True)


def run_build(query, payload, deadline_seconds=7200, label="pinned build and GPU test", excerpts=None):
    formatter = Path(__file__).with_name("console_evidence.py").read_text()
    # A command result is only available on completion. Start one driver-local process,
    # then issue short read-only commands in the same context while it is running.
    setup = f"""
import json, os, subprocess, sys, tempfile
from pathlib import Path
_ci_dir = Path(tempfile.mkdtemp(prefix="gpu-ci-console-"))
_ci_script = _ci_dir / "build_driver.py"
_ci_script.write_text({payload!r})
_ci_view = {{}}
exec(compile({formatter!r}, "console_evidence.py", "exec"), _ci_view)
with (_ci_dir / "result.json").open("xb") as _ci_out:
    _ci_proc = subprocess.Popen([sys.executable, "-I", str(_ci_script)], cwd=_ci_dir,
        env={{"PATH":os.defpath, "LANG":"C.UTF-8"}}, stdin=subprocess.DEVNULL,
        stdout=_ci_out, stderr=subprocess.DEVNULL, start_new_session=True)
print(json.dumps({{"started": True}}))
"""
    if query(setup) != {"started": True}:
        raise RuntimeError("Driver did not confirm build process launch; not resubmitting")
    print(f"Started {label}; sampling driver stdout and stderr over HTTPS", flush=True)

    poll = """
_ci_rc = _ci_proc.poll()
try:
    _ci_preview = _ci_view["snapshot"](_ci_dir) if _ci_rc is None else {}
except Exception:
    _ci_preview = {}
print(json.dumps({"rc":_ci_rc,"preview":_ci_preview}))
"""
    last = {}
    last_notice = 0
    start = time.monotonic()
    while time.monotonic() - start < deadline_seconds:
        state = observe(lambda: query(poll))
        if state["rc"] is not None:
            break
        preview = state["preview"]
        try:
            print_snapshot({k: v for k, v in preview.items() if last.get(k) != v})
            last = preview
        except (TypeError, ValueError):
            print("Driver live excerpt unavailable; continuing build", flush=True)
        if time.monotonic() >= last_notice:
            print(f"Build/test process still running ({int(time.monotonic() - start)}s elapsed)", flush=True)
            last_notice = time.monotonic() + 60
        time.sleep(15)
    else:
        raise RuntimeError("Driver build/test process exceeded its deadline")

    result = query("""
_ci_result = _ci_dir / "result.json"
try:
    if _ci_result.stat().st_size > 32000:
        raise ValueError()
    _ci_report = json.loads(_ci_result.read_text())
except (OSError, ValueError):
    _ci_report = {"ok":False,"lines":["Driver exited without a complete build report"]}
print(json.dumps(_ci_report))
""")
    if state["rc"] != 0:
        result = {"ok": False, "lines": [f"Driver build process exit {int(state['rc'])}"]}
    passed = result.get("ok") is True
    print("Final driver console: " + ("success tails" if passed else "failure tails and diagnostic evidence"), flush=True)
    # Fetch small pages. A truncated command response would discard the entire JSON tail.
    try:
        counts = query(f"""
_ci_pages = {{}}
for _key, _lines in _ci_view["snapshot"](_ci_dir, finished=True, ok={passed!r}).items():
    _parts, _part, _size = [], [], 2
    for _line in _lines:
        _n = len(json.dumps(_line)) + 2
        if _part and _size + _n > 8000:
            _parts.append(_part)
            _part, _size = [], 2
        _part.append(_line)
        _size += _n
    _ci_pages[_key] = _parts + ([_part] if _part else [])
print(json.dumps({{k:len(v) for k,v in _ci_pages.items()}}))
""")
        for channel in CHANNELS:
            n = counts[channel]
            if type(n) is not int or not 0 <= n <= 32:
                raise ValueError("Invalid console page count")
            if passed and n:
                print(f"::group::Final driver {channel}", flush=True)
            try:
                for i in range(n):
                    lines = query(f'print(json.dumps(_ci_pages[{channel!r}][{i}]))')
                    print_snapshot({channel: lines})
                    if excerpts is not None:
                        excerpts.setdefault(channel, []).extend(lines)
            finally:
                if passed and n:
                    print("::endgroup::", flush=True)
    except (OSError, RuntimeError, KeyError, TypeError, ValueError):
        print("Driver console retrieval incomplete; preserving workload result", flush=True)
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
