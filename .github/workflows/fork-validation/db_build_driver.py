# Copyright (c) 2026, NVIDIA CORPORATION.
"""Fixed workload executed as a Python command on a fresh, dedicated DBR driver."""

from contextlib import ExitStack
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import xml.etree.ElementTree as ET
import zipfile


REVISION = "ce575b3891f0233983f3a924ca73f83cceb5c1cb"
PINS = {
    "thirdparty/parquet-testing": "d79a0101d90dfa3bbb10337626f57a3e8c4b5363",
    "thirdparty/spark-rapids-common": "5b570402a63ce8feeb97494c9998076a1908c0ae",
}
HOME = Path("/home/ubuntu")
SOURCE = HOME / "spark-rapids"
TEST = "range_test.py::test_simple_range"
PATH = "/databricks/python/bin:/usr/local/nvidia/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
STREAMS = (Path("stdout.log"), Path("stderr.log"))
OFFSETS = {}


class Rejected(Exception):
    """Only fixed messages or numeric exit codes may be returned to GitHub."""


def tail(path, start, size=65536):
    with path.open("rb") as log:
        log.seek(max(start, log.seek(0, os.SEEK_END) - size))
        return log.read(size)


def run(label, args, cwd, env, timeout, *, capture=False):
    # The controller starts this process in its own private directory on the disposable driver.
    with ExitStack() as stack:
        out, err = [stack.enter_context(path.open("ab", buffering=0)) for path in STREAMS]
        for stream in (out, err):
            stream.write(f"\n=== {label} ===\n".encode())
        OFFSETS.clear()
        OFFSETS.update(zip(STREAMS, (out.tell(), err.tell())))
        with subprocess.Popen(args, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                              stdout=out, stderr=err, start_new_session=True) as proc:
            try:
                rc = proc.wait(timeout)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                raise Rejected(f"{label}: exceeded {timeout}s") from None
        if rc:
            recent = b"\n".join(tail(path, pos) for path, pos in OFFSETS.items()).lower()
            # Never return raw text: even normal Maven or Spark exceptions can include endpoints.
            for needle, category in ((b"no space left", "disk full"), (b"permission denied", "permission denied"),
                                     (b"a password is required", "noninteractive sudo denied"),
                                     (b"could not resolve host", "DNS resolution failed"),
                                     (b"could not resolve dependencies", "Maven dependency resolution failed"),
                                     (b"compilation failure", "Maven compilation failed"),
                                     (b"outofmemoryerror", "JVM out of memory")):
                if needle in recent:
                    raise Rejected(f"{label}: exit {rc} ({category})")
            raise Rejected(f"{label}: exit {rc}")
        if capture:
            with STREAMS[0].open("rb") as log:
                log.seek(OFFSETS[STREAMS[0]])
                return log.read(1024).strip()


def checkout(scratch, env, destination):
    # Empty Git home, no shared credentials, prompts, URL rewrites, or non-HTTPS transports.
    git_env = {**env, "HOME": str(scratch), "XDG_CONFIG_HOME": str(scratch),
               "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
               "GIT_ALLOW_PROTOCOL": "https", "GIT_TERMINAL_PROMPT": "0"}

    def git(label, *args, cwd=destination, capture=False):
        return run(label, ["git", *args], cwd, git_env, 240, capture=capture)

    destination.mkdir(mode=0o700)
    git("init", "init", "--quiet")
    git("fetch", "fetch", "--quiet", "--no-tags", "--depth=1",
        "https://github.com/NVIDIA/cudf-spark.git", REVISION)
    git("checkout", "checkout", "--quiet", "--detach", "FETCH_HEAD")
    if git("revision", "rev-parse", "HEAD", capture=True) != REVISION.encode():
        raise Rejected("checkout: unexpected source revision")
    git("submodules", "submodule", "update", "--init", "--depth=1", "--", *PINS)
    for path, sha in PINS.items():
        if git("submodule-revision", "rev-parse", "HEAD", cwd=destination / path, capture=True) != sha.encode():
            raise Rejected("checkout: unexpected submodule revision")


def log_excerpt(label):
    path = STREAMS[0]
    if path not in OFFSETS:
        return []
    try:
        sample = tail(path, OFFSETS[path])
    except OSError:
        return []
    markers = {"[INFO] BUILD SUCCESS", "[INFO] BUILD FAILURE", "Initializing build for Databricks:",
               "Reading package lists...", "Building dependency tree...", "Reading state information..."}
    found = []
    for line in sample.decode(errors="replace").splitlines():
        line = line.strip()
        if line in markers or re.fullmatch(r"(?:\[INFO\] )?Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+, Skipped: [0-9]+", line):
            if line not in found and len(found) < 10:
                found.append(line)
    return [f"{label} output | {line}" for line in found]


class Excerpt:
    """Retain short sections verbatim and both ends of long ones; memory is bounded."""

    def __init__(self, half):
        self.half = half
        self.first = bytearray()
        self.last = bytearray()
        self.size = 0

    def add(self, data):
        self.size += len(data)
        split = min(self.half - len(self.first), len(data))
        self.first.extend(data[:split])
        self.last.extend(data[max(split, len(data) - self.half):])
        del self.last[:max(0, len(self.last) - self.half)]

    def text(self):
        missing = self.size - len(self.first) - len(self.last)
        middle = f"\n[... {missing} bytes omitted ...]\n".encode() if missing else b""
        return (self.first + middle + self.last).decode("utf-8", "replace")


def log_sections(path, start=0):
    # Scan the whole failed phase to find sections even in the middle of a large log.
    # This file is driver-private; console_evidence redacts before transmitting.
    marked, fallback = Excerpt(16384), Excerpt(4096)
    end = None
    line_start = True
    try:
        with path.open("rb") as log:
            remaining = max(0, os.fstat(log.fileno()).st_size - start)
            log.seek(start)
            while remaining:
                chunk = log.readline(min(16384, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                fallback.add(chunk)
                tag = re.sub(rb"\x1b\[[0-9;]*m", b"", chunk).strip() if line_start else b""
                line_start = chunk.endswith(b"\n")
                if end is None:
                    if tag.startswith(b"=== Databricks failure summary:"):
                        end = b"=== End Databricks failure summary ==="
                    elif tag == b"=== Databricks GPU diagnostics ===":
                        end = b"=== End Databricks GPU diagnostics ==="
                    elif re.fullmatch(rb"=+ (?:FAILURES|ERRORS) =+", tag):
                        end = b"pytest"
                    elif tag.startswith((b"----- First Spark failure records:", b"----- Spark driver/pytest log:")):
                        # The repo launcher emits its worker/executor diagnostic blocks at phase end.
                        end = b"end-of-phase"
                if end is not None:
                    marked.add(chunk)
                    if tag == end or (end == b"pytest" and re.fullmatch(
                            rb"=+ (?!FAILURES =|ERRORS =).+ =+", tag)):
                        end = None
    except OSError:
        return ""
    return (marked if marked.size else fallback).text()


def worker_section(directory):
    try:
        for path in directory.glob("*_worker_logs.log"):
            if path.is_file() and not path.is_symlink():
                return log_sections(path)
    except Exception:
        pass  # Optional evidence must not replace the launcher's exit status.
    return ""


def failure_diagnostics(stage, reason, case, worker_evidence):
    blocks = ["[driver]", reason]
    for path, start in OFFSETS.items():
        section = log_sections(path, start)
        if section:
            blocks.extend([f"[{path.name}]", section])
    if worker_evidence:
        blocks.extend(["[Spark worker log]", worker_evidence])
    if case is not None:
        for tag in ("failure", "error", "skipped"):
            node = case.find(tag)
            if node is not None:
                detail = Excerpt(16384)
                for part in (node.get("message", ""), "\n", node.text or ""):
                    detail.add(part.encode("utf-8"))
                blocks.extend([f"[JUnit {tag}: {TEST}]", detail.text()])
    elif stage in ("pytest", "JUnit"):
        blocks.append("[JUnit] No matching single-case report")
    # Resource evidence, not the full nvidia-smi process table, serial numbers or topology.
    try:
        gpu = subprocess.run(["nvidia-smi", "--query-gpu=memory.used,memory.total", "--format=csv,noheader,nounits"],
                             env={"PATH": PATH}, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, timeout=5, check=True).stdout.strip()
        if re.fullmatch(rb"[0-9]{1,7}, [0-9]{1,7}", gpu):
            blocks.extend(["[GPU memory used, total (MiB)]", gpu.decode("ascii")])
    except (OSError, subprocess.SubprocessError):
        pass
    Path("diagnostics.log").write_text("\n".join(blocks) + "\n", encoding="utf-8")


def built_jar(target):
    for path in sorted(target.glob("cudf-spark_2.13-*.jar")):
        with zipfile.ZipFile(path) as jar:
            if ("ai/rapids/cudf/NativeDepUtil.class" in jar.namelist()
                    and any(name.endswith("spark400db173/SparkShimServiceProvider.class")
                            for name in jar.namelist())):
                return path
    raise Rejected("jar: no Scala 2.13 distribution with cuDF and DBR 17.3 shim")


def junit_case(directory):
    reports = list(directory.glob("TEST-pytest-*.xml"))
    try:
        if len(reports) != 1 or reports[0].is_symlink() or reports[0].stat().st_size > 8 * 1024 * 1024:
            return None
        cases = list(ET.parse(reports[0]).iter("testcase"))
    except (OSError, ET.ParseError):
        return None
    if (len(cases) != 1 or cases[0].get("name", "").partition("[")[0] != "test_simple_range"
            or cases[0].get("classname", "").split(".")[-1] != "range_test"):
        return None
    return cases[0]


def driver_report():
    with ExitStack() as lifetime:
        return driver_result(lifetime)


def driver_result(lifetime):
    lines = []
    evidence = None
    case = None
    worker_evidence = ""
    logs_ready = False
    stage = "prerequisites"
    try:
        # Never truncate an earlier run or follow a pre-existing symlink.
        for path in (*STREAMS, Path("diagnostics.log")):
            os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
        logs_ready = True
        if Path("/databricks/DBR_VERSION").read_text().strip() != "17.3":
            raise Rejected("prerequisites: expected DBR 17.3")
        if not Path("/databricks/jars").is_dir() or not Path("/databricks/spark/bin/pyspark").is_file():
            raise Rejected("prerequisites: missing Databricks Spark or runtime jars")
        env = {"PATH": PATH, "HOME": str(HOME), "LANG": "C.UTF-8", "DB_HOME": "/databricks",
               "BASE_SPARK_VERSION": "4.0.0", "WITH_DEFAULT_UPSTREAM_SHIM": "0",
               "DBFS_CACHE_DIR": str(HOME / ".cache/spark-ci"), "DEBIAN_FRONTEND": "noninteractive"}
        scratch = Path(lifetime.enter_context(tempfile.TemporaryDirectory(prefix="db-build-")))
        usable_home = HOME.is_dir() and os.access(HOME, os.W_OK | os.X_OK)
        if usable_home and any(os.path.lexists(p) for p in (SOURCE, HOME / "spark-rapids-built.tgz")):
            raise Rejected("prerequisites: refusing to overwrite existing build or source")
        stage = "checkout"
        checkout(scratch, env, SOURCE if usable_home else scratch / "spark-rapids")
        lines.append(f"Verified source {REVISION} and both pinned submodules")
        if not usable_home:
            raise Rejected("prerequisites: build requires writable /home/ubuntu")
        stage = "prerequisites"
        run("sudo-check", ["sudo", "-n", "true"], HOME, env, 10)
        if not run("gpu-check", ["nvidia-smi", "-L"], HOME, env, 30, capture=True).startswith(b"GPU 0:"):
            raise Rejected("prerequisites: no visible NVIDIA GPU")
        lines.append("DBR 17.3, NVIDIA GPU and build prerequisites confirmed")
        stage = "build"
        try:
            run(stage, ["bash", "jenkins/databricks/build.sh"], SOURCE, env, 75 * 60)
        finally:
            lines.extend(log_excerpt(stage))
        lines.append("jenkins/databricks/build.sh: exit 0 (DB shim only)")
        stage = "jar"
        jar = built_jar(SOURCE / "scala2.13/dist/target")
        lines.append(f"Verified Scala 2.13 distribution and spark400db173 shim ({jar.stat().st_size} bytes)")
        stage = "pytest"
        reports = scratch / "junit"
        reports.mkdir()
        test_env = {**env, "PLUGIN_JAR": str(jar), "TESTS": TEST, "TEST_PARALLEL": "0",
                    "RUN_DIR": str(reports), "TEST_TYPE": "pre-commit", "REDUCED_IT": "false"}
        try:
            run(stage, ["bash", "-ec", """
source jenkins/databricks/setup.sh
source jenkins/databricks/common_vars.sh
[[ "$SPARK_SHIM_VER" == spark400db173 ]]
exec bash integration_tests/run_pyspark_from_build.sh --runtime_env=databricks --test_type=pre-commit
"""], SOURCE, test_env, 25 * 60)
        finally:
            case = junit_case(reports)
            if case is not None:
                evidence = {tag: int(case.find(tag) is not None) for tag in ("failure", "error", "skipped")}
            if evidence is None or any(evidence.values()):
                worker_evidence = worker_section(reports)
            lines.extend(log_excerpt(stage))
        lines.append("Databricks integration launcher: exit 0")
        stage = "JUnit"
        if evidence is None or any(evidence.values()):
            raise Rejected("JUnit: expected exactly one passing range_test.test_simple_range")
        lines.append(f"JUnit: {TEST} passed (1 executed, 0 skipped)")
    except Rejected as exc:
        lines.append(str(exc))
    except OSError as exc:
        lines.append(f"{stage}: operating system error (errno {exc.errno})")
    except Exception:
        lines.append(f"{stage}: unexpected or malformed driver evidence")
    else:
        return {"ok": True, "lines": lines, "junit": evidence}
    if logs_ready:
        try:
            failure_diagnostics(stage, lines[-1], case, worker_evidence)
        except Exception:
            lines.append("Could not write driver failure diagnostics")
    return {"ok": False, "lines": lines, "junit": evidence}


if __name__ == "__main__":
    print(json.dumps(driver_report()))
