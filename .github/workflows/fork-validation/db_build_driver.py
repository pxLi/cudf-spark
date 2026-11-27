# Copyright (c) 2026, NVIDIA CORPORATION.
"""Fixed workload executed as a Python command on a fresh, dedicated DBR driver."""

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


class Rejected(Exception):
    """Only fixed messages or numeric exit codes may be returned to GitHub."""


def run(label, args, cwd, env, scratch, timeout, *, capture=False):
    with (scratch / (label + ".log")).open("wb+") as log:
        with subprocess.Popen(args, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                              stdout=log, stderr=subprocess.STDOUT, start_new_session=True) as proc:
            try:
                rc = proc.wait(timeout)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                raise Rejected(f"{label}: exceeded {timeout}s") from None
        if rc:
            log.seek(max(0, log.tell() - 65536))
            tail = log.read().lower()
            # Never return raw text: even normal Maven or Spark exceptions can include endpoints.
            for needle, category in ((b"no space left", "disk full"), (b"permission denied", "permission denied"),
                                     (b"a password is required", "noninteractive sudo denied"),
                                     (b"could not resolve host", "DNS resolution failed"),
                                     (b"could not resolve dependencies", "Maven dependency resolution failed"),
                                     (b"compilation failure", "Maven compilation failed"),
                                     (b"outofmemoryerror", "JVM out of memory")):
                if needle in tail:
                    raise Rejected(f"{label}: exit {rc} ({category})")
            raise Rejected(f"{label}: exit {rc}")
        if capture:
            log.seek(0)
            return log.read(1024).strip()


def checkout(scratch, env, destination):
    # Empty Git home, no shared credentials, prompts, URL rewrites, or non-HTTPS transports.
    git_env = {**env, "HOME": str(scratch), "XDG_CONFIG_HOME": str(scratch),
               "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
               "GIT_ALLOW_PROTOCOL": "https", "GIT_TERMINAL_PROMPT": "0"}

    def git(label, *args, cwd=destination, capture=False):
        return run(label, ["git", *args], cwd, git_env, scratch, 240, capture=capture)

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


def log_excerpt(path):
    if not path.is_file():
        return []
    markers = {"[INFO] BUILD SUCCESS", "[INFO] BUILD FAILURE", "Initializing build for Databricks:",
               "Reading package lists...", "Building dependency tree...", "Reading state information..."}
    found = []
    with path.open(errors="replace") as stream:
        for line in stream:
            line = line.strip()
            if line in markers or re.fullmatch(r"(?:\[INFO\] )?Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+, Skipped: [0-9]+", line):
                if line not in found and len(found) < 10:
                    found.append(line)
    return [f"{path.stem} output | {line}" for line in found]


def built_jar(target):
    for path in sorted(target.glob("cudf-spark_2.13-*.jar")):
        with zipfile.ZipFile(path) as jar:
            if ("ai/rapids/cudf/NativeDepUtil.class" in jar.namelist()
                    and any(name.endswith("spark400db173/SparkShimServiceProvider.class")
                            for name in jar.namelist())):
                return path
    raise Rejected("jar: no Scala 2.13 distribution with cuDF and DBR 17.3 shim")


def junit_counts(directory):
    reports = list(directory.glob("TEST-pytest-*.xml"))
    if len(reports) != 1 or reports[0].stat().st_size > 8 * 1024 * 1024:
        return None
    try:
        cases = list(ET.parse(reports[0]).iter("testcase"))
    except (OSError, ET.ParseError):
        return None
    if (len(cases) != 1 or cases[0].get("name", "").partition("[")[0] != "test_simple_range"
            or cases[0].get("classname", "").split(".")[-1] != "range_test"):
        return None
    return {tag: int(cases[0].find(tag) is not None) for tag in ("failure", "error", "skipped")}


def driver_report():
    lines = []
    evidence = None
    stage = "prerequisites"
    try:
        if Path("/databricks/DBR_VERSION").read_text().strip() != "17.3":
            raise Rejected("prerequisites: expected DBR 17.3")
        if not Path("/databricks/jars").is_dir() or not Path("/databricks/spark/bin/pyspark").is_file():
            raise Rejected("prerequisites: missing Databricks Spark or runtime jars")
        env = {"PATH": PATH, "HOME": str(HOME), "LANG": "C.UTF-8", "DB_HOME": "/databricks",
               "BASE_SPARK_VERSION": "4.0.0", "WITH_DEFAULT_UPSTREAM_SHIM": "0",
               "DBFS_CACHE_DIR": str(HOME / ".cache/spark-ci"), "DEBIAN_FRONTEND": "noninteractive"}
        with tempfile.TemporaryDirectory(prefix="db-build-") as tmp:
            scratch = Path(tmp)
            usable_home = HOME.is_dir() and os.access(HOME, os.W_OK | os.X_OK)
            if usable_home and any(os.path.lexists(p) for p in (SOURCE, HOME / "spark-rapids-built.tgz")):
                raise Rejected("prerequisites: refusing to overwrite existing build or source")
            stage = "checkout"
            checkout(scratch, env, SOURCE if usable_home else scratch / "spark-rapids")
            lines.append(f"Verified source {REVISION} and both pinned submodules")
            if not usable_home:
                raise Rejected("prerequisites: build requires writable /home/ubuntu")
            stage = "prerequisites"
            run("sudo-check", ["sudo", "-n", "true"], HOME, env, scratch, 10)
            if not run("gpu-check", ["nvidia-smi", "-L"], HOME, env, scratch, 30, capture=True).startswith(b"GPU 0:"):
                raise Rejected("prerequisites: no visible NVIDIA GPU")
            lines.append("DBR 17.3, NVIDIA GPU and build prerequisites confirmed")
            stage = "build"
            try:
                run(stage, ["bash", "jenkins/databricks/build.sh"], SOURCE, env, scratch, 75 * 60)
            finally:
                lines.extend(log_excerpt(scratch / "build.log"))
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
"""], SOURCE, test_env, scratch, 25 * 60)
            finally:
                evidence = junit_counts(reports)
                lines.extend(log_excerpt(scratch / "pytest.log"))
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
    return {"ok": False, "lines": lines, "junit": evidence}


print(json.dumps(driver_report()))
