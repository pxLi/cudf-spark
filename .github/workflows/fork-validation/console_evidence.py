# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read bounded, public excerpts of the fork pilot's driver logs."""

import json
import os
import re
import stat


_WINDOW = 256 * 1024
_BUDGET = 56 * 1024
_OMITTED = "[... omitted ...]"
_SGR = re.compile(rb"\x1b\[[0-9;:]*m")
_PRIVATE = re.compile(
    r"(?i)\b(?:authorization|bearer|cookie|credential\w*|password|passwd|secret\w*|token|"
    r"private key|proxy|[a-z0-9_]*(?:_key|_token|_password|_secret))\b|"
    r"-----BEGIN |://|(?<=\S)@(?=\S)|[?&][\w%-]+=|(?:^|\s)\++(?:\s|$)|"
    r"(?<!\w)(?:[0-9a-f:.]{0,45}::[0-9a-f:.]{0,45}|(?:[0-9a-f]{1,4}:){3,}[0-9a-f:.]+)(?!\w)"
)
_TOKEN = re.compile(r"[A-Za-z0-9_./@%$=+~\\-]+")
# Only public build vocabulary survives verbatim, not arbitrary Maven arguments,
# host labels, dependency coordinates, environment values or driver exception text.
_WORDS = set("""
a an the and or in of on at for to from by with without as is was are were not no
info warn warning warnings error errors debug fatal success successful failure failures failed
starting started running finished complete completed skipped skip passed pass collected selected
build building builds reactor order summary total time elapsed wall clock minutes seconds min sec s ms
compiling compile compiled compiler compilation sources source resources resource files file classes
java scala python spark maven junit pytest tests test suites suite plugin plugins module project
executing execute execution phase goal goals install installing installed package packages packaging
dependency dependencies resolution resolving resolved download downloading downloaded transfer transferred
copy copying clean cleaned scanning checking processing generating generated writing reading validating
progress done up-to-date changes detected recompiling nothing all some one more missing invalid required
main warning deprecated feature found cannot could unable find symbol class method type value exception
caused reason expected actual return returned rc exit code timed out timeout signal killed terminated
disk space left memory heap outofmemoryerror permission denied access network connection refused
reset temporary retry retries unavailable unknown invalid argument fatal traceback most recent call last
collect collected deselected xfailed xpassed test_simple_range range_test.py assertion assertionerror
testsuite report reports log logs diagnostics output stdout stderr head tail lines bytes blocks
prerequisites checkout submodules revision jar build.sh pytest git fetch sudo-check gpu-check
initializing databricks dbr nvidia rapids cudf cuda gpu cpu driver distribution shim only
scala-maven-plugin maven-compiler-plugin maven-surefire-plugin scala2.13 spark400db173
reactor-summary scanning projects effective model failed-to-execute verify validate lifecycle
disk filesystem size used avail available use mounted free total shared buff/cache swap
linux collected items item skipped passed failed selected warnings generated error failure
database server refused unable socket node start stop stage retry reason dependency tree
reading package lists state information dependency tree unpacking setting up old new
apache accelerator root sql udf optimizer worker memory mib gib stdout.log stderr.log diagnostics.log
exit status compilation failed exception device operation permitted readonly read-only system directory
load locate verify certificate no matching single-case classnotfoundexception sparkexception
noclassdeffounderror unsatisfiedlinkerror filenotfounderror modulenotfounderror importerror permissionerror
cuda_error_out_of_memory cudaerrormemoryallocation assert sudo errno pinned submodules distribution
line lines files testcompile cannot resolve dependencies non-resolvable parent pom transfer artifact
space left device directory such certificate verification initialization matched matching single-case
root cause trace suppressed failed attempt aborted stack task lost scheduler executor jvm killed
fail ran ok unittest
""".split())
_PUBLIC = set("""
java.lang.OutOfMemoryError java.lang.IllegalArgumentException java.lang.NullPointerException
java.lang.ClassNotFoundException java.lang.NoClassDefFoundError java.lang.UnsatisfiedLinkError
java.lang.RuntimeException java.io.IOException java.io.FileNotFoundException
java.lang.reflect.InvocationTargetException java.util.concurrent.ExecutionException
org.apache.spark.SparkException org.apache.spark.sql.AnalysisException
org.apache.spark.sql.catalyst.errors.package$TreeNodeException java.lang.Thread.run Thread.java
org.apache.spark.SparkContext.runJob SparkContext.scala org.apache.spark.scheduler.DAGScheduler.runJob
org.apache.spark.scheduler.DAGScheduler.abortStage DAGScheduler.scala
org.apache.spark.scheduler.ResultTask.runTask ResultTask.scala org.apache.spark.scheduler.Task.run Task.scala
org.apache.spark.executor.Executor$TaskRunner.run Executor.scala
java.util.concurrent.ThreadPoolExecutor.runWorker java.util.concurrent.ThreadPoolExecutor$Worker.run ThreadPoolExecutor.java
org.apache.maven.lifecycle.LifecycleExecutionException org.apache.maven.plugin.MojoFailureException
org.apache.maven.plugins net.alchim31.maven com.nvidia com.nvidia.spark ai.rapids
pom.xml scala2.13/pom.xml jenkins/databricks/build.sh jenkins/databricks/setup.sh
jenkins/databricks/common_vars.sh integration_tests/run_pyspark_from_build.sh
integration_tests/src/main/python/range_test.py
test_console_probe.py ConsoleProbe.test_expected_failure test_console_probe.ConsoleProbe.test_expected_failure
test_expected_failure self.assertEqual
""".split())
_MODULE = re.compile(r"cudf-spark(?:-(?:parent|aggregator|tests|sql|udf|shuffle|integration-tests|"
                     r"shim-deps-parent|sql-plugin-(?:api|format|fileio|columnar)))?_2\.(?:12|13)")
_NUMBER = re.compile(r"(?:\d{1,8}|\d{1,5}(?:\.\d{1,4}){1,2})(?:%|ms|s|m|h|[kmgt]i?b|-snapshot)?", re.I)
_TEST_COUNT = re.compile(r"(?:failures|errors|skipped|tests|passed|warnings)=[0-9]{1,6}", re.I)


def _public(raw):
    if len(raw) > 4096:
        return _OMITTED
    raw = _SGR.sub(b"", raw.removesuffix(b"\r")).replace(b"\t", b" ")
    if not raw.isascii() or any(c < 32 or c == 127 for c in raw):
        return None
    line = raw.decode("ascii").strip()
    if line == "sudo: a password is required":
        return line  # A fixed sudo failure, not a password value.
    if not line or line.startswith(("+", "$ ", ">>>", "::")) or _PRIVATE.search(line):
        return None

    def replace(match):
        raw_token = match[0]
        token = raw_token.rstrip(".") if raw_token[-1] == "." and raw_token[0] != "." else raw_token
        suffix = raw_token[len(token):][:3]
        relative = token.removeprefix("/home/ubuntu/spark-rapids/").removeprefix("./")
        if relative.endswith("/test_console_probe.py"):
            relative = "test_console_probe.py"
        if relative in _PUBLIC:
            return relative + suffix
        if (token.lower() in _WORDS or token in {"=", "=="}
                or _NUMBER.fullmatch(token) or _MODULE.fullmatch(token) or _TEST_COUNT.fullmatch(token)):
            return token + suffix
        if len(token) > 1 and set(token) in ({"-"}, {"="}, {"."}):
            return token[:72]
        return "[redacted]"

    # No terminal escapes, GHA workflow commands, or uninspected tail cut out of an identifier.
    line = re.sub(r":{2,}", lambda m: " ".join(m[0]), _TOKEN.sub(replace, line)).replace("##[", "# #[")
    line = re.sub(r"(?:\[redacted\][ ,;]*){2,}", "[redacted] ", line).rstrip()
    return line[:500] + _OMITTED if len(line) > 500 else line


def _window(stream, start, end, finish_line):
    # Inspect the previous byte to avoid publishing a suffix of a private or oversized line.
    stream.seek(max(0, start - 1))
    raw = stream.read(end - start + int(start > 0))
    if start:
        raw = raw.partition(b"\n")[2]
    if not finish_line:
        raw = raw.rpartition(b"\n")[0]
    return raw.split(b"\n")


def _take(lines, count, budget, *, tail):
    kept = []
    size = 2  # JSON array brackets; cost below includes the separator.
    for raw in reversed(lines) if tail else lines:
        line = _public(raw)
        if line is None:
            continue
        cost = len(json.dumps(line)) + 2
        if len(kept) == count or size + cost > budget:
            return (kept[::-1] if tail else kept), True
        kept.append(line)
        size += cost
    return (kept[::-1] if tail else kept), False


def _excerpt(directory_fd, filename, finished, ok):
    try:
        fd = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    except FileNotFoundError:
        return []
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
            raise ValueError("Expected a regular private log file")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            end = st.st_size
            if not finished or ok or filename != "diagnostics.log":
                lines = _window(stream, max(0, end - _WINDOW), end, finished)
                count = 2 if not finished else (1000 if ok else 10)
                budget = _BUDGET - (len(_OMITTED) + 4 if ok and finished else 0)
                tail, clipped = _take(lines, count, budget, tail=True)
                if ok and finished and len(tail) < count and (clipped or end > _WINDOW):
                    tail.insert(0, _OMITTED)
                return tail

            if end <= 2 * _WINDOW:
                first = last = _window(stream, 0, end, True)
                entire, overflow = _take(first, 1000, _BUDGET, tail=False)
                if not overflow:
                    return entire
            else:
                first = _window(stream, 0, _WINDOW, False)
                last = _window(stream, end - _WINDOW, end, True)
            half = (_BUDGET - len(json.dumps(_OMITTED)) - 8) // 2
            return (_take(first, 499, half, tail=False)[0] + [_OMITTED]
                    + _take(last, 500, half, tail=True)[0])
    finally:
        os.close(fd)


def snapshot(directory, finished=False, ok=False):
    """Return stdout, stderr and diagnostics lists; no reads outside these three files."""
    names = ("stdout", "stderr", "diagnostics")
    try:
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return {name: [] for name in names}
    try:
        return {name: _excerpt(fd, name + ".log", finished, ok) for name in names}
    finally:
        os.close(fd)
