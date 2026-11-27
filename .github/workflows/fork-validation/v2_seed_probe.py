# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Fresh-driver check; executed before the product build on the disposable fork cell."""

from contextlib import redirect_stdout
import hashlib
import io
from pathlib import Path
import subprocess
import tarfile
from tempfile import TemporaryDirectory
from urllib.request import urlopen

ARCHIVE = Path("/dbfs/cached_jars/cudf-spark-ci/fork-validation/maven-releases-v1.tar")
PLUGIN = "org.apache.maven.plugins:maven-clean-plugin:3.2.0:clean"
PLUGIN_JAR = "org/apache/maven/plugins/maven-clean-plugin/3.2.0/maven-clean-plugin-3.2.0.jar"


def maven(tmp):
    # Match build.sh on a stock driver. This bootstrap is only the CLI binary, not a plugin or dependency.
    name = "apache-maven-3.8.1-bin.tar.gz"
    cached = Path("/dbfs/cached_jars") / name
    url = "https://maven-central.storage-download.googleapis.com/maven2/org/apache/maven/apache-maven/3.8.1/" + name
    with (cached.open("rb") if cached.is_file() else urlopen(url, timeout=45)) as src:
        data = src.read(9536839)
    if len(data) != 9536838 or hashlib.sha512(data).hexdigest() != (
            "0ec48eb515d93f8515d4abe465570dfded6fa13a3ceb9aab8031428442d9912e"
            "c20f066b2afbf56964ffe1ceb56f80321b50db73cf77a0e2445ad0211fb8e38d"):
        raise ValueError("Unexpected Maven CLI bootstrap")
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        tar.extractall(tmp, filter="data")
    return str(tmp / "apache-maven-3.8.1/bin/mvn")


def prove(restore, digest, settings, archive=ARCHIVE, installed=None, executable=None):
    if ((installed or Path.home() / ".m2/repository") / PLUGIN_JAR).exists():
        raise ValueError("Driver already has the probe plugin; not a fresh seed validation")
    with TemporaryDirectory(prefix="fork-release-proof-") as tmp:
        root = Path(tmp)
        mvn = executable or maven(root)
        (root / "settings.xml").write_text(settings)
        (root / "pom.xml").write_text("<project><modelVersion>4.0.0</modelVersion><groupId>ci</groupId>"
                                      "<artifactId>seed-check</artifactId><version>1</version></project>")
        env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": str(root), "LANG": "C.UTF-8"}

        def clean(repo):
            return subprocess.run([mvn, "-B", "-o", "-ntp", "-s", "settings.xml", f"-Dmaven.repo.local={repo}", PLUGIN],
                                  cwd=root, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                  stderr=subprocess.DEVNULL, timeout=120).returncode

        cold, warm = root / "empty", root / "from-seed"
        if clean(cold) == 0:
            raise ValueError("Empty repository unexpectedly resolved the offline plugin")
        with redirect_stdout(io.StringIO()):
            if restore(warm, archive, digest) < 1:
                raise ValueError("New seed restored no released artifacts")
        if (any((warm / p).exists() for p in ("com/nvidia", "ai/rapids", "com/databricks"))
                or any("SNAPSHOT" in str(p.relative_to(warm)).upper() for p in warm.rglob("*"))):
            raise ValueError("Nonpublic or mutable coordinates present in seed")
        sentinel = root / "target/fork-clean-marker"
        sentinel.parent.mkdir()
        sentinel.touch()
        if clean(warm) != 0 or sentinel.exists():
            raise ValueError("New seed did not execute the offline clean plugin")
        artifact = hashlib.sha256((warm / PLUGIN_JAR).read_bytes()).hexdigest()
    return {"ok": True, "plugin": PLUGIN, "cold": "miss", "seeded": "offline-clean",
            "private": "absent", "artifact": artifact}
