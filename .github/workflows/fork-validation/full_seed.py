import hashlib
from pathlib import Path
import sys
import tarfile
import tempfile

here = Path(__file__).resolve().parent
sys.path[:0] = [str(here), str(here.parents[2] / "jenkins/databricks")]
from maven_seed_v2 import pack, restore
from v2_seed_probe import prove

with tempfile.TemporaryDirectory() as temp:
    archive = Path(temp) / "seed.tar"
    count = pack(Path.home() / ".m2/repository", archive)
    with tarfile.open(archive) as tar:
        jars = [m.name for m in tar if m.name.endswith(".jar")]
    scala = [p for p in jars if p.startswith(("org/scala-lang/", "org/scala-sbt/"))]
    print(f"Packed {count} dependency files, including {len(jars)} JARs, {len(scala)} Scala toolchain JARs; {archive.stat().st_size} bytes")
    if not scala:
        raise ValueError("No Scala artifacts from the restored Maven repository")
    with archive.open("rb") as stream:
        sha = hashlib.file_digest(stream, "sha256").hexdigest()
    # The producer's ~/.m2 is intentionally warm; emulate a different driver account on this hosted runner.
    consumer = Path(temp) / "fresh-driver-repository"
    consumer.mkdir()
    result = prove(restore, sha, (here.parents[2] / "jenkins/databricks/maven-settings.xml").read_text(),
                   archive=archive, installed=consumer)
    assert result["cold"] == "miss" and result["seeded"] == "offline-clean" and result["private"] == "absent"
    print("Maven 3.8.1: empty repository missed; full-size dependency seed passed offline")
