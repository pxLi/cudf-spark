# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Stage a pinned distribution on a disposable Databricks driver, never a shared Maven repo."""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile


def copy_verified(source, target, size, digest):
    # Check the bytes we copy, not a shared pathname that another driver can replace.
    fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as src:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size != size:
            return False
        check = hashlib.sha512()
        remaining = size
        while remaining:
            chunk = src.read(min(remaining, 1024 * 1024))
            if not chunk:
                return False
            check.update(chunk)
            if target is not None:
                target.write(chunk)
            remaining -= len(chunk)
        return not src.read(1) and check.hexdigest() == digest


def ensure(name, entry, local_dir, dbfs_root=Path("/dbfs")):
    digest, size, url = entry["sha512"], entry["size"], entry["url"]
    if (not re.fullmatch(r"[\w.-]+\.(?:tar\.gz|tgz)", name) or not re.fullmatch(r"[a-f0-9]{128}", digest)
            or type(size) is not int or size <= 0 or not url.startswith("https://")):
        raise ValueError("Invalid pinned distribution metadata")
    local_dir = Path(local_dir)
    local_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if local_dir.resolve() != local_dir:
        raise ValueError("Refusing redirected driver cache")
    dest = local_dir / name
    shared = dbfs_root / "cached_jars" / "cudf-spark-ci" / digest / name
    try:
        if copy_verified(dest, None, size, digest):
            print(f"Verified driver-local {name}", flush=True)
            return dest
    except FileNotFoundError:
        pass

    with tempfile.TemporaryDirectory(dir=local_dir) as tmp:
        candidate = Path(tmp) / name
        # Existing Jenkins jobs use a flat cache. Read it only, never modify it.
        for source in (shared, dbfs_root / "cached_jars" / name):
            try:
                with candidate.open("wb") as out:
                    hit = copy_verified(source, out, size, digest)
            except OSError:
                continue
            if hit:
                if source == shared:
                    print(f"Verified DBFS cache hit: {name}", flush=True)
                    candidate.replace(dest)
                    return dest
                print(f"Verified legacy DBFS cache hit: {name}", flush=True)
                break
        else:
            print(f"Downloading pinned {name}: no verified DBFS hit", flush=True)
            # The Spark smoke distribution is >400 MB. Bound retries inside one transfer budget.
            seconds = 300 if size < 16 * 1024 * 1024 else 3600
            subprocess.run(["curl", "--fail", "--silent", "--show-error", "--location", "--proto", "=https",
                            "--proto-redir", "=https", "--connect-timeout", "15", "--max-time", str(seconds),
                            "--retry", "1", "--max-filesize", str(size), "--output", str(candidate), url],
                           check=True, timeout=seconds + 15)
            if not copy_verified(candidate, None, size, digest):
                raise ValueError(f"Downloaded {name} failed size or SHA-512 verification")
        try:
            if not dbfs_root.is_dir() or shared.parent.resolve() != shared.parent:
                raise OSError("DBFS not available")
            shared.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(dir=shared.parent) as staging:
                partial = Path(staging) / name
                shutil.copyfile(candidate, partial)
                partial.replace(shared)
            print(f"Populated DBFS cache: {name}", flush=True)
        except OSError:
            print(f"DBFS cache not writable for {name}; using verified local copy", flush=True)
        candidate.replace(dest)
    return dest


if __name__ == "__main__":
    ensure(sys.argv[1], json.loads(sys.argv[2]), sys.argv[3])
