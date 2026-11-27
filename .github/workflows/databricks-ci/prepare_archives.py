# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check or warm digest-addressed DBFS distributions without allocating a driver."""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit
from uuid import uuid4

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))  # Trusted checkout under python -I.
from workload import CLOUD, HOSTNAME, profile_config


def read_catalog():
    catalog = json.loads((HERE / "archives.json").read_text())
    if not isinstance(catalog, dict) or not catalog:
        raise ValueError("Expected a nonempty Databricks archive catalog")
    for name, entry in catalog.items():
        if (not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9.-]*", name)
                or not re.fullmatch(r"[0-9a-f]{128}", entry["sha512"])
                or type(entry["size"]) is not int or not 0 < entry["size"] < 2**31
                or urlsplit(entry["url"]).scheme != "https"
                or entry["url"].rsplit("/", 1)[-1] != name):
            raise ValueError("Invalid Databricks archive catalog entry")
    return catalog


def matches(local, entry):
    digest = hashlib.sha512()
    with local.open("rb") as data:
        for chunk in iter(lambda: data.read(1024 * 1024), b""):
            digest.update(chunk)
    return local.stat().st_size == entry["size"] and digest.hexdigest() == entry["sha512"]


def fetch_verified(local, entry):
    # The source GET has no workspace credentials. Keep bytes on disk for streaming upload.
    subprocess.run(["curl", "-q", "--fail", "--silent", "--show-error", "--location",
                    "--proto", "=https", "--proto-redir", "=https", "--connect-timeout", "15",
                    "--max-time", "3600", "--max-filesize", str(entry["size"]),
                    "--output", str(local), entry["url"]], check=True)
    if not matches(local, entry):
        raise ValueError(f"{local.name}: downloaded size or SHA-512 differs from the catalog")


class Dbfs:
    def __init__(self, host, token):
        if not re.fullmatch(r"https://" + HOSTNAME, host) or not token or not token.isprintable():
            raise ValueError("Invalid protected Databricks workspace or credential")
        self.url = host + "/api/2.0/dbfs/"
        self.token = token

    def request(self, operation, *args, expected_error=None):
        # No redirects, credentials in argv, or automatic retries of writes.
        limit = "1800" if operation == "put" else "60"
        response_limit = "1500000" if operation == "read" else "65536"
        cmd = ["curl", "-q", "--silent", "--proto", "=https", "--connect-timeout", "15",
               "--max-time", limit, "--max-filesize", response_limit, "--header", "@-",
               "--write-out", "\n%{http_code}", *args, self.url + operation]
        result = subprocess.run(cmd, input=f"Authorization: Bearer {self.token}\n",
                                capture_output=True, text=True)
        if result.returncode:
            raise RuntimeError(f"DBFS {operation}: curl exit {result.returncode}")
        body, status = result.stdout.rsplit("\n", 1)
        if status == "200":
            payload = json.loads(body)
            if not isinstance(payload, dict):
                raise ValueError(f"DBFS {operation}: unexpected success response")
            return payload
        if expected_error and status in ("400", "404", "409"):
            try:
                if json.loads(body).get("error_code") == expected_error:
                    return None
            except (ValueError, AttributeError):
                pass
        raise RuntimeError(f"DBFS {operation}: HTTP {status}")

    def post(self, operation, data, expected_error=None):
        return self.request(operation, "--header", "Content-Type: application/json",
                            "--data-binary", json.dumps(data), expected_error=expected_error)

    def present(self, path, size, legacy=False):
        info = self.request("get-status", "--get", "--data-urlencode", f"path={path}",
                            expected_error="RESOURCE_DOES_NOT_EXIST")
        if info is None:
            return False
        if (info.get("path") != path or type(info.get("is_dir")) is not bool
                or type(info.get("file_size")) is not int or info["file_size"] < 0):
            raise ValueError("Unexpected DBFS file status")
        if info["is_dir"] or info["file_size"] != size:
            if legacy:
                return False
            raise ValueError(f"Conflicting DBFS object at {path}; expected a {size}-byte file")
        return True

    def copy_legacy(self, path, candidate, entry):
        # DBFS read accepts at most 1 MiB. The old flat namespace has no digest in its name.
        with candidate.open("wb") as output:
            offset = 0
            while offset < entry["size"]:
                length = min(1 << 20, entry["size"] - offset)
                block = self.request("read", "--get", "--data-urlencode", f"path={path}",
                                     "--data", f"offset={offset}", "--data", f"length={length}",
                                     expected_error="RESOURCE_DOES_NOT_EXIST")
                if block is None:
                    return False
                data = base64.b64decode(block["data"], validate=True)
                if type(block["bytes_read"]) is not int or block["bytes_read"] != len(data) or not 0 < len(data) <= length:
                    return False
                output.write(data)
                offset += len(data)
        return matches(candidate, entry)


def prepare(dbfs, name, entry, ensure, scratch):
    directory = f'/cached_jars/cudf-spark-ci/{entry["sha512"]}'
    final = f"{directory}/{name}"
    if dbfs.present(final, entry["size"]):
        print(f"{name}: present ({entry['size']} bytes)")
        return
    local = scratch / name
    old = f"/cached_jars/{name}"
    legacy = not local.exists() and dbfs.present(old, entry["size"], legacy=True)
    print(f"{name}: missing" + ("; found legacy size match" if legacy else ""), flush=True)
    if not ensure:
        return
    if not local.exists():  # Reuse only bytes verified in this invocation's private tempdir.
        candidate = local.with_suffix(".part")
        if legacy and dbfs.copy_legacy(old, candidate, entry):
            print(f"{name}: verified existing legacy file")
        else:
            fetch_verified(candidate, entry)
        candidate.replace(local)
    stage = f"{directory}/.upload-{uuid4().hex}"
    dbfs.post("mkdirs", {"path": directory})
    # A collision is foreign. Every other outcome can only leave this unique staging file.
    uploaded = False
    try:
        result = dbfs.request("put", "--form-string", f"path={stage}", "--form-string", "overwrite=false",
                              "--form", f"contents=@{local}", expected_error="RESOURCE_ALREADY_EXISTS")
        if result is None:
            stage = None
            raise RuntimeError(f"Staging path collision for {name}")
        if not dbfs.present(stage, entry["size"]):
            raise RuntimeError(f"DBFS did not retain staged {name}")
        moved = dbfs.post("move", {"source_path": stage, "destination_path": final},
                          expected_error="RESOURCE_ALREADY_EXISTS")
        uploaded = moved is not None
        if not dbfs.present(final, entry["size"]):
            raise RuntimeError(f"DBFS did not publish {name}")
    finally:
        if stage and not uploaded:
            try:
                dbfs.post("delete", {"path": stage, "recursive": False}, expected_error="RESOURCE_DOES_NOT_EXIST")
            except (OSError, ValueError, RuntimeError) as exc:
                print(f"Warning: could not clean our staging file {stage}: {exc}", file=sys.stderr)
    print(f"{name}: confirmed ({entry['size']} bytes)")


def main():
    catalog = read_catalog()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("plan", "ensure"))
    profiles = parser.add_mutually_exclusive_group()
    profiles.add_argument("--profile", choices=CLOUD)
    profiles.add_argument("--all-profiles", action="store_true")
    parser.add_argument("--archive", action="append", choices=catalog)
    args = parser.parse_args()
    failures = []
    with tempfile.TemporaryDirectory() as scratch:
        for profile in CLOUD if args.all_profiles else [args.profile]:
            try:
                host = profile_config(profile)["host"] if profile else os.environ["DATABRICKS_HOST"]
                token = f"DATABRICKS_{profile.upper()}_TOKEN" if args.all_profiles else "DATABRICKS_TOKEN"
                dbfs = Dbfs(host.rstrip("/"), os.environ[token])
            except (KeyError, ValueError) as exc:
                failures.append(f"{profile or 'workspace'} configuration: {exc}")
                continue
            for name in args.archive or catalog:
                print(f"Checking {profile or 'workspace'}: {name}", flush=True)
                try:
                    prepare(dbfs, name, catalog[name], args.mode == "ensure", Path(scratch))
                except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as exc:
                    failures.append(f"{profile or 'workspace'} / {name}: {exc}")
    if failures:
        raise RuntimeError("\n".join(failures))


if __name__ == "__main__":
    try:
        main()
    except (KeyError, OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        sys.exit(f"Archive preparation failed: {error}")
