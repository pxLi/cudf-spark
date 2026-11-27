# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise workspace cache hits, verified uploads, conflicts and failures without a service."""

import base64
import hashlib
import json
from pathlib import Path
from subprocess import CompletedProcess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import prepare_archives as warmer


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.payload = b"fixture archive"
        self.name = "example.tgz"
        self.entry = dict(url="https://example.invalid/example.tgz", size=len(self.payload),
                          sha512=hashlib.sha512(self.payload).hexdigest())
        self.final = f"/cached_jars/cudf-spark-ci/{self.entry['sha512']}/{self.name}"
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.files = {}
        self.calls = []
        self.download = self.payload
        self.concurrent = None
        self.api_error = {}
        self.dbfs = warmer.Dbfs("https://example.invalid", "workspace-token")
        curl = patch.object(warmer.subprocess, "run", side_effect=self.curl)
        curl.start()
        self.addCleanup(curl.stop)

    def curl(self, cmd, **kwargs):
        if "--output" in cmd:
            self.calls.append("download")
            self.assertNotIn("input", kwargs)
            Path(cmd[cmd.index("--output") + 1]).write_bytes(self.download)
            return
        op = cmd[-1].rsplit("/", 1)[-1]
        self.calls.append(op)
        self.assertEqual(kwargs["input"], "Authorization: Bearer workspace-token\n")
        self.assertNotIn("--location", cmd)
        self.assertNotIn("workspace-token", " ".join(cmd))
        status, result = self.api_error.get(op, (200, {}))
        if op not in self.api_error:
            if op == "get-status":
                path = cmd[cmd.index("--data-urlencode") + 1][5:]
                if path in self.files:
                    result = {"path": path, "file_size": len(self.files[path]), "is_dir": False}
                else:
                    status, result = 404, {"error_code": "RESOURCE_DOES_NOT_EXIST"}
            elif op == "read":
                path = cmd[cmd.index("--data-urlencode") + 1][5:]
                start = int(next(x[7:] for x in cmd if x.startswith("offset=")))
                length = int(next(x[7:] for x in cmd if x.startswith("length=")))
                self.assertLessEqual(length, 1 << 20)
                data = self.files[path][start:start + length]
                result = {"data": base64.b64encode(data).decode(), "bytes_read": len(data)}
            elif op == "put":
                self.assertIn("overwrite=false", cmd)
                stage = cmd[cmd.index("--form-string") + 1][5:]
                self.files[stage] = Path(cmd[cmd.index("--form") + 1].removeprefix("contents=@")).read_bytes()
            elif op == "move":
                args = json.loads(cmd[cmd.index("--data-binary") + 1])
                if self.concurrent is not None:
                    self.files[self.final] = self.concurrent
                    status, result = 409, {"error_code": "RESOURCE_ALREADY_EXISTS"}
                else:
                    self.files[args["destination_path"]] = self.files.pop(args["source_path"])
            elif op == "delete":
                args = json.loads(cmd[cmd.index("--data-binary") + 1])
                self.assertFalse(args["recursive"])
                self.assertNotEqual(args["path"], self.final)
                self.files.pop(args["path"], None)
        return CompletedProcess(cmd, 0, json.dumps(result) + "\n" + str(status))

    def prepare(self, ensure=True):
        warmer.prepare(self.dbfs, self.name, self.entry, ensure, Path(self.tmp.name))

    def test_cold_then_warm_across_workspaces(self):
        self.prepare(False)
        self.assertEqual(self.calls, ["get-status", "get-status"])
        self.prepare()
        self.assertEqual(self.files, {self.final: self.payload})
        self.assertEqual(self.calls.count("download"), 1)
        self.calls.clear()
        self.prepare()
        self.assertEqual(self.calls, ["get-status"])
        self.files.clear()  # Same invocation, second workspace misses.
        self.prepare()
        self.assertNotIn("download", self.calls)
        self.assertEqual(self.files, {self.final: self.payload})

    def test_invalid_download_cannot_publish_or_poison_the_next_workspace(self):
        self.download = b"x" * len(self.payload)
        with self.assertRaisesRegex(ValueError, "SHA-512"):
            self.prepare()
        self.assertEqual(self.calls, ["get-status", "get-status", "download"])
        self.download = self.payload
        self.prepare()
        self.assertEqual(self.files, {self.final: self.payload})

    def test_verified_legacy_archive_avoids_origin_download(self):
        self.payload = b"A" * ((1 << 20) + 1)
        self.entry.update(size=len(self.payload), sha512=hashlib.sha512(self.payload).hexdigest())
        self.final = f"/cached_jars/cudf-spark-ci/{self.entry['sha512']}/{self.name}"
        old = f"/cached_jars/{self.name}"
        self.files[old] = self.payload
        self.prepare(False)
        self.assertEqual(self.calls, ["get-status", "get-status"])
        self.prepare()
        self.assertEqual(self.calls.count("read"), 2)
        self.assertNotIn("download", self.calls)
        self.assertEqual(self.files, {old: self.payload, self.final: self.payload})

    def test_invalid_legacy_file_is_preserved(self):
        old = f"/cached_jars/{self.name}"
        for foreign in (b"short", b"x" * len(self.payload)):
            with self.subTest(size=len(foreign)):
                self.files = {old: foreign}
                self.calls.clear()
                cached = Path(self.tmp.name) / self.name
                cached.unlink(missing_ok=True)
                self.prepare()
                self.assertEqual(self.calls.count("download"), 1)
                self.assertEqual(self.calls.count("read"), int(len(foreign) == len(self.payload)))
                self.assertEqual(self.files, {old: foreign, self.final: self.payload})

    def test_conflict_is_never_replaced(self):
        self.files[self.final] = b"foreign"
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            self.prepare()
        self.assertEqual(self.calls, ["get-status"])
        self.assertEqual(self.files[self.final], b"foreign")

    def test_concurrent_move_rechecks_winner_and_removes_only_our_stage(self):
        self.concurrent = self.payload
        self.prepare()
        self.assertEqual(self.files, {self.final: self.payload})
        self.files.clear()
        self.concurrent = b"conflict"
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            self.prepare()
        self.assertEqual(self.files, {self.final: b"conflict"})

    def test_unknown_response_is_not_a_cache_miss(self):
        for status, value in [(302, {}), (403, {"error_code": "PERMISSION_DENIED"}),
                              (404, {}), (200, None), (200, {"path": self.final}),
                              (200, {"path": "/other", "is_dir": False, "file_size": len(self.payload)})]:
            with self.subTest(status=status, value=value):
                self.api_error["get-status"] = (status, value)
                self.calls.clear()
                with self.assertRaises((RuntimeError, ValueError)):
                    self.prepare()
                self.assertEqual(self.calls, ["get-status"])

    def test_staging_collision_leaves_foreign_file_alone(self):
        self.api_error["put"] = (409, {"error_code": "RESOURCE_ALREADY_EXISTS"})
        with self.assertRaisesRegex(RuntimeError, "collision"):
            self.prepare()
        self.assertNotIn("delete", self.calls)
        self.assertNotIn("move", self.calls)

    def test_all_profiles_use_the_matching_token_and_continue_after_a_missing_token(self):
        name = next(iter(warmer.read_catalog()))
        profiles = {p: {"host": f"https://{p}.invalid"} for p in warmer.CLOUD}
        env = {"DATABRICKS_PROFILES_JSON": json.dumps(profiles),
               "DATABRICKS_AWS2_TOKEN": "first", "DATABRICKS_AZURE3_TOKEN": "third"}
        with patch.dict(warmer.os.environ, env, clear=True), patch.object(warmer, "prepare") as prepare, \
                patch.object(warmer.sys, "argv", ["warm", "plan", "--all-profiles", "--archive", name]):
            with self.assertRaisesRegex(RuntimeError, "DATABRICKS_AWS_TOKEN"):
                warmer.main()
        self.assertEqual([(c.args[0].url, c.args[0].token, c.args[1], c.args[3])
                          for c in prepare.call_args_list],
                         [("https://aws2.invalid/api/2.0/dbfs/", "first", name, False),
                          ("https://azure3.invalid/api/2.0/dbfs/", "third", name, False)])
