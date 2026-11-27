"""Use the real result gate against the fork run's GitHub Actions API."""
import json
import os
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "gpu-ci"))
from job_receipts import require_lanes


def api(path, timeout):
    return json.loads(subprocess.check_output(
        ["gh", "api", "repos/pxLi/cudf-spark/" + path], timeout=timeout))


rows = json.loads(os.environ["MATRIX"])["include"]
require_lanes(int(os.environ["GITHUB_RUN_ID"]), int(os.environ["GITHUB_RUN_ATTEMPT"]),
              rows, api=api, selection_attempt=int(os.environ["SELECTION_ATTEMPT"]))
print(f"Confirmed {len(rows)} simulated lanes from the jobs API, including the cleanup step.")
