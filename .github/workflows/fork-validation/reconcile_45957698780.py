# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read-only fork investigation. No prior provider ID survived the failed job."""

import hashlib
import http.client
import json
import os
import re
import sys
from urllib.parse import urlencode


TAG = "fork-gha-45957698780-1-17.3-CI_PART1-aws2-1"
TAGS = {"CI_MANAGED_BY": "cudf-spark-databricks-ci-fork", "CI_REPOSITORY": "pxLi/cudf-spark",
        "CI_WORKFLOW": ".github/workflows/fork-v2-validation.yml", "CI_PROFILE": "aws2", "CI_BUILD_TAG": TAG}
CLUSTERS = "/api/2.1/clusters/"
READS = {"me": ("GET", "/api/2.0/preview/scim/v2/Me"), "list": ("GET", CLUSTERS + "list"),
         "get": ("GET", CLUSTERS + "get"), "events": ("POST", CLUSTERS + "events")}


class Inconclusive(Exception):
    pass


def read(host, token, operation, **fields):
    method, path = READS[operation]  # Databricks only exposes this read-only event history as POST.
    conn = http.client.HTTPSConnection(host, timeout=20)  # No redirects or ambient credentials.
    try:
        conn.request(method, path + ("?" + urlencode(fields) if method == "GET" else ""),
                     body=json.dumps(fields) if method == "POST" else None,
                     headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        response = conn.getresponse()
        raw = response.read(4 * 1024 * 1024 + 1)
        if len(raw) > 4 * 1024 * 1024:
            raise Inconclusive(f"{operation}: oversized response")
    finally:
        conn.close()
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError()
    except ValueError:
        raise Inconclusive(f"{operation}: HTTP {response.status}; no structured response") from None
    if response.status != 200:
        code = data.get("error_code")
        code = code if isinstance(code, str) and re.fullmatch(r"[A-Z_]{1,64}", code) else "unknown"
        message = data.get("message")
        cid = fields.get("cluster_id")
        # Never emit provider bodies; report an exact known template only if it contains this exact ID.
        shape = "unrecognized/withheld"
        if cid and isinstance(message, str):
            for template in ("Cluster {} does not exist", "Cluster {} does not exist.",
                             "Cluster '{}' does not exist", "Cluster with id {} does not exist"):
                if message == template.format(cid):
                    shape = template.format("<target-id>")
                    break
        raise Inconclusive(f"{operation}: HTTP {response.status} {code}; message={shape}")
    return data


def inspect(api):
    creator = api("me", attributes="userName").get("userName")
    if not isinstance(creator, str) or not creator.strip():
        raise Inconclusive("Sandbox principal not confirmed")
    matches, seen = {}, set()
    params = {"page_size": 100}  # No state filter: includes recently terminated clusters.
    for page_number in range(1, 501):
        page = api("list", **params)
        rows = page.get("clusters")
        if not isinstance(rows, list):
            raise Inconclusive("Incomplete inventory response")
        for row in rows:
            if not isinstance(row, dict):
                raise Inconclusive("Invalid inventory row")
            name, tags = row.get("cluster_name"), row.get("custom_tags", {})
            if not isinstance(name, str) or not isinstance(tags, dict):
                raise Inconclusive("Inventory row lacks usable identity")
            if name == TAG or tags.get("CI_BUILD_TAG") == TAG:
                cid = row.get("cluster_id")
                if not isinstance(cid, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", cid):
                    raise Inconclusive("Matching inventory row has no usable ID")
                matches[cid] = None
        token = page.get("next_page_token")
        if token is None or token == "":
            break
        if not isinstance(token, str) or token in seen:
            raise Inconclusive("Invalid or repeated inventory page token")
        seen.add(token)
        params["page_token"] = token
    else:
        raise Inconclusive("Inventory did not reach its last page")
    print(f"Completed {page_number} inventory page(s); exact name or build-tag candidates: {len(matches)}", flush=True)
    if len(matches) != 1:
        raise Inconclusive("No unique visible candidate; no historical cluster ID or independent audit receipt. "
                           "Cannot query ID-scoped events or confirm the prior purge.")
    cid, = matches
    row = api("get", cluster_id=cid)
    tags = row.get("custom_tags")
    if (row.get("cluster_id") != cid or row.get("cluster_name") != TAG or row.get("creator_user_name") != creator
            or not isinstance(tags, dict) or any(tags.get(k) != v for k, v in TAGS.items())):
        raise Inconclusive("Matching name/tag but full ownership did not match")
    state = row.get("state")
    if not isinstance(state, str) or not re.fullmatch(r"[A-Z_]{1,32}", state):
        raise Inconclusive("Invalid state for the matching cluster")
    print(f"Matching owned object currently exists: state={state}, id_sha256={hashlib.sha256(cid.encode()).hexdigest()}",
          flush=True)
    # A bounded historical cross-check, not a search for unrelated IDs or a substitute for a purge receipt.
    history = api("events", cluster_id=cid, start_time=1795764000000, end_time=1795765800000,
                  event_types=["CREATING", "RUNNING", "TERMINATING"], order="ASC", page_size=50)
    events = history.get("events", [])
    if not isinstance(events, list) or any(not isinstance(e, dict) or e.get("cluster_id") != cid for e in events):
        raise Inconclusive("Cluster event identity missing or mismatched")
    print("Historical lifecycle events on first page: " +
          ", ".join(k for k in ("CREATING", "RUNNING", "TERMINATING") if any(e.get("type") == k for e in events)))
    raise Inconclusive("The owned cluster is visible; it was not confirmed absent. No action taken.")


if __name__ == "__main__":
    host = re.fullmatch(r"https://([a-z0-9.-]+\.cloud\.databricks\.com)/?", os.environ["DATABRICKS_HOST"])
    token = os.environ["DATABRICKS_TOKEN"]
    if not host or not token or not token.isascii() or not token.isprintable():
        sys.exit("Invalid protected AWS2 workspace configuration")
    try:
        inspect(lambda op, **params: read(host[1], token, op, **params))
    except Inconclusive as error:
        sys.exit(f"Historical purge not confirmed: {error}")
    except (OSError, http.client.HTTPException) as error:
        sys.exit(f"Incomplete workspace read: {type(error).__name__}")
