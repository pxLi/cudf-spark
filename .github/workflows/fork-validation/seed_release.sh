#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

# Never use a product POM or the runner's existing Maven repository for this small pilot.
root=$(cd "$(dirname "$0")/../../.." && pwd)
scratch=$(mktemp -d)
scratch=$(cd "$scratch" && pwd -P)
trap 'rm -rf "$scratch"' EXIT
printf '%s\n' '<project><modelVersion>4.0.0</modelVersion><groupId>ci</groupId><artifactId>seed-check</artifactId><version>1</version></project>' > "$scratch/pom.xml"
maven=mvn
if [[ ${1:-} != --offline-check ]]; then
    # Use the same CLI as the fresh-driver proof, not the hosted image's changing default.
    maven=$(env -u DATABRICKS_TOKEN -u DATABRICKS_HOST python3 -IB -c \
        'import pathlib, runpy, sys; print(runpy.run_path(sys.argv[1])["maven"](pathlib.Path(sys.argv[2])))' \
        "$root/.github/workflows/fork-validation/v2_seed_probe.py" "$scratch")
fi
env -u DATABRICKS_TOKEN -u DATABRICKS_HOST "$maven" -B -ntp -s "$root/jenkins/databricks/maven-settings.xml" \
    -f "$scratch/pom.xml" -Dmaven.repo.local="$scratch/repository" org.apache.maven.plugins:maven-clean-plugin:3.2.0:clean
if [[ ${1:-} == --offline-check ]]; then
    helper="$root/jenkins/databricks/maven_seed_v2.py"
    python3 -IB "$helper" pack --archive "$scratch/seed.tar" --repository "$scratch/repository"
    sha=$(shasum -a 256 "$scratch/seed.tar")
    python3 -IB "$helper" restore --archive "$scratch/seed.tar" --sha256 "${sha%% *}" --repository "$scratch/fresh"
    mvn -o -B -ntp -s "$root/jenkins/databricks/maven-settings.xml" \
        -f "$scratch/pom.xml" -Dmaven.repo.local="$scratch/fresh" org.apache.maven.plugins:maven-clean-plugin:3.2.0:clean
    exit 0
fi
python3 -IB "$root/.github/workflows/databricks-ci/publish_maven_seed.py" --single \
    --directory /cached_jars/cudf-spark-ci/fork-validation --receipt "$scratch/sha256" --repository "$scratch/repository"
