### Description

**Draft: do not review yet. The latest test-only commit intentionally fails the reduced CI stages to validate diagnostic output. Trigger manually.**

An intermittent Spark shell smoke-test failure can report only executor disassociation, while the executor stderr contains no useful explanation. A lost JVM or pytest worker during other integration tests can similarly leave important evidence only in worker logs. This change prints bounded Spark diagnostics directly to the console, on the test host before cluster cleanup, without depending on a later debug-bundle upload.

- Enable INFO logging for the smoke query so the local Spark worker reports executor termination details.
- Print a focused master/worker lifecycle section with surrounding context, retaining executor state/exit messages and application-removal events even when shutdown stack traces fill the overall log tail.
- Capture and check all three smoke-test pipeline exit statuses. The expected query output no longer masks a nonzero Spark shell or `tee` exit.
- On an xdist worker loss, immediately print its ID, available Python PID, and a bounded worker-log excerpt from the surviving coordinator.
- Cache Spark application/process identity while the JVM is healthy. Include the test name and available JVM exit status when the existing dead-JVM handler terminates a pytest worker; failure diagnostics do not call the dead JVM.
- After a failed serial or parallel integration-test command, print bounded worker-log excerpts, focused master/worker lifecycle messages, available local executor stdout/stderr, and recent JVM fatal-error headers. Executor logs are limited to application IDs found in the current run's logs; remote-only executor logs are not collected by this change.
- Keep the additional collection best effort and time bounded, inline in the existing runner. No extra script, packaging change, host diagnostics, or log retention.
- Keep downloaded Databricks JUnit XML only in its run directories so recursive publishing counts each report once.

The permanent diagnostic changes preserve the smoke query, heap settings, retry policy, and original integration-test exit status. The permanent smoke execution timeout is unchanged. The temporary fault-injection commit below deliberately changes the workload and bounds the injected smoke commands. These logs provide evidence; they do not classify a real failure as an infrastructure or product fault.

### Temporary Validation Commits

Separate test-only commits enable the three standalone premerge stages (`mvn verify`, `Premerge CI 2`, and `CI scala 2.13`) plus both Databricks parts (`CI_PART1` and `CI_PART2`) for DBR 14.3 and 17.3. Skills stages remain skipped.

The previous success-path round at `707d5b53d8` completed 24 standalone and 32 Databricks sanity-test executions, plus both upstream smoke checks. The Databricks JUnit UI counted duplicate XML copies. The follow-up success-path round at `017a5f9b2f` (parent 15051, Databricks 5563/5564) passed and verified the fix: each Databricks child now reports 16 passes, not 32.

The latest test-only commit replaces the success-only selection with these deliberate failures:

| Stage | Injected failure | Expected console evidence |
| --- | --- | --- |
| Standalone `mvn verify` | Serial assertion failure with a healthy JVM | `FAILED integration tests`, failing test and worker-log excerpt; no worker-loss event |
| Standalone `Premerge CI 2` | Abrupt xdist Python worker exit `88` | `pytest worker lost`, worker identity, injection marker and bounded log excerpt |
| Standalone `CI scala 2.13` | `SIGSEGV` to the xdist worker's own driver JVM | JVM fatal-error header, cached Spark identity, dead-driver handling and worker-loss diagnostics |
| Databricks CI1, DBR 14.3 | Upstream smoke executor `Runtime.halt(86)` | Executor exit, master/application-removal lifecycle messages, available executor stderr |
| Databricks CI1, DBR 17.3 | Successful smoke query followed by driver `Runtime.halt(87)` | Expected result still appears, but `spark-shell exit=87` and `grep exit=0` must fail the smoke check |
| Databricks CI2, DBR 14.3 | Serial driver `Runtime.halt(87)` | Failed wrapper, recorded test/application identity and available worker log; no xdist event expected |
| Databricks CI2, DBR 17.3 | xdist driver `Runtime.halt(87)` | Dead-driver handling with cached identity/available JVM exit status, then worker-loss diagnostics |

Each pytest invocation explicitly selects `range_test.py::test_simple_range` plus one fault case. The fault module is deliberately named `diagnostic_faults.py`, outside normal pytest filename discovery. Parallel scenarios use two workers with `--max-worker-restart=0`; random OOM injection is disabled. The native crash targets only that worker's live JVM child and disables core dumps while retaining `hs_err` output. In the serial driver-loss case the JVM is externally launched, so a cached child-PID/exit status may be unavailable and abrupt termination may prevent JUnit XML generation.

Standalone stages still build only the selected shim with unit tests skipped: Spark 3.3.0 for Scala 2.12 and Spark 4.0.1 for Scala 2.13. JaCoCo publication remains skipped. Both Databricks parts build independently and retain readiness; CI1 also retains upstream readiness before its injected smoke failure. Full integration suites remain skipped.

Parent and Databricks matrix fail-fast are temporarily disabled so all scenarios can finish. Duplicate-build cancellation remains disabled for concurrent manual triggers. Existing fallback classification and cluster cleanup are unchanged; injected workload failures must not cause cross-CSP fallback.

**Expected outcome: failed stages/jobs with the corresponding injection markers and diagnostics, not a green build.** A setup failure or the test-only smoke timeout (`124`) is not proof that the intended fault was exercised. The injected smoke command is bounded at 180 seconds, with diagnostics outside that timeout. Remove all temporary validation changes, including this fault module and timeout, before merging.

Validation completed:
- `bash -n integration_tests/run_pyspark_from_build.sh`
- `shellcheck -S error integration_tests/run_pyspark_from_build.sh`
- ShellCheck at warning severity on the inline diagnostics block.
- Bash syntax checks on `jenkins/databricks/test.sh` and `jenkins/spark-premerge-build.sh`.
- All three changed shell scripts pass ShellCheck with the same exclusions as the GitHub workflow, including its informational-level checks.
- Python syntax checks of the diagnostic code, `jenkins/databricks/run-tests.py`, and the temporary fault module.
- Groovy conversion-phase syntax parsing of both premerge Jenkinsfiles (not live Jenkins validation).
- JDK 17 accepts the native-crash scenario's `-XX:-CreateCoredumpOnCrash` option.
- `git diff --check`
- `codex review --uncommitted`: no actionable defects found.
- Checked the lifecycle filter against an existing failed smoke-test log: executor loss, the Master's removal message, and subsequent shutdown events were retained. The cause of the executor failure remains unconfirmed.
- Shell dry runs: expected smoke output with producer exit `137` is rejected; a successful producer is accepted. The integration-test wrapper preserves success and exit `42` even when the diagnostics command is unavailable.

Validation status before requesting review:
- [x] Manually run all seven injected scenarios and verify console diagnostics, failure propagation, available reports, and cluster cleanup. Evidence and reporting limits are below.
- [ ] Remove the temporary validation commits and restore the original smoke query/timeout, full suites, JaCoCo publication, normal stage selection, fail-fast, and duplicate-build cancellation.

Performance benchmarking is not required: this changes test logging and failure diagnostics, not production execution.

### Fault-Injection Results (2026-10-16)

**All seven scenarios behaved as expected. No diagnostic fix was needed from this round.** Parent **15052** and Databricks children **5565 / CI_PART1** and **5566 / CI_PART2** ran PR commit `5602e2f9e7` as merged source `87e3659f47`, with shared library `f988a007c3`. Their failed status is intentional: every selected fault reached its injection point and produced the corresponding failure diagnostics. None was a setup failure or the injection timeout.

All four Databricks cells reached readiness, kept workload failures non-retryable, and recorded both `termination_confirmed=true` and `cleanup_complete=true`. No cross-CSP fallback occurred for these test failures.

Screenshots below are actual Blue Ocean pipeline-graph captures, with the relevant stage selected, taken on 2026-10-16. They are cropped to the graph; console excerpts are shown separately. Internal hosts and paths are redacted, timestamps and unrelated lines are omitted. Graph colors alone do not establish the cause; the console and available bundles were checked separately.

#### 1. Serial assertion failure, healthy JVM

Parent 15052, `mvn verify`: the ordinary assertion failed without a worker-loss event. The wrapper returned exit 1 and printed the failing test and worker log. The healthy control passed.

![Serial assertion failure: selected mvn verify stage](https://raw.githubusercontent.com/pxLi/cudf-spark/pr-16377-diagnostic-evidence/serial-assertion.png)

```text
Failed: TEST ONLY: expected assertion failure with a healthy JVM
TEST ONLY: injecting ordinary assertion failure; {'worker': 'gw0', 'host': '[REDACTED_HOST]', 'python_pid': 3240, 'jvm_pid': 'externally launched', 'application_id': 'local-1792128822901'}
FAILED integration tests (exit=1, run_dir=[RUN_DIR])
----- Spark process identity and master/worker lifecycle -----
```

#### 2. Abrupt pytest worker loss

Parent 15052, `Premerge CI 2`: the worker exited through `os._exit(88)`. The surviving coordinator printed the loss and its log; the overall test command returned exit 1. Although the coordinator header did not have the Python PID, the cached identity in the accompanying worker excerpt did.

![Abrupt pytest worker loss: selected Premerge CI 2 stage](https://raw.githubusercontent.com/pxLi/cudf-spark/pr-16377-diagnostic-evidence/pytest-worker-exit.png)

```text
----- pytest worker lost: gw1, python_pid=unknown, error=Not properly terminated -----
Spark process identity: {'worker': 'gw1', 'host': '[REDACTED_HOST]', 'python_pid': 3125, 'jvm_pid': 3210, 'application_id': 'local-1792128802458'}
TEST ONLY: injecting pytest worker os._exit(88); ...
FAILED integration tests (exit=1, run_dir=[RUN_DIR])
```

#### 3. Native driver-JVM crash

Parent 15052, `CI scala 2.13`: the injected SIGSEGV produced an `hs_err` file. The bounded fatal-error header, cached identity, unusable-driver message, and worker-loss message all appeared in the console. The test wrapper returned exit 1; no core dump was generated.

![Native JVM crash: selected CI scala 2.13 stage](https://raw.githubusercontent.com/pxLi/cudf-spark/pr-16377-diagnostic-evidence/native-jvm-crash.png)

```text
set_spark_job_timeout: Unusable Spark Driver JVM detected!
Spark process identity: {'worker': 'gw1', 'host': '[REDACTED_HOST]', 'python_pid': 3388, 'jvm_pid': 3478, 'application_id': 'local-1792128831358', 'jvm_exit': 1}
----- JVM fatal-error header: [RUN_DIR]/hs_err_pid3478.log -----
#  SIGSEGV (0xb) at pc=0x00007f62b8cac0d5 (sent by kill), pid=3478, tid=3478
# JRE version: OpenJDK Runtime Environment (17.0.20.1+1) (build 17.0.20.1+1-1-22.04-Ubuntu)
# CreateCoredumpOnCrash turned off, no core file dumped
```

#### 4. Smoke executor loss and application removal

Databricks 5565, DBR 14.3, upstream Spark 3.3.0 smoke: **the executor exit, Master's removal decision, and executor stderr were all visible before cleanup**. The REPL itself exited zero, but the missing expected result correctly failed the smoke check.

![Smoke executor loss: selected DBR 14.3 test stage](https://raw.githubusercontent.com/pxLi/cudf-spark/pr-16377-diagnostic-evidence/smoke-executor-removal.png)

```text
INFO Worker: Executor app-20261016054334-0000/0 finished with state EXITED message Command exited with code 86 exitStatus 86
INFO Master: Removing executor app-20261016054334-0000/0 because it is EXITED
ERROR StandaloneSchedulerBackend: Application has been killed. Reason: Master removed our application: FAILED
TEST ONLY: smoke executor halt(86)
FAILED spark-shell smoke test (spark-shell exit=0, tee exit=0, grep exit=1)
```

#### 5. Matching smoke output does not hide driver failure

Databricks 5565, DBR 17.3, upstream Spark 3.5.0 smoke: the query printed the expected result, then the driver halted with code 87. `grep` and `tee` succeeded, but the smoke wrapper correctly failed.

![Smoke driver exit after matching output: selected DBR 17.3 test stage](https://raw.githubusercontent.com/pxLi/cudf-spark/pr-16377-diagnostic-evidence/smoke-driver-exit.png)

```text
val res0: Array[org.apache.spark.sql.Row] = Array([4950])
TEST ONLY: smoke driver halt(87)
FAILED spark-shell smoke test (spark-shell exit=87, tee exit=0, grep exit=0)
```

#### 6. Serial integration-test driver loss

Databricks 5566, DBR 14.3: driver exit 87 propagated through the integration-test wrapper and SSH command. The console retained the test/application identity and dead-driver message. No xdist event was expected in this serial path.

![Serial driver loss: selected DBR 14.3 test stage](https://raw.githubusercontent.com/pxLi/cudf-spark/pr-16377-diagnostic-evidence/serial-driver-exit.png)

```text
TEST ONLY: injecting driver Runtime.halt(87); ...
set_spark_job_timeout: Unusable Spark Driver JVM detected!
Spark process identity: {'worker': 'gw0', 'host': '[REDACTED_HOST]', 'python_pid': 14276, 'jvm_pid': 'externally launched', 'application_id': 'local-1792129652926', 'jvm_exit': 'unavailable'}
FAILED integration tests (exit=87, run_dir=[RUN_DIR])
```

#### 7. Parallel integration-test driver loss

Databricks 5566, DBR 17.3: the dead-driver handler logged cached identity, then the coordinator reported the lost worker and printed its log. The test wrapper returned exit 1. `jvm_exit=None` means the child process had not yet reported an exit status when polled; it is not evidence of a successful exit.

![Parallel driver loss: selected DBR 17.3 test stage](https://raw.githubusercontent.com/pxLi/cudf-spark/pr-16377-diagnostic-evidence/parallel-driver-exit.png)

```text
TEST ONLY: injecting driver Runtime.halt(87); ...
set_spark_job_timeout: Unusable Spark Driver JVM detected!
Spark process identity: {'worker': 'gw1', 'host': '[REDACTED_HOST]', 'python_pid': 13508, 'jvm_pid': 13598, 'application_id': 'local-1792130392253', 'jvm_exit': None}
----- pytest worker lost: gw1, python_pid=unknown, error=Not properly terminated -----
FAILED integration tests (exit=1, run_dir=[RUN_DIR])
```

#### Evidence Limits

- Downloaded all five available debug bundles for the three standalone stages and both CI2 runtime cells; SHA-256 and archive-integrity checks passed. Worker logs and the native-crash header agree with the console.
- CI1 smoke failures did not produce uploaded bundles: the existing uploader found no matching files in the agent workspace. Their driver/lifecycle/executor diagnostics were nevertheless printed on the test host before cleanup. No additional packaging or retention is introduced.
- Abrupt process death can leave incomplete JUnit XML. In particular, serial driver loss did not produce a normal failed testcase; the preserved nonzero command exit still failed CI. Fault-round report totals are not a reliable completed-test count. The separate successful 5563/5564 round verifies report deduplication.
- This validates the seven injected mechanisms, not every real crash or a root cause for the original intermittent smoke failure. It does not validate remote-only executor-log retrieval.
- The separate Markdown-link check failed on the existing Oracle Java conventions URL (status 0), unrelated to these diagnostic changes.

### Checklists

Documentation
- [ ] Updated for new or modified user-facing features or behaviors
- [x] No user-facing change

Testing
- [x] Added or modified tests to cover new code paths
- [ ] Covered by existing tests
      (The success-path sanity round and all seven temporary fault-injection scenarios were validated in CI.)
- [ ] Not required

Performance
- [ ] Tests ran and results are added in the PR description
- [ ] Issue filed with a link in the PR description
- [x] Not required
