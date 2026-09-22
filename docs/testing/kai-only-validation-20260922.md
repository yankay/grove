# KAI-Only Recovery Validation: September 22, 2026

## Verdict

**PASS for all eight recorded runs.** The native KAI PodGroup lifecycle used by the tested Grove prototype supports deletion/recreation and router-preserving in-place restoration in this configuration. Two deterministic delayed-policy injections did not reproduce the partial-binding failure previously observed with Volcano.

This is a scheduler-only feasibility result, not a Grove end-to-end result or approval to merge PR #686. The probe directly creates and patches KAI PodGroups and ordinary Pods. It does not install Grove or create PCSG, PodGangMap, or Grove PodGang objects.

The [program and usage guide](../../operator/hack/gang-validation/KAI_ONLY.md) describe the exact scenarios and execution commands. The entry point is [kai_only.py](../../operator/hack/gang-validation/kai_only.py).

## Configuration

| Item | Recorded value |
| --- | --- |
| Cluster | Dedicated two-node Kind cluster `kind-grove-kai-01a0c817` |
| Kubernetes | v1.37.0 |
| KAI chart and component images | v0.17.0 |
| Scheduler image digest | `sha256:c6971f8391fe354fd224acfadc7166bae03afcda5c1c5f644e97e5258d98d09a` |
| Native API | `scheduling.run.ai/v2alpha2` |
| Queue | `test`, chart-provided unlimited resource limits |
| Workload node | Exactly 7 allocatable CPUs; all test workers request 1 CPU |
| Control node | KAI components, 25m-CPU router, and independent canary |
| Target PodGroup eviction policy | `stalenessGracePeriod: "-1s"`; no global eviction override |
| Negative observation | At least 45 seconds per window, plus continuous Pod watches |
| Podgrouper behavior | Each native member opts out using `kai.scheduler/skip-podgrouper: "true"` |
| Scheduling gates | None on the tested Pods |
| Scheduling implementation | Unmodified KAI scheduler and binder images |

The fault proxy is the same standard-library implementation used for the Volcano experiment, with a KAI installer option. It routes only the scheduler's API traffic through Pod loopback. Binder, admission, podgrouper, PodGroup controller, and queue controller retain their ordinary API connections. The installer scales the KAI deployment-management operator to zero in this disposable cluster so it cannot undo the instrumentation. No scheduler algorithm, scheduling plugin, or scheduler-global eviction flag is changed. Both no-delay proxy controls use this same instrumentation.

## Recorded Results

| Evidence directory | Scenario | Result |
| --- | --- | --- |
| `single-recreate/` | Delete all members and PodGroup; restore under insufficient capacity and then with a required subgroup absent | PASS; new worker and PodGroup UIDs, zero premature bindings, both recoveries Ready |
| `single-inplace/` | Preserve router; patch `minMember: 5 -> 1 -> 5` and remove/restore four subgroups | PASS; same PodGroup UID, both recoveries Ready, uninterrupted router |
| `multi-recreate/` | Require leader=1 and worker=2; supply three leaders and two workers, one worker unschedulable | PASS; zero bindings for 45.13 seconds, all five workers Ready after replacement |
| `multi-inplace/` | Same two-Pod worker subgroup, plus surviving router | PASS; zero bindings for 45.38 seconds, five workers recover, uninterrupted router |
| `control-inplace/` | Same proxy and paused deployment operator, no injected delay | PASS; capacity and missing-subgroup windows, both recoveries, router continuity |
| `stale-policy/` | Freeze router-only policy while forwarding restored Pods | PASS; zero waking bindings for 45.39 seconds, live canary, successful recovery |
| `stale-repeat/` | Repeat the policy-delay injection in a fresh namespace | PASS; zero waking bindings for 45.53 seconds, live canary, successful recovery |
| `control-recreate/` | Same proxy, no delay, deletion/recreation repeated | PASS; both negative windows and recoveries |

Across the eight runs, the 12 negative windows totaled **549.03 seconds and 452 polling checks**, supplemented by continuous Pod watches. All 12 recovery cycles passed. All five router-preserving runs passed full-watch continuity checks: the router retained its UID and node and remained Ready after first becoming Ready.

The multiple-member case distinguishes subgroup enforcement from aggregate headcount. With three schedulable leaders and one schedulable worker, there are enough schedulable Pods to satisfy the total minimum of three, but not the required two-Pod worker subgroup. A successful five-CPU probe establishes available capacity before the negative window. No waking member binds until the second worker becomes schedulable.

## Why The Delayed-Policy Case Passed

Both injections established this state:

```text
API PodGroup:
  minMember: 5
  subGroups: router, leader-0, worker-0, leader-1, worker-1

Last policy forwarded to the KAI scheduler:
  minMember: 1
  subGroups: router

All four new worker Pod UIDs:
  forwarded to the scheduler, no scheduling gates

Independent KAI canary:
  bound and Ready while the new target policy was held
```

The first injection held PodGroup resource version `5791`; the second held `6463`. Neither hold expired or reported a proxy transport error. The API snapshots show the new five-member policy, while the proxy records show the old router-only policy was still the last one delivered to the scheduler.

More importantly, all four restored Pods received `PodScheduled=False`, `reason=Unschedulable`, and a member-specific message such as:

```text
Pod references subgroup "worker-0", which does not exist in PodGroup kai-only-stale-policy/wake
```

These conditions establish that the scheduler was actually processing the restored members against the missing-subgroup state, not merely that the proxy had forwarded an old object. In each blocked snapshot, only the router and canary had successful BindRequests; the four restored workers had none. The primary safety oracle remains actual Pod node assignments and the Pod watch, since the separate binder does not pass through the scheduler's proxy.

This agrees with the inspected KAI v0.17.0 implementation: `PodGroupInfo.AddTaskInfo` looks up the member's subgroup in `PodSets`; a missing subgroup is recorded in `InvalidSubGroupTasks` and is not assigned to a normal schedulable PodSet. The error is produced by `addInvalidSubGroupTask` in `pkg/scheduler/api/podgroup_info/job_info.go`. Releasing the updated subgroup policy and resource blockers allowed all four restored workers to become Ready without replacing the router.

The difference from the recorded Volcano experiment is therefore concrete and scenario-specific: KAI rejected the new members' unknown subgroup names under the old router-only policy, whereas Volcano's delayed-policy runs bound two of four restored members early. This does not turn API Patch/GET into a general scheduler-observation barrier.

## Scope And Remaining Gaps

- Tested idle semantics remove worker subgroup entries, then restore them. Raising minima within subgroup names that remain present in an old cached policy is a different case and is not validated here.
- Whole-gang idle deletes the native PodGroup. The probe does not leave an empty `minMember: 0` PodGroup.
- PCSG admission, PodGangMap entries and epochs, Grove PodGang mutation, owner-reference garbage collection, current-generation Anchor dependency checks, and ScaleOut release ordering are not exercised.
- GPU resources, topology constraints, other KAI/Kubernetes versions, scheduler restart during a hold, and arbitrary API delivery failures are outside this test.
- This result does not close the compatibility-matrix, capability-condition, ownership, or release-coordination questions from the meeting notes.

## Evidence And Verification

Evidence is in the validation worktree under `.ai-tmp/20260922-pcsg-gang-validation/kai-only/`. `summary.json` indexes all eight runs; `audit-results.sh` and `audit.log` independently check the expected run/window/recovery counts, exact missing-subgroup conditions for every restored Pod in both delayed snapshots, successful router/canary BindRequests, clean hold completion, and final source checksums.

Per-run evidence includes `result.json`, actual input bodies, command results, Pod watches, native policy snapshots, Pods, Events, and BindRequests. The first `single-recreate` run preceded the addition of BindRequest snapshots; the final `control-recreate` run repeats the same behavioral checks with that diagnostic collection enabled. Scheduler, binder, PodGroup-controller and proxy logs, Helm values, cluster settings, installed proxy checksum, and image identities are saved separately.

All test-driver kubectl commands returned zero. Setup emitted registry-helper warnings, a transient Helm release-metadata update timeout, and an initial port-forward connection retry; installation and readiness completed before the affected tests. Neither delayed observation window had transport errors. Final KAI components and the proxy were Ready with zero container restarts; the deployment operator was intentionally absent during instrumentation.

The focused suite passed **36 unit tests**, including all 28 existing checks and eight KAI-specific checks. Ruff lint/format and source-license checks passed. No Grove Go implementation was changed; the full Grove Go test suite was not rerun. Shared-probe changes are covered by the existing unit tests, but Volcano cluster experiments were not rerun in this task.

The task-local port forward was stopped after the final run. Cluster cleanup is recorded in `cleanup.log`. The source bundle and evidence bundle exclude kubeconfig credentials and Secrets. At the time of this historical evidence capture, no changes had been committed, pushed, or posted to GitHub.
