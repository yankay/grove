# Volcano-Only PodGroup Feasibility Probe

`volcano_only.py` directly creates and patches Volcano PodGroups and ordinary Pods. It does not install, query, or create Grove resources: no PodCliqueSet, PCSG, PodGangMap, or PodGang. Keep `run.py` beside it; only its standard-library kubectl, Pod-watch, wait, and evidence helpers are reused.

**Updated verdict, September 22, 2026:** the strengthened stale-policy test reproduced partial binding in two independent runs. Ordinary policy mutation and subgroup enforcement can work, but API readback alone is not a scheduler-observation barrier. The original passing runs below do not establish race safety.

## What Is Tested

| Mode | Lifecycle | Expected PodGroup identity |
| --- | --- | --- |
| `recreate` | Four workers Ready, delete all workers and the PodGroup, recreate both | Same name, new UID |
| `inplace` | Router plus four workers Ready, patch to router-only, delete workers, restore full policy and new workers | Same name and UID; router stays Ready with the same UID and node |

The default `--scenario singleton` performs two sleep/wake cycles:

1. **Insufficient capacity:** a 3-CPU blocker and a persistent 1-CPU probe run on the 7-CPU workload node. A second temporary 1-CPU proof Pod must become Ready and be removed before the gang members are created, proving residual capacity for partial scheduling. Four 1-CPU gang members must then remain unbound without scheduling gates. After 45 seconds, release capacity and require all four workers to become Ready.
2. **Missing subgroup:** first prove a 4-CPU ordinary Pod can run, then remove it. Restore the gang with four worker Pods, but give the fourth Pod the first leader's member label instead of the required second worker label. The total Pod count reaches `minMember`, and CPU capacity is sufficient, but one required subgroup is absent. No worker may bind during the observation window. Replace the decoy with the correct member and require all workers to become Ready.

The second cycle distinguishes `subGroupPolicy` enforcement from a scheduler that only checks the aggregate `minMember`. Every policy uses a member-specific label selector, `subGroupSize: 1`, and `minSubGroups: 1`. In-place patches update `minMember` and the complete `subGroupPolicy` together, with a resource-version precondition.

No Pod scheduling gates, Grove dependency controller, fake bindings, or fake readiness are used. This probe does not claim to validate PCSG scale admission, PodGangMap ordering, extra replicas, GPUs, topology constraints, scheduler restart, or native Kubernetes CompositePodGroups.

Two additional scenarios target the remaining questions:

- `--scenario multi-subgroup`: require one leader and two workers, with `minSubGroups: 1` for each role. Supply three leaders and two workers, but make one worker's node selector unsatisfiable. A five-CPU ordinary probe first proves sufficient aggregate capacity. There are enough schedulable Pods to satisfy the total minimum, but the two-Pod worker subgroup is incomplete. No waking member may bind; replacing the blocked worker must allow all five to become Ready. Both `recreate` and `inplace` are supported; the latter adds the surviving router to the total minimum.
- `--scenario stale-policy`: keep the router, freeze delivery of the new PodGroup policy to Volcano, and forward new Pod events normally. The API contains `minMember: 5`, while the last policy forwarded to Volcano has `minMember: 1` and only the router subgroup. Capacity allows some, but not all, waking workers. An independent, pre-registered canary gang must still schedule through Volcano. Any early binding fails the test, even if releasing the delay and capacity subsequently makes all workers Ready. Requires `--mode inplace` and the optional fault proxy below.

## Prerequisites

- A dedicated disposable two-node cluster. Do not use a shared workload node; the test deliberately consumes schedulable CPU.
- Exactly one node labeled `validation.grove.io/role=workload`, with 7 allocatable CPUs and fewer than 1 CPU requested by system Pods. For the recorded 24-CPU host, the Kind worker uses kubelet `kube-reserved=cpu=17,memory=256Mi`.
- A separate node labeled `validation.grove.io/role=control`. Volcano components run there. The optional router tolerates the standard control-plane `NoSchedule` taint.
- A running Volcano scheduler named `volcano`, matching CRDs exposing `spec.subGroupPolicy`, and an open queue with sufficient resources, `default` unless overridden.
- Python 3.12+, `kubectl`, and an available image containing `sh` and `sleep`.

## Run

Always pass a dedicated kubeconfig and a fresh namespace and evidence directory. Run the modes sequentially.

```bash
python3 operator/hack/gang-validation/volcano_only.py \
  --kubeconfig "$KUBECONFIG" \
  --mode recreate --namespace volcano-only-recreate \
  --image "$WORKLOAD_IMAGE" \
  --output "$EVIDENCE/volcano-only-recreate" \
  --observe 45 --cleanup

kubectl --kubeconfig "$KUBECONFIG" wait --for=delete \
  namespace/volcano-only-recreate --timeout=180s

python3 operator/hack/gang-validation/volcano_only.py \
  --kubeconfig "$KUBECONFIG" \
  --mode inplace --namespace volcano-only-inplace \
  --image "$WORKLOAD_IMAGE" \
  --output "$EVIDENCE/volcano-only-inplace" \
  --observe 45 --cleanup
```

`--cleanup` deletes only the namespace created by that invocation, after saving diagnostics, including on failure. Omit it to retain resources for inspection. Cluster setup and deletion are deliberately outside the probe.

### Multi-Pod Subgroup

Use a fresh namespace and output directory for each mode; wait for namespace deletion before the next capacity-sensitive test.

```bash
python3 operator/hack/gang-validation/volcano_only.py \
  --kubeconfig "$KUBECONFIG" --image "$WORKLOAD_IMAGE" \
  --scenario multi-subgroup --mode inplace \
  --namespace volcano-multi-inplace --output "$EVIDENCE/multi-inplace" \
  --observe 45 --cleanup
```

Repeat with `--mode recreate` and distinct namespace/output values.

### Deterministic Policy Delay

**Use only the dedicated disposable cluster.** Installing the proxy changes and restarts its Volcano scheduler Deployment. It does not modify the Volcano binary or scheduling configuration. The installer saves the original Deployment and applied patch. Namespace cleanup does not uninstall the proxy; delete the disposable cluster after collecting evidence.

The optional `volcano_fault_proxy.py` sidecar uses Python's standard library. It listens only on Pod loopback, forwards authenticated requests to the API server with service-account CA verification, and records no tokens. The host control endpoint is exposed through loopback-only port forwarding. New policies for the target PodGroup are held in watch, list, direct-read, and write responses; unrelated Pod streams continue. The affected PodGroup watch stream pauses behind the held event, so other PodGroup events on that stream can also be delayed. The canary PodGroup is therefore delivered before arming the fault. Holds expire after a bounded timeout and are explicitly released in test cleanup.

```bash
python3 operator/hack/gang-validation/volcano_fault_proxy.py \
  --install --kubeconfig "$KUBECONFIG" \
  --image "$PYTHON_IMAGE" --output "$EVIDENCE/fault-install"

kubectl --kubeconfig "$KUBECONFIG" -n volcano-system \
  port-forward deployment/volcano-scheduler 58688:18080 --address=127.0.0.1
```

Leave port forwarding running in that terminal. In another terminal:

```bash
python3 operator/hack/gang-validation/volcano_only.py \
  --kubeconfig "$KUBECONFIG" --image "$WORKLOAD_IMAGE" \
  --scenario stale-policy --mode inplace \
  --fault-proxy http://127.0.0.1:58688 \
  --namespace volcano-stale-policy --output "$EVIDENCE/stale-policy" \
  --observe 45 --cleanup
```

The recorded Volcano v1.15.2 runs exit **1** with `Forbidden partial gang binding`. This is a reproduced safety failure, not a successful negative-window assertion. `fault-control.jsonl` records the held resource versions, last forwarded policy, forwarded Pod UIDs, binding requests during the hold, and transport errors. `stale-policy-violation/` captures actual API state before releasing the fault. A passing test requires an active, unexpired hold, no proxy errors, delivery of all current worker UIDs, a Ready canary, and no early worker binding.

## Evidence And Exit Status

Exit code zero requires every negative window and recovery in the selected scenario to pass. `result.json` records the verdict and each check. `inputs.jsonl` contains the actual create and patch bodies; `commands.jsonl` captures command output; `pod-watch.jsonl` records continuous Pod events. Phase directories contain Pods, PodGroups, and Events. Cluster version, nodes, CRDs, queue, and deployment images are captured separately; kubeconfig contents are never copied.

Run the focused unit checks:

```bash
python3 -m unittest discover -s operator/hack/gang-validation -p 'test_*.py' -v
ruff check operator/hack/gang-validation
ruff format --check operator/hack/gang-validation
```

## Original Recorded Run

On September 22, 2026, both modes passed on a dedicated two-node Kind cluster running Kubernetes v1.37.0 and Volcano v1.15.2, without Grove CRDs. Volcano used the chart's default `enqueue, allocate, backfill` actions and gang plugin; scheduler components ran on the control node.

| Check | `recreate` | `inplace` |
| --- | --- | --- |
| Insufficient-capacity window | PASS: no worker bound | PASS: no worker bound |
| Missing-subgroup window, total count satisfied | PASS: no worker bound | PASS: no worker bound |
| Recovery after each window | All four workers Ready | All four workers Ready |
| PodGroup identity across both cycles | Same name, new UID | Same name and UID |
| Router continuity, full Pod-watch replay | Not applicable | Same UID and node; remained Ready |

The four negative windows totaled 181.38 seconds and 159 polling checks, supplemented by Pod watches. All four recoveries passed. The focused suite passed 20 unit tests; Ruff lint, format, and source-license checks passed. No scheduling failure was observed in these runs. These results establish feasibility only for the tested configuration and scenarios, not a general compatibility matrix or Grove end-to-end correctness.

Evidence is under `.ai-tmp/20260922-pcsg-gang-validation/volcano-only/` in the validation worktree. Use `final-recreate/result.json` and `final-inplace/result.json` for the recorded verdicts; earlier exploratory runs are excluded. The directory also contains per-phase snapshots, command and Pod-watch logs, scheduler configuration and logs, image references, and `tested-source.sha256`. The dedicated test cluster was deleted after evidence collection; `cleanup.log` records successful deletion.

## Strengthened Validation

The follow-up on September 22, 2026 used a newly created dedicated cluster with the same Kubernetes v1.37.0 and Volcano v1.15.2 versions. Evidence remains under the same `volcano-only/` directory, separate from the original runs.

| Scenario | Evidence directory | Result |
| --- | --- | --- |
| Two-Pod worker subgroup, PodGroup recreated | `multi-recreate/` | PASS: no partial binding for 45.22 seconds; all five workers Ready after replacement |
| Two-Pod worker subgroup, router preserved | `multi-inplace/` | PASS: no partial binding for 45.48 seconds; all five workers Ready after replacement; router continuity passed |
| Stale router-only policy, first injection | `stale-policy/` | FAIL: two waking leaders bound while both workers remained Pending |
| Stale router-only policy, repeated injection | `stale-repeat/` | FAIL: partial binding reproduced |
| Same proxy, no delay, router preserved | `control-inplace/` | PASS: capacity and missing-subgroup windows; both recoveries and router continuity |
| Same proxy, no delay, PodGroup recreated | `control-recreate/` | PASS: capacity and missing-subgroup windows; both recoveries |

The first injection held PodGroup resource version `2242` (`minMember: 5`, five subgroup policies) while the last forwarded version was `2217` (`minMember: 1`, router-only policy). All four new Pod UIDs were forwarded. At 10:24:38.315-10:24:38.317 UTC, Volcano issued bindings for both leaders and the independent canary while the delay was active. The violation snapshot shows both leaders Running and both workers Pending, with the capacity blocker and probe still Running. The canary became Ready, and the proxy reported no transport errors or timeout expiry. Releasing the fault and capacity allowed all four workers to become Ready, without interrupting the router; the overall verdict deliberately remained FAIL.

This demonstrates a concrete stale-policy race in this minimal integration, not its frequency in ordinary operation. It does not establish that PodGangMap or PodGang planning is wrong, and it is not a Grove end-to-end reproduction. A production handoff needs a scheduler-observation guarantee or another protocol that prevents new members from being admitted under the old policy. A successful API Patch followed by GET is insufficient on its own.

The optional proxy used Python 3.11.2 from the already-cached `builder:1.26.3-bookworm` image, staged in the task-local registry as `grove/volcano-proxy-python:01a0c817-local` (manifest digest `sha256:4856e8bd43383c7c121323ea87a9e00c6d8af92b02b8a13dadce2eed43a0fc68`). The initial Python image pull failed; no cluster test result depends on that image. The installed proxy source hash was checked against the local file. The 28 focused unit tests and Ruff/license checks passed.

`strengthened-summary.json` indexes all six scenarios: four passes and two reproduced safety failures. `strengthened-source.sha256` records the final program and test sources. Scheduler/proxy logs, deployed image identities, proxy installation inputs, and the per-run evidence are retained separately from the original evidence. The dedicated cluster and local port forwarding were removed after capture; `strengthened-cleanup.log` records successful cluster deletion.
