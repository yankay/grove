# PR 686: PCSG Scale-to-Zero Gang Scheduling Validation

Test owner: Kay. Execution date: September 22, 2026 (Asia/Shanghai). Status: complete.

## Verdict

**Follow-up, September 22, 2026:** a [strengthened Volcano-only probe](../../operator/hack/gang-validation/VOLCANO_ONLY.md#strengthened-validation) subsequently reproduced partial binding in two deterministic stale-policy injections on Volcano v1.15.2. It delayed delivery of the restored PodGroup policy while forwarding new Pods. The bounded Grove runs below remain valid historical results, but they did not test this ordering and do not establish safe policy handoff. The reproduction is backend-only, not a rerun of the full Grove chain.

The linked hibernation prototype passes the requested KAI and Volcano backend feasibility validation on the recorded configuration: four scenarios, eight `3 -> 0 -> 3` cycles, and twelve insufficient-capacity observation windows. Across 544.06 seconds and 444 repeated assertions, no waking worker bound prematurely. Independent Pod-watch replay confirms all 24 extra-Pod bindings, including initial deployment, followed four live bound minimum Pods.

The unchanged runtime carried by PR 686 fails both baseline cases. Do not interpret this report as an implementation pass for the documentation-only PR. It closes the recorded prototype/backend experiment, not native Kubernetes CompositePodGroup support or the broader compatibility and capability-condition follow-ups.

## Scope

[PR 686](https://github.com/ai-dynamo/grove/pull/686), head `839522c12a0ecd7de4190ff12985a78085ea352f`, adds GREP-0677 only. It does not implement component hibernation. Test its unchanged runtime as a baseline and separately test the GREP-linked [fork prototype](https://github.com/yankay/grove/pull/2), head `d94776b62010d54672d81db130236f37860aab35`. Prototype results must not be attributed to code changes in PR 686.

The meeting record's Kubernetes 1.37 and CompositePodGroup prerequisites concern native Kubernetes hierarchical scheduling. This experiment exercises the KAI and Volcano CRD-backed implementations; it does not validate the native Kubernetes backend, the proposed capability condition, or a complete compatibility matrix.

## Acceptance Criteria

1. Start with a PCSG at `replicas: 3`, `minAvailable: 2`, containing one leader and one worker per replica. All six Pods must become Ready.
2. Scale the derived PCSG through `/scale` to zero. Preserve its positive `minAvailable`; remove its member PodCliques, worker Pods, gang membership, and applicable native PodGroups.
3. Restore the PCSG to three replicas. Replicas 0 and 1 must form the current wake quorum; replica 2 must not bypass it because an anchor was scheduled historically.
4. Prove nontrivial gang enforcement: a real 1-CPU probe must run on the workload node while four quorum Pods cannot collectively fit. The quorum Pods must have no Grove scheduling gate and no node binding. Extra replica Pods must remain gated.
5. Observe the blocked state continuously with a Pod watch and repeated API assertions for 45 seconds. Resource existence alone is not a pass.
6. Release resource pressure and require all six worker Pods to become Ready. Compare old/new worker and native PodGroup UIDs.
7. Repeat twice with all components idle and with a router surviving. In the surviving-router case, preserve its UID, node, and Ready state; restart Grove and delete/reconstruct the native anchor during the blocked wake, then repeat the negative scheduling observation.
8. Capture configuration, actual image identities, native policy, Pod watch, events, and failures for each backend.

## Environment

- Isolated two-node Kind cluster, dedicated kubeconfig and fixed loopback API port; no access to another task's cluster.
- Kubernetes v1.37.0; no explicit CompositePodGroup, GenericWorkload, or TopologyAwareWorkloadScheduling gate override.
- KAI v0.17.0 and Volcano v1.15.2, pinned cached release charts. No global KAI stale-gang override.
- Real kubelets and containers; no KWOK, fake Ready status, or fake binding.
- One workload node: 24 logical CPUs reported by Docker, 17 CPU reserved through kubelet configuration, yielding 7 allocatable CPUs before system Pod requests. Workload Pods request 1 CPU each. A 3-CPU blocker plus 1-CPU probe leaves fewer than 4 CPUs available, while the probe demonstrates individual schedulability.
- Scheduler and Grove components run on the control-plane node. An optional router requests 25m CPU there.
- Sleep workloads model scheduling resource demand, not GPU execution or inference performance.

## Results

| Runtime | Backend | Scenario | Result |
| --- | --- | --- | --- |
| PR 686 head `839522c1` | KAI 0.17.0 | All-idle `3 -> 0 -> 3` | FAIL: restored minimum never leaves Grove scheduling gates |
| PR 686 head `839522c1` | Volcano 1.15.2 | All-idle `3 -> 0 -> 3` | FAIL: same missing-anchor dependency |
| Prototype `d94776b6` | KAI 0.17.0 | All-idle, two cycles | PASS |
| Prototype `d94776b6` | KAI 0.17.0 | Surviving-router, restart, native reconstruction, two cycles | PASS |
| Prototype `d94776b6` | Volcano 1.15.2 | All-idle, two cycles | PASS |
| Prototype `d94776b6` | Volcano 1.15.2 | Surviving-router, restart, native reconstruction, two cycles | PASS |

### Positive And Negative Evidence

- Each backend passed four sleep/wake cycles. Each cycle recreated all six worker Pods with new UIDs and recovered them to Ready after capacity release.
- In the all-idle cases, native PodGroups disappeared at zero and returned with new UIDs. In the surviving-router cases, the router remained Ready with its original UID and node.
- Each backend also passed a Grove restart and a forced native anchor PodGroup deletion/reconstruction while insufficient capacity remained. The reconstructed native PodGroup had a new UID and preserved gang enforcement for another 45-second window.
- All twelve blocked windows required four minimum Pods without Grove scheduling gates, no worker bindings, and gated extra Pods. A real 1-CPU default-scheduler probe was Ready at the same time.
- Volcano explicitly reported `Pending: 2 Schedulable, 2 Unschedulable` and that individual Pods could be placed once `minAvailable` was satisfied. KAI reported insufficient CPU. The Pod watch, not event wording alone, established zero partial bindings.
- All eight idle snapshots preserved `minAvailable: 2`, zero observed PCSG counters, and `MinAvailableBreached=False`, reason `Idle`, with `observedGeneration` equal to the current object generation.

### Local Checks

- The prototype's KAI backend, Volcano backend, and PodGangMap unit packages: 172 tests passed.
- Driver oracle unit tests: seven passed, including rejection of Grove-gated minima, partial binding, premature extras, missing Pods, and stale native policy.
- Independent binding-order, worker-UID, observation-window, and idle-status audits: four successful scenario records passed.
- Ruff lint/format, license-header checks, and `make verify-toc`: passed. No full repository E2E suite, race run, or performance benchmark is claimed.

### Baseline Failure

Both backends initially ran six Ready Pods. Scaling to zero removed all six Pods and the original native PodGroups. Waking to three replicas created three new native PodGroups with `minMember: 2` each, but all six Pods retained `grove.io/podgang-pending-creation`. The remaining PodGangMap entry was `ScaleOut`, contained replica indices `[0, 1, 2]`, and depended on the now-absent original anchor epoch. KAI was observed for 180 seconds after wake; Volcano was observed for 60 seconds.

The baseline therefore does not reach the scheduler-side test precondition. This is a Grove membership/dependency failure, not evidence that either scheduler rejected or accepted a partial wake gang. In particular, new native PodGroup UIDs are insufficient to claim success.

The observed shape agrees with the baseline implementation in `operator/internal/controller/podcliqueset/components/podgangmap/steadystate.go`: scale-out appends new indices to the ScaleOut entry; empty entries are removed except the current-generation ScaleOut entry. This test does not modify that implementation.

### KAI Annotation Contention

The first prototype all-idle run omitted `kai.scheduler/skip-podgrouper` on the PCS itself. It eventually passed both cycles, but initial readiness took approximately 86 seconds while Grove repeatedly reported optimistic status-patch conflicts. A separate 15-second PodGang watch captured 13 MODIFIED events: six with the skip annotation and seven without it. Grove's PCS metadata mirroring and KAI backend annotation enforcement compete over this field. This is not a scheduler gang-admission failure.

Subsequent scenarios explicitly put `kai.scheduler/skip-podgrouper: "true"` on the PCS. A 15-second watch of the annotated scenario captured no missing skip annotation. This is a workload-local test configuration, not the forbidden global stale-gang eviction override. The native KAI PodGroups separately receive the prototype's `spec.stalenessGracePeriod: "-1s"`; the scheduler CLI has no global stale-grace override.

The baseline cases and the first KAI all-idle run used no PCS skip annotation. The saved `workload.json` is authoritative for each case. Use `--omit-pcs-skip-podgrouper` to reproduce that variant with the final driver.

### Test Infrastructure Notes

The first driver bootstrap used an incorrect PCS label selector and was interrupted before any lifecycle verdict; it is excluded from the result matrix. The final selector follows Grove's `app.kubernetes.io/part-of` convention.

Helm 4.2.4 required the full environment-variable name/value when overriding the chart's array. A later server-side Helm upgrade conflicted with controller-managed TLS Secret fields. Retrying with `--server-side=false` completed the upgrade. No TLS verification, admission webhook, scheduling gate, or scheduler policy was disabled to make the scenarios pass.

## Limits And Follow-Ups

- This is a bounded functional experiment, not the complete Grove E2E suite, a performance benchmark, or a long-duration soak. It uses CPU scheduling resources, not GPU devices, inference requests, preemption, or topology-aware placement.
- The two tested backend/version combinations do not establish compatibility with other KAI, Volcano, Kubernetes, or feature-gate combinations. [Issue 806](https://github.com/ai-dynamo/grove/issues/806) still needs a CI-backed compatibility matrix.
- The API server metrics explicitly report `CompositePodGroup=0`, `GenericWorkload=0`, and `TopologyAwareWorkloadScheduling=0`. Passing the third-party CRD-backed scenarios is not evidence for native Kubernetes CompositePodGroup support.
- The proposed capability-unavailable condition's type, trigger, observed generation, and clearing behavior are not implemented or validated by this experiment. The independently checked `MinAvailableBreached=False/Idle` condition is a different contract.
- Prototype feasibility does not make the documentation-only PR's runtime implement hibernation. Any eventual implementation or dependency revision must rerun the driver against its exact source and installed scheduler artifacts.
- The KAI metadata contention remains a separately recorded Grove integration risk. No scheduler-side partial-gang acceptance has been observed in completed prototype scenarios. No issue, PR comment, or team notification was posted; external communication still requires approval.

## Reproduction And Evidence

The test driver is `operator/hack/gang-validation/run.py`. It consumes an explicit kubeconfig, scheduler name, namespace, image, and fresh output directory. It never selects the global current-context.

```bash
python3 operator/hack/gang-validation/run.py \
  --kubeconfig "$KUBECONFIG" --scheduler kai-scheduler \
  --namespace pr686-kai-survivor --image "$WORKLOAD_IMAGE" \
  --output "$EVIDENCE/kai-survivor" \
  --cycles 2 --observe 45 --survivor --disrupt --cleanup
```

Use `--scheduler volcano` for Volcano. Omit `--survivor --disrupt` for the all-idle scenario. Run cases sequentially because they deliberately share and exhaust a task-owned workload node.

The capacity-release method follows Grove GS1/GS2. The lifecycle and live-quorum assertions extend the linked prototype's ZR3/ZR7 patterns. Unlike the standard KWOK setup, this run uses real kubelets and containers. `audit.py` independently reconstructs the Pod watch and checks that each extra replica binds only after all four live minimum Pods are bound.

Task-local build inputs, cluster configuration, captured upstream revisions, binaries, command logs, snapshots, and results are stored under `.ai-tmp/20260922-pcsg-gang-validation/` in this worktree. Do not publish the kubeconfig or credentials.

Evidence index, relative to that directory:

| Artifact | Contents |
| --- | --- |
| `evidence-summary.json` | All six verdicts, twelve observation windows, independent binding-order audit, and native UID history |
| `baseline-kai-v2/`, `baseline-volcano/` | Failed baseline command logs, complete Pod watch, and final dangling-anchor state |
| `prototype-kai-allidle/`, `prototype-kai-survivor/` | KAI workload manifests, per-phase snapshots, Pod watches, and successful results |
| `prototype-volcano-allidle/`, `prototype-volcano-survivor/` | Corresponding Volcano evidence |
| `baseline-final/`, `prototype-final/` | Installed manifests, deployments, image identities, nodes, feature-gate metrics, and controller/scheduler logs |
| `build-baseline/`, `build-prototype/` | Source SHA, binary checksums, and built image identities |
| `prototype-default-podgang-watch.json`, `prototype-annotated-podgang-watch.json` | Unannotated/annotated KAI metadata-contention evidence |
| `tool-artifact-checksums.txt`, `scheduler-image-identities.json`, `kubernetes-version.json` | Pinned tool/chart checksums and actual runtime artifacts |
| `pcsg-gang-validation-evidence.tar.gz`, `pcsg-gang-validation-evidence.tar.gz.sha256` | Portable evidence archive and checksum, excluding kubeconfig and build binaries |

The PR and prototype heads were rechecked after testing and remained unchanged. No production controller code, GREP, GitHub state, or original checkout changes were made.

The task-owned Kind cluster was deleted after evidence capture; `cluster-cleanup.log` records deletion. Both source worktrees and the approximately 12 MiB evidence archive remain available locally.
