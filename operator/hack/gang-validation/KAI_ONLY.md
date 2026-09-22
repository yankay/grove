# KAI-Only PodGroup Feasibility Probe

`kai_only.py` tests KAI's native PodGroup recovery without Grove. Keep `run.py`, `volcano_only.py`, and, for optional fault injection, `volcano_fault_proxy.py` beside it. The two scheduler probes share their lifecycle scenarios, Pod-watch checks, timeouts, and evidence format. Only the native policy and Pod membership mapping differ.

**Recorded result, September 22, 2026:** all eight runs passed on KAI v0.17.0 and Kubernetes v1.37.0, including two deterministic delayed-policy injections. See the [full report](../../../docs/testing/kai-only-validation-20260922.md) for results, evidence, and limits.

The probe neither creates nor validates PCSG, PodGangMap, or Grove PodGang. It manually issues the native operations that the prototype's KAI backend performs, and verifies actual Pod binding and readiness, not merely resource recreation.

## Native Mapping

- API: `scheduling.run.ai/v2alpha2`, resource `podgroups.scheduling.run.ai`.
- PodGroup: `minMember` is the sum of member minima; each member maps to one `subGroups` entry containing `name` and `minMember`.
- Pod: `schedulerName: kai-scheduler`, annotation `pod-group-name`, label `kai.scheduler/subgroup-name`, queue label `kai.scheduler/queue`, and annotation `kai.scheduler/skip-podgrouper: "true"`.
- Every target PodGroup sets `stalenessGracePeriod: "-1s"` to match the tested Grove prototype. This disables stale-gang eviction for that PodGroup, without changing global scheduler configuration.
- An in-place update patches the total minimum and the complete subgroup list together, with a resource-version precondition. Router-only idle means `minMember: 1` and exactly one router subgroup, not zero-sized worker subgroups.

## Scenarios

| Scenario | Recreate | In Place |
| --- | --- | --- |
| `singleton` | Delete all four workers and PodGroup; recreate with new UIDs | Keep router and PodGroup UIDs; remove and restore four worker subgroups |
| `multi-subgroup` | Recreate a gang requiring leader=1 and worker=2 | Same minima plus a surviving router |
| `stale-policy` | Not applicable | Delay the new policy while forwarding every restored Pod |

`singleton` has two negative windows, each followed by successful recovery. First, a 3-CPU blocker and a persistent 1-CPU probe leave insufficient capacity for four 1-CPU members. A second temporary 1-CPU proof Pod must become Ready and be removed before the members are created, proving that the blocked window still has capacity for partial scheduling. No member may bind. Second, four members are supplied but one worker subgroup is missing, replaced with an extra leader. Total count and CPU capacity suffice, but subgroup membership does not; no member may bind until the correct worker is supplied.

`multi-subgroup` supplies three leaders and two workers. One worker has an unsatisfiable node selector. There are enough schedulable Pods for the total minimum, but not for the two-member worker subgroup. A prior five-CPU probe proves sufficient capacity. All waking Pods must remain unbound; after replacing the blocked worker, all five must become Ready.

`stale-policy` preserves the router and freezes the scheduler's last forwarded policy at `minMember: 1` with only the router subgroup. The API is updated to `minMember: 5` with five subgroups. All four restored Pod UIDs must be forwarded to the scheduler, and an independent pre-registered KAI canary gang must become Ready during the hold. No restored member may bind. Releasing the hold and resource blockers must permit recovery. A forbidden binding remains a failure even if recovery subsequently succeeds.

All negative windows reject scheduling gates, incorrect native membership, missing or replaced Pods, inactive Pod watches, and partial binding. In-place runs replay the complete Pod watch to verify router UID, node, and readiness continuity.

## Prerequisites And Execution

Use an isolated disposable two-node cluster and dedicated kubeconfig. Exactly one node must have `validation.grove.io/role=workload`, 7 allocatable CPUs, and less than 1 CPU requested by system Pods. The other must have `validation.grove.io/role=control`. Install KAI and its matching CRDs on the control node, with a usable queue named `test` or pass `--queue`. Supply Python 3.12+, `kubectl`, and a workload image containing `sh` and `sleep`. No Python packages are required.

Run cases sequentially, always with a fresh namespace and output directory:

```bash
python3 operator/hack/gang-validation/kai_only.py \
  --kubeconfig "$KUBECONFIG" --image "$WORKLOAD_IMAGE" \
  --mode recreate --scenario singleton \
  --namespace kai-single-recreate --output "$EVIDENCE/single-recreate" \
  --observe 45 --cleanup

kubectl --kubeconfig "$KUBECONFIG" wait --for=delete \
  namespace/kai-single-recreate --timeout=180s
```

Repeat with `--mode inplace`; test both modes with `--scenario multi-subgroup`.

### Optional Policy Delay

**Only install into the dedicated disposable cluster.** The installer saves the original KAI operator and scheduler Deployments, scales `kai-operator` to zero to prevent it undoing the test instrumentation, and routes only the scheduler through a loopback API proxy using `KUBECONFIG`. Binder, admission, podgrouper, PodGroup controller, and queue controller remain running and use their normal API connections. Scheduler algorithms and flags are unchanged. Namespace cleanup does not remove the instrumentation; delete the disposable cluster after evidence capture.

```bash
python3 operator/hack/gang-validation/volcano_fault_proxy.py \
  --install --scheduler kai-scheduler --kubeconfig "$KUBECONFIG" \
  --image "$PYTHON_IMAGE" --output "$EVIDENCE/fault-install"

kubectl --kubeconfig "$KUBECONFIG" -n kai-scheduler \
  port-forward deployment/kai-scheduler-default 58690:18080 --address=127.0.0.1
```

With the port forward running, use another terminal:

```bash
python3 operator/hack/gang-validation/kai_only.py \
  --kubeconfig "$KUBECONFIG" --image "$WORKLOAD_IMAGE" \
  --scenario stale-policy --mode inplace \
  --fault-proxy http://127.0.0.1:58690 \
  --namespace kai-stale-policy --output "$EVIDENCE/stale-policy" \
  --observe 45 --cleanup
```

The proxy name is historical; the same standard-library implementation handles KAI and Volcano PodGroup resources. It holds changed target policies in API responses without altering their contents, and forwards Pods normally. The affected PodGroup watch stream pauses behind the held event, so the canary PodGroup must be delivered before arming. API authentication and CA validation remain enabled; tokens are not recorded. Holds have bounded deadlines and are explicitly released by the driver. Since KAI uses a separate binder, an empty proxy binding-request list is not a safety verdict: the oracle examines actual Pod node assignments and Pod watches.

## Evidence And Scope

`result.json` contains the verdict and individual checks. Inputs, API results, Pod watches, Pods, Events, native PodGroups, and BindRequests are saved per phase, alongside version, node, CRD, queue, and deployment information. Save scheduler and binder logs before deleting the cluster. Never publish kubeconfigs or Secrets with evidence.

```bash
python3 -m unittest discover -s operator/hack/gang-validation -p 'test_*.py' -v
ruff check operator/hack/gang-validation
ruff format --check operator/hack/gang-validation
```

Passing this probe establishes only the tested native policy lifecycle and admission behavior. It does not establish Grove reconciliation correctness, PodGangMap dependency ordering, extra-replica gating, GPU behavior, topology handling, native Kubernetes CompositePodGroups, or arbitrary policy-update safety. In particular, restoring previously absent subgroup names is not equivalent to increasing minima inside subgroup names that remain present in an old cached policy.
