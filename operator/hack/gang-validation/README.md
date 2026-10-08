# KAI and Volcano Capability Validation Experiments

This suite groups the Grove PCSG lifecycle experiments, standalone KAI and Volcano capability probes, deterministic policy-delay fault injection, and evidence audits. The [experiment index](../../../docs/testing/kai-volcano-capability-validation-experiments.md) records the historical results, evidence locations, and transfer scope. Copying the suite into this worktree does not establish that this worktree's Grove revision passes the cluster experiments.

| Experiment | Entry point | Guide |
| --- | --- | --- |
| Grove PCSG scale-to-zero and recovery | `run.py` | This document |
| Native KAI PodGroup capabilities | `kai_only.py` | [KAI guide](KAI_ONLY.md) |
| Native Volcano PodGroup capabilities | `volcano_only.py` | [Volcano guide](VOLCANO_ONLY.md) |
| Policy-delivery fault injection for either scheduler | `volcano_fault_proxy.py` | Scheduler-specific guides above |
| Grove evidence replay | `audit.py` | Audit section below |

## Grove PCSG Lifecycle Experiment

This black-box test uses real Kubernetes Pods and scheduler-native PodGroups. It compares Grove revisions without importing either revision's Go API types. The [September 22 report](../../../docs/testing/pr686-gang-validation-20260922.md) records the tested revisions, configuration, results, and limitations.

For a smaller experiment without Grove, use the [Volcano-only probe](VOLCANO_ONLY.md). It tests native PodGroup recreation, in-place policy mutation, insufficient capacity, and a missing required subgroup despite a sufficient total Pod count.

The [KAI-only probe](KAI_ONLY.md) applies the same lifecycle and negative checks to KAI `minMember` and `subGroups`, including a two-Pod worker subgroup and deterministic delayed-policy injection.

## Prerequisites

- Use a dedicated disposable cluster and explicit kubeconfig. Never use a shared workload node: the test intentionally exhausts its schedulable CPU.
- Label exactly one workload node `validation.grove.io/role=workload`. Give it 7 allocatable CPUs, with fewer than 1 CPU requested by system Pods. The recorded 24-CPU Kind host used kubelet `kube-reserved=cpu=17,memory=256Mi`; adjust the reserved amount for another host.
- Label a separate node `validation.grove.io/role=control`. Run Grove and scheduler components there, tolerating `node-role.kubernetes.io/control-plane:NoSchedule` when applicable.
- Install the selected scheduler with its matching CRDs. The experiment uses KAI 0.17.0, queue `test`, and Volcano 1.15.2, queue `default`.
- Install Grove and its CRDs from the exact runtime revision being tested. Enable `default-scheduler`, `kai-scheduler`, and `volcano` only when the corresponding dependencies are present. When switching revisions, update CRDs explicitly; Helm upgrade alone does not upgrade CRDs.
- Supply an image with `sh` and `sleep`. The image must be accessible from both nodes. The saved image digest identifies the BusyBox artifact used in the report.
- Python 3.12+ and `kubectl` are sufficient for the driver; no Python packages are required.

The source and image build details are recorded in the report's evidence bundle. Do not enable a scheduler-global stale-gang eviction override. The prototype sets the negative staleness grace period only on its own native KAI PodGroups.

## Run

```bash
python3 operator/hack/gang-validation/run.py \
  --kubeconfig "$KUBECONFIG" \
  --scheduler kai-scheduler \
  --namespace gang-kai-allidle \
  --image "$WORKLOAD_IMAGE" \
  --output "$EVIDENCE/kai-allidle" \
  --cycles 2 --observe 45 --cleanup
```

Add `--survivor --disrupt` for the running-router, operator-restart, and forced native PodGroup reconstruction scenario. Change `--scheduler` to `volcano` for Volcano. Every invocation requires a new output directory and namespace. Run sequentially, waiting for the preceding namespace to disappear.

By default the PCS carries `kai.scheduler/skip-podgrouper: "true"` to avoid the known metadata-writer contention documented in the report. Use `--omit-pcs-skip-podgrouper` to reproduce the unannotated configuration. This does not disable the scheduler or Grove's production admission/gating paths.

The driver returns nonzero on failure and saves diagnostics before optional namespace deletion. Before each capacity-sensitive wake, it keeps the 3-CPU blocker and persistent 1-CPU probe running, schedules and removes a second 1-CPU proof Pod, and only then creates the gang members. A PASS therefore requires verified residual capacity, an unbound scheduler-visible quorum, and real Ready Pods after capacity release. `SchedulingGated` minimum Pods cannot satisfy the negative test.

## Audit

```bash
python3 operator/hack/gang-validation/audit.py \
  "$EVIDENCE/kai-allidle" "$EVIDENCE/kai-survivor" \
  "$EVIDENCE/volcano-allidle" "$EVIDENCE/volcano-survivor" \
  --output "$EVIDENCE/summary.json"
```

The audit replays the saved Pod watch to ensure every newly bound extra replica follows four currently bound minimum Pods. It also checks worker UID replacement and negative observation durations. Input cases whose driver failed stay failed in the summary.

Run focused driver checks with:

```bash
python3 -m unittest discover -s operator/hack/gang-validation -p 'test_*.py' -v
ruff check operator/hack/gang-validation
ruff format --check operator/hack/gang-validation
```

Raw kubeconfigs contain credentials and must not be published with test evidence.
