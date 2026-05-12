# GREP-531: Workload API Gang Scheduling for default-scheduler Backend

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
  - [Limitations / Risks &amp; Mitigations](#limitations--risks--mitigations)
- [Design Details](#design-details)
  - [Architecture Overview](#architecture-overview)
  - [OperatorConfiguration Extension](#operatorconfiguration-extension)
  - [API Version Strategy](#api-version-strategy)
  - [PodGang Mapping and Updates](#podgang-mapping-and-updates)
  - [Key Control Flow](#key-control-flow)
  - [Validation and API Discovery](#validation-and-api-discovery)
  - [Dependencies](#dependencies)
  - [Test Plan](#test-plan)
  - [Graduation Criteria](#graduation-criteria)
  - [Implementation Phases (by Kubernetes minor)](#implementation-phases-by-kubernetes-minor)
    - [Phase 1: Kubernetes 1.35 (v1alpha1)](#phase-1-kubernetes-135-v1alpha1)
    - [Phase 2: Kubernetes 1.36 (v1alpha2, conditional on KEP-5832 shipping)](#phase-2-kubernetes-136-v1alpha2-conditional-on-kep-5832-shipping)
    - [Phase 3: Kubernetes 1.37+ (hierarchical, topology, GA-track)](#phase-3-kubernetes-137-hierarchical-topology-ga-track)
      - [Hierarchical Gang via CompositePodGroup](#hierarchical-gang-via-compositepodgroup)
- [Appendix](#appendix)
  - [Upstream Kubernetes KEPs](#upstream-kubernetes-keps)
<!-- /toc -->

## Summary

Add gang scheduling to Grove's existing `default-scheduler` backend (introduced in [GREP-375][grep-375]) by translating each `PodGang` into upstream `scheduling.k8s.io` `Workload` / `PodGroup` resources, so `kube-scheduler` enforces all-or-nothing admission. Opt-in via the existing `KubeSchedulerConfig.GangScheduling` field; no new backend, no framework change, no new user-facing API. Satisfies the Beta criterion already recorded in [GREP-375][grep-375] and is forward-compatible with [KEP-6012 CompositePodGroup][kep-6012] for hierarchical gangs.

## Motivation

Today the `default-scheduler` backend only sets `Pod.Spec.SchedulerName` and provides no gang guarantees, leaving operators on vanilla clusters with no path to gang admission for Grove workloads. Upstream `scheduling.k8s.io` Workload APIs close that gap once Grove is built against a Kubernetes minor that exposes the chosen API version: v1alpha1 ships in Grove's current dependency baseline (`k8s.io/api` v0.35.x), while the v1alpha2 decoupled `PodGroup` model is still unreleased design work. Grove's `PCS → PCSG → role` hierarchy is a natural consumer of the upcoming hierarchical pieces (KEP-6012).

### Goals

- Map each `PodGang` to upstream `Workload` / `PodGroup`.
- Implement `SyncPodGang`, `OnPodGangDelete`, `PreparePod` in the `kube` backend (today no-ops) behind `KubeSchedulerConfig.GangScheduling`.
- **Reject-at-submit** for shapes the chosen API version cannot represent ([GREP-375][grep-375]'s "fail-submit vs pass-through").
- Define **API discovery**, an **escape hatch** for external owners (Kueue, cross-PCS controllers), and a forward path to **hierarchical gang** via [KEP-6012][kep-6012].

### Non-Goals

- Adding a new backend, changing `SchedulerProfile.Name`, or redesigning the framework ([GREP-375][grep-375]).
- New user-facing scheduling API in `PodCliqueSet` / `PodGang`.
- Workload-aware extensions such as [KEP-5732 TAS][kep-5732], topology on `Workload` (future `kube/topology.go`), and CompositePodGroup wiring in **alpha** (forward-compatible only).
- Kueue integration logic; Kueue consumes the `Workload` directly or via the escape hatch described in [PodGang Mapping and Updates](#podgang-mapping-and-updates).
- Reacting to `PodGang` status-only updates (current contract preserved).

## Proposal

When `GangScheduling=true`, the backend:

1. Reconciles a `Workload` per `PodGang`, with `MinCount = MinReplicas` per role; Phase 2 also reconciles standalone `PodGroup`s.
2. In `PreparePod`, sets `Pod.Spec.WorkloadRef` in Phase 1 or `Pod.Spec.SchedulingGroup.PodGroupName` in Phase 2.
3. Runs PodCliqueSet validation &mdash; rejects un-mappable shapes, escape-hatch contract violations, and clusters missing the upstream API.

When `GangScheduling=false`, behavior is unchanged.

### User Stories

1. **No third-party scheduler.** Platform operator on vanilla `kube-scheduler` wants gang admission so partial Grove deployments do not waste GPUs.
2. **Migration target.** Grove user migrates from KAI / Volcano to upstream once [KEP-4671][kep-4671] graduates, with no `PodCliqueSet` change.
3. **External owner** (Kueue, cross-PCS gang controller): see the escape hatch contract in [PodGang Mapping and Updates](#podgang-mapping-and-updates).

### Limitations / Risks &amp; Mitigations

| Risk | Mitigation |
|---|---|
| Upstream KEPs at very different maturity (full table in [Appendix](#upstream-kubernetes-keps)); load-bearing unreleased ones are [KEP-5832][kep-5832] (v1alpha2) and [KEP-6012][kep-6012] (hierarchical). | Start with the v1alpha1 API that ships in Kubernetes 1.35, keep the design compatible with the v1alpha2 direction, and track [LWS KEP-666][lws-844] / [JobSet #1068][jobset-1068] so divergence is intentional. |
| [KEP-5832 §Risks][kep-5832] plans a validating admission controller enforcing `Workload → PodGroup → Pod` creation order; pod-first creation would break. | Backend creates gang resources in `SyncPodGang` **before** `PodGang.Initialized=True`; Pods stay scheduling-gated. Owner is `PodGang`, not a Pod. |
| The selected upstream API may make gang shape fields immutable, including role membership or `MinCount`. | Grove rejects in-place updates to immutable gang shape and asks users to recreate the Grove workload; future phases may relax this only if the upstream API explicitly supports safe mutation. |

## Design Details

### Architecture Overview

| Surface | Trigger | `default-scheduler` responsibility |
|---|---|---|
| Initialization | Operator startup | Construct backend; unmarshal `KubeSchedulerConfig`; cache RESTMapper. |
| PodCliqueSet validation | PCS create/update webhook | API discovery; reject un-mappable shapes; reject escape-hatch contract violations. |
| Pod preparation | PodClique controller builds Pod | Always set `SchedulerName="default-scheduler"`. If gated on, also set the membership field. |
| PodGang sync | PodGang create / generation change | If gated on, reconcile `Workload` (+ standalone `PodGroup`s in Phase 2). Else no-op. |
| PodGang deletion | Delete event | No-op; cascading delete via owner reference. |

### OperatorConfiguration Extension

`default-scheduler` already exists ([GREP-375][grep-375]); opt in by:

```yaml
scheduler:
  defaultProfileName: default-scheduler
  profiles:
    - name: default-scheduler
      config:
        gangScheduling: true
```

`KubeSchedulerConfig.GangScheduling` already exists in `operator/api/config/v1alpha1/types.go`. No changes to `SchedulerProfile.Name`, its `+kubebuilder:validation:Enum`, or `manager.newBackendForProfile`.

### API Version Strategy

Phase 1 targets **v1alpha1** because it ships in Grove's Kubernetes 1.35 dependency baseline. Phase 2 moves to **v1alpha2 semantics** ([KEP-5832][kep-5832]; decoupled standalone `PodGroup`) once that API ships in a Grove-supported Kubernetes minor. The v1alpha2 direction still informs the design: it decouples `PodGroup` lifecycle from the `Workload`, LWS dropped v1alpha1 entirely, and `Pod.Spec.SchedulingGroup` is the upstream direction for the membership field. Grove only mutates fields that the selected upstream API marks mutable.

The implementation must move Grove's Kubernetes dependencies to a minor containing the selected Pod API fields (`WorkloadRef` for v1alpha1 or `SchedulingGroup` for v1alpha2), or explicitly use dynamic/unstructured patches for those fields. The Grove-facing surface (`gangScheduling: bool`) is unchanged between phases.

### PodGang Mapping and Updates

Flat: one `Workload` per `PodGang`, one `PodGroup` per Grove role.

| Grove source | Phase 1: v1alpha1 | Phase 2: v1alpha2 |
|---|---|---|
| `PodGang.{Name,Namespace}` | `Workload.{Name,Namespace}` | same |
| `PodGang.Spec.PodGroups[i].Name` | `Workload.Spec.PodGroups[i].Name` | `Workload.Spec.PodGroupTemplates[i].Name` and runtime `PodGroup.Name` |
| `PodGang.Spec.PodGroups[i].MinReplicas` | `Workload.Spec.PodGroups[i].Policy.Gang.MinCount` | `Workload.Spec.PodGroupTemplates[i].SchedulingPolicy.Gang.MinCount` and runtime `PodGroup.Spec.SchedulingPolicy.Gang.MinCount` |
| `PodGang.Spec.PodGroups[i]` | n/a | `PodGroup.Spec.PodGroupTemplateRef.Workload = {workloadName: <PodGang>, podGroupTemplateName: <PodGroupName>}` plus inline `SchedulingPolicy` |
| `PodGang` controller owner ref | `Workload` | `Workload` and each `PodGroup` |
| Pod label `grove.io/podgang` | `Pod.Spec.WorkloadRef.Name` | input to runtime `PodGroup.Name` |
| Pod label `grove.io/podclique` | `Pod.Spec.WorkloadRef.PodGroup` | input to runtime `PodGroup.Name` |

Pod-side membership is populated by `PreparePod` from labels in `operator/api/common/labels.go`. In Phase 2, `PreparePod` must be able to compute the runtime `PodGroup.Name` without a client lookup:

```text
runtimePodGroupName = "<podgang-name>-<podgroup-name>"
```

where `<podgang-name>` comes from `grove.io/podgang` and `<podgroup-name>` comes from `grove.io/podclique` (the Grove `PodGang.Spec.PodGroups[i].Name`). Phase 1 validation requires `PodGang.Spec.PodGroups[i].Name` to be valid for `Pod.Spec.WorkloadRef.PodGroup`; Phase 2 validation also rejects any generated runtime name that is not a valid upstream `PodGroup` object name or would collide within the namespace.

**Update semantics.** Grove follows the mutability rules of the selected upstream API. Gang shape changes are rejected once gang resources exist unless the upstream API explicitly marks the relevant fields mutable.

| Concern | Phase 1: v1alpha1 | Phase 2: v1alpha2 |
|---|---|---|
| `MinCount` change | Rejected after gang resources are created; recreate the Grove workload to change the gang admission threshold. | Same unless the released v1alpha2 API explicitly marks `MinCount` mutable. |
| Add / remove role | Rejected after gang resources are created; recreate the Grove workload to change the role set. | Same unless the released v1alpha2 API explicitly supports safe `PodGroup` add/remove for existing workloads. |
| Update safety | No automatic delete-recreate of immutable gang resources after creation. | Pods reference runtime `PodGroup`s, but Grove still follows the mutability rules of the released upstream API. |

**Escape hatch.** Mirroring [LWS KEP-666][lws-844], a `PodCliqueSet` may opt into gang scheduling but delegate `Workload` / `PodGroup` lifecycle to an external owner (Kueue, cross-PCS controller, future hierarchical wrapper). Contract:

- Pre-set `Pod.Spec.SchedulingGroup` / `WorkloadRef` in the template &rarr; `PreparePod` preserves it; the PodGang builder marks the generated `PodGang` with `grove.io/external-workload-managed="true"`; `SyncPodGang` skips Workload reconciliation when that label is present; the external owner creates the matching resources.
- Pre-set membership **without** `GangScheduling=true` &rarr; rejected, keeping the flag as the single source of truth.

No new Grove API; doubles as the alpha workaround for use cases the built-in mapping cannot yet express.

### Key Control Flow

1. Backend resolved via existing `grove.io/scheduler-name` label flow.
2. `GangScheduling=false` &rarr; return (current behavior).
3. Escape-hatch managed `PodGang` (label `grove.io/external-workload-managed="true"`) &rarr; skip Workload reconciliation.
4. Otherwise compute desired `Workload` (+ PodGroups under v1alpha2); create if absent. Owner is the `PodGang` (anchors cascading delete; matches the planned [KEP-5832][kep-5832] admission ordering).
5. Update: reconcile only fields that the selected upstream API marks mutable. Immutable gang shape changes are rejected and require recreating the Grove workload.
6. `OnPodGangDelete` is a no-op; cleanup via owner reference.

The implementation mirrors the proven `kai/backend.go` + `kai/topology.go` split:

```text
operator/internal/scheduler/kube/
├── backend.go        # existing; thin Backend interface impl
├── backend_test.go   # existing
├── workload.go       # new; buildWorkload, syncWorkload, RESTMapper helpers
└── workload_test.go  # new
```

`SyncPodGang` becomes a thin gate-check delegating to `workload.go`. A future `kube/topology.go` is reserved for [KEP-5732][kep-5732].

### Validation and API Discovery

`ValidatePodCliqueSet` rejects:

- Any `topologyConstraint` while the selected Workload API version has no topology surface; Grove's current topology API is a hard `packDomain` requirement, so accepting it would silently degrade. Points at [KEP-5732][kep-5732].
- Pre-set membership without `GangScheduling=true`.
- Missing upstream API.
- More than 8 Grove `PodGroup`s in a single `PodGang`, matching upstream `WorkloadMaxPodGroups`.
- Names that cannot be represented by the selected API version: Phase 1 `WorkloadRef.PodGroup` requires the Grove `PodGroup.Name` to satisfy the upstream DNS-label constraint; Phase 2 generated runtime `PodGroup.Name` must be a valid object name and non-colliding.
- Updates that would change gang shape fields the selected upstream API treats as immutable, such as role membership or `MinReplicas`.

`schedulerName` mismatch is already handled by the framework webhook ([GREP-375][grep-375]) and not duplicated here.

API discovery follows the pattern from [LWS KEP-666][lws-844]: the backend builds a cached RESTMapper at `Init()`, the webhook resolves `scheduling.k8s.io` GVKs via that cache, `NoMatchError` invalidates the cache and retries once, and a second miss rejects validation with an error naming the missing GVK and pointing at [KEP-4671][kep-4671] / [KEP-5832][kep-5832].

### Dependencies

This feature depends on a Kubernetes minor that serves the selected `scheduling.k8s.io` Workload API and has the corresponding scheduler feature gate enabled on `kube-apiserver` and `kube-scheduler`. Grove's Go dependencies must also include the selected Pod-side membership field (`WorkloadRef` for v1alpha1 or `SchedulingGroup` for v1alpha2), unless the implementation intentionally uses dynamic/unstructured patches.

Required RBAC:

| API group | Resource | Verbs | Purpose |
|---|---|---|---|
| `scheduling.k8s.io` | `workloads` | create, get, list, watch, patch, update, delete | `PodGang` &rarr; `Workload` reconciliation. |
| `scheduling.k8s.io` | `podgroups` | create, get, list, watch, patch, update, delete | Standalone `PodGroup` reconciliation under v1alpha2. |

No new metrics are required for alpha. Validation failures should be returned directly by the existing webhook path; reconciliation failures should surface through existing controller logs, events, and PodGang readiness behavior. A follow-up may add Workload-specific status conditions or metrics once the upstream API shape is stable enough to avoid churn.

### Test Plan

**Unit.** `PreparePod` sets `SchedulerName` unconditionally and membership only when gated on; preserves pre-set membership. `SyncPodGang` produces correct mapping and owner ref; reconciles mutable fields only; is bit-for-bit identical to current `main` when gated off; skips reconciliation in escape-hatch mode. `ValidatePodCliqueSet` rejects any topology constraint while the selected Workload API version lacks topology support, pre-set membership without the flag, missing upstream API, too many `PodGroup`s, names that cannot be represented by the selected upstream API, and updates to immutable gang shape. API discovery: missing GVK &rarr; clear error; later install of the API succeeds without restart.

**E2E.** Gated on: Pods carry the membership field; `Workload` (and `PodGroup`s) created; Pods gated until admitted; PCS delete cascades. Gated off: no `Workload` created; pass-through matches today. Escape hatch: pre-set `SchedulingGroup` &rarr; backend creates no `Workload` or `PodGroup`.

### Graduation Criteria

**Alpha.** `SyncPodGang` / `PreparePod` / `ValidatePodCliqueSet` implemented behind `GangScheduling`; unit tests cover mapping, gate-off identity, escape hatch, API discovery; Workload-specific code in `kube/workload.go`.

**Beta.** Upstream `Workload` / `PodGroup` APIs reach at least beta, or Grove explicitly accepts the risk of continuing to depend on alpha upstream APIs; E2E on at least one upstream Kubernetes minor that enables the Workload feature by default; follow-up GREP for hierarchical gang via [KEP-6012][kep-6012] drafted; user-facing migration guide from KAI / Volcano.

**GA.** `scheduling.k8s.io` Workload reaches v1 (or the project explicitly commits to its alpha state); multiple production reports.

### Implementation Phases (by Kubernetes minor)

Phases align Grove's rollout with the Kubernetes minor in which each upstream API ships. They are orthogonal to the [Graduation Criteria](#graduation-criteria) above: a phase fixes the upstream API surface Grove targets at that point, while the criteria fix the maturity bar Grove commits to within that surface. The user-facing `gangScheduling: bool` is unchanged across phases; the API version is an implementation detail.

#### Phase 1: Kubernetes 1.35 (v1alpha1)

- Baseline: `k8s.io/api` v0.35.x. `Workload`, `PodGroup`, `WorkloadSpec.PodGroups[i].Policy.Gang`, and `Pod.Spec.WorkloadRef` are all available.
- Mapping: flat &mdash; one `Workload` per `PodGang`, one `Workload.Spec.PodGroups[i]` per Grove role.
- Pod-side membership: `Pod.Spec.WorkloadRef.{Name,PodGroup}`.
- Update semantics: `Workload.Spec.PodGroups` is immutable; role or `MinReplicas` changes after gang resources are created are rejected and require recreating the Grove workload.
- Scope: `SyncPodGang` / `PreparePod` / `ValidatePodCliqueSet`, escape hatch, API discovery, RBAC, `WorkloadMaxPodGroups=8` validation.
- Explicitly deferred: topology, hierarchical gang, in-place gang shape mutation.

#### Phase 2: Kubernetes 1.36 (v1alpha2, conditional on KEP-5832 shipping)

- Prerequisite: a follow-up dependency bump to `k8s.io/api` v0.36.x. If 1.36 does not ship [KEP-5832][kep-5832], Phase 1 (v1alpha1) is carried forward until a minor that does.
- Switch to decoupled API: `Workload.Spec.PodGroupTemplates` plus standalone `PodGroup` resources keyed off the runtime name in [PodGang Mapping and Updates](#podgang-mapping-and-updates).
- Pod-side membership: `Pod.Spec.SchedulingGroup.PodGroupName` (replaces `WorkloadRef`).
- Update semantics: follow the released v1alpha2 mutability rules. If `MinCount` is mutable, Grove may patch the standalone `PodGroup`; otherwise the Phase 1 recreate-workload rule remains.

#### Phase 3: Kubernetes 1.37+ (hierarchical, topology, GA-track)

Each item is independently gated on the corresponding upstream KEP and can land as a separate sub-GREP / sub-PR.

- [KEP-6012 CompositePodGroup][kep-6012]: map `PCS -> PCSG -> PCSG replica -> role` onto `CompositePodGroup` layers.
- [KEP-5732 Workload-aware TAS][kep-5732]: introduce `operator/internal/scheduler/kube/topology.go`; convert today's topology-constraint rejection in [Validation and API Discovery](#validation-and-api-discovery) into actual constraint propagation onto the `Workload`.
- GA target: when `scheduling.k8s.io` graduates `Workload` / `PodGroup` to `v1`.

##### Hierarchical Gang via CompositePodGroup

Grove's `PodCliqueSet → PodCliquesScalingGroup → PCSG replicas → PodClique` is a near-literal match for [KEP-6012 CompositePodGroup][kep-6012]:

```text
CompositePodGroup <pcs-name>                        # PCS-wide gang
├─ CompositePodGroup <pcs-name>-<pcsg>              # MinCount = PCSG MinAvailable
│  ├─ CompositePodGroup <pcs-name>-<pcsg>-0         # PCSG replica 0
│  │   ├─ PodGroup <pcs-name>-<pcsg>-0-prefill      # leaf, MinCount = role.MinReplicas
│  │   └─ PodGroup <pcs-name>-<pcsg>-0-decode
│  └─ CompositePodGroup <pcs-name>-<pcsg>-1
└─ PodGroup <pcs-name>-<standalone-clique>
```

Future Work, not Phase 1 or Phase 2. The flat mapping is forward-compatible: a hierarchical controller can later wrap the per-`PodGang` `Workload`s in parent `CompositePodGroup`s without invalidating Pod-side state. This is the **primary differentiator** between Grove and LWS in the upstream gang ecosystem &mdash; LWS's flat replicas need only one tier; Grove naturally consumes two or three.

## Appendix

**Grove.** [GREP-375 Scheduler Backend Framework][grep-375] (this GREP plugs into it; its Beta criterion explicitly anticipates this work); tracking issue [#531][issue-531]; umbrella [#395][issue-395]; [PR #532][pr-532] (DRAFT, predates this GREP &mdash; decisions here supersede #532 where they differ); sibling in-flight backend GREPs: KAI ([PR #553][pr-553]), Volcano ([#571][issue-571]), Koordinator ([#537][issue-537]).

**Sibling integrations on the same upstream APIs.** [LWS PR #844 (KEP-666)][lws-844] is the most directly comparable design and the source of the lifecycle invariants, escape hatch, API discovery, and per-role gang policy patterns adopted here; [LWS KEP-766 DisaggregatedSet][lws-disaggregatedset] for per-role minAvailability; [JobSet PR #1068][jobset-1068] (sibling consumer, v1alpha1).

### Upstream Kubernetes KEPs

| KEP | Title | Status |
|---|---|---|
| [KEP-4671][kep-4671] | Gang Scheduling | v1alpha1 ships in Grove's current 1.35 dependency baseline; v1alpha2 redesign targets a later Kubernetes minor |
| [KEP-5558][kep-5558] | Workload API | Alpha (alongside 4671) |
| [KEP-5832][kep-5832] | Decouple PodGroup API (v1alpha2) | **Design / unreleased** |
| [KEP-6012][kep-6012] | CompositePodGroup (hierarchical) | **Design / unreleased** |
| [KEP-5732][kep-5732] | Workload-aware Topology-Aware Scheduling | **Design / unreleased** |

[grep-375]: ../375-scheduler-backend-framework/README.md
[issue-531]: https://github.com/ai-dynamo/grove/issues/531
[issue-395]: https://github.com/ai-dynamo/grove/issues/395
[pr-532]: https://github.com/ai-dynamo/grove/pull/532
[pr-553]: https://github.com/ai-dynamo/grove/pull/553
[issue-571]: https://github.com/ai-dynamo/grove/issues/571
[issue-537]: https://github.com/ai-dynamo/grove/issues/537
[kep-4671]: https://github.com/kubernetes/enhancements/tree/master/keps/sig-scheduling/4671-gang-scheduling
[kep-5558]: https://github.com/kubernetes/enhancements/pull/5558
[kep-5832]: https://github.com/kubernetes/enhancements/tree/master/keps/sig-scheduling/5832-decouple-podgroup-api
[kep-6012]: https://github.com/kubernetes/enhancements/issues/6012
[kep-5732]: https://github.com/kubernetes/enhancements/issues/5732
[lws-844]: https://github.com/kubernetes-sigs/lws/pull/844
[lws-disaggregatedset]: https://github.com/kubernetes-sigs/lws/tree/main/keps/766-DisaggregatedSet
[jobset-1068]: https://github.com/kubernetes-sigs/jobset/pull/1068
