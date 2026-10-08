# Coherent Hibernation Prototype

Updated October 8, 2026. This is a feasibility prototype, not a declaration of production support for scale-to-zero on every scheduler.

## Design References

- [GREP-0677: Scale-to-Zero Component Hibernation](https://github.com/ai-dynamo/grove/blob/839522c12a0ecd7de4190ff12985a78085ea352f/docs/proposals/0677-scale-to-zero-component-hibernation/README.md), the proposal revision used for this adaptation.
- [Coherent update engine](https://github.com/ai-dynamo/grove/pull/831), included in upstream main at `78231d747d1dfe877dadf0bd9c6c261293b245a1`.
- [GREP-393: Coherent Rolling Updates](../proposals/393-coherent-rolling-updates/README.md).

## Implemented Behavior

Standalone PodCliques and PodCliqueScalingGroups can idle at zero without lowering their positive `minAvailable`. Omitted PodClique replicas default to one; explicit zero is preserved. Omitted PodClique `minAvailable` defaults to `max(1, replicas)`. Positive below-minimum requests are rejected, including through `/scale`; unrelated updates of legacy objects remain possible.

Idle components contribute no required PodGroups. A waking PCSG places its first `minAvailable` replicas together in a current-generation anchor; extra replicas use ScaleOut. New extras wait for the live minimum replicas to be scheduled in their committed gangs. Historical `LastScheduled` alone is insufficient.

The coherent planner remains the authority for replacement placement and order. Every PCS replica is protected against component scaling during a coherent update, including replicas not yet selected for replacement. Frozen replicas retain their generation and running revision. PCSG controllers execute committed placement rather than running an independent rolling-recreate sequence.

Standalone Pods can span multiple anchors, so their PodClique has no gang label. Standalone placement uses numeric epoch ordering. PCSG scale-in removes replica indices at or above the new count, regardless of where a coherent update placed them. Startup dependencies use actual PodGangMap membership, including PCSG indices beyond the bootstrap minimum, and exclude idle or unrelated members.

Reconciliation derives existence from the object fetched for create-or-patch, not an earlier list. This preserves an accepted zero target when the list is stale. Gang recovery preserves mutable scale targets and uses optimistic locking to fence stale recovery transitions.

## Differences From the Proposal

The referenced GREP predates removal of `AnchorIndex` and retains empty anchor slots. This prototype follows the coherent engine instead: numeric epochs order anchors, empty anchors are removed, and an empty ScaleOut slot survives only while a current-generation anchor remains nonempty. A wake reuses a surviving anchor; after all entries disappear, it creates a new scheduling identity. It does not retain an empty anchor merely to preserve its epoch.

The referenced GREP proposes disabling KAI stale-gang termination on Grove-managed PodGroups. This prototype does not do so, following the [maintainer's review](https://github.com/ai-dynamo/grove/pull/686#discussion_r4070233807). It removes the earlier per-PodGroup `-1s` override and leaves the native policy enabled. The KAI v0.17.0 CRD preflight remains necessary to remove that previously written field.

Observed replica counters describe remaining Pods during deletion and converge to zero after they drain. Idle availability does not require fabricated Ready replicas. API pointer consistency beyond PodClique and compatibility of Grove's HPA-producing `scaleConfig` remain proposal review topics; this change does not redesign those APIs.

## Backend Limits

| Backend | Prototype behavior | Remaining qualification |
| --- | --- | --- |
| KAI v0.17.0 | Synchronizes complete subgroup policies and leaves native termination enabled. | Router continuity under a blocked wake must be retested without a negative staleness override. |
| Volcano | Synchronizes `MinMember` and complete subgroup policies together. | API equality is not scheduler acknowledgment. Strict safety with stale scheduler policy still needs an upstream mechanism and retesting. |
| Kubernetes WAS | No native hierarchical implementation is added to Grove's kube backend. | A native mapping, mutable policy integration, and Grove-backed lifecycle tests are still required. |

The [September 24 native experiments](native-scheduler-capabilities-summary-20260924.md) remain historical evidence for their exact binaries and configurations. KAI passes with termination disabled are not evidence for the current configuration. The Volcano eventual-adoption result does not establish strict admission safety. A merged upstream API change alone does not qualify a released scheduler or a Grove integration.

## Validation Scope

Focused tests cover idle defaulting and admission, current wake quorum gating, multi-anchor counts, replica identity during scale-in, frozen coherent generations, startup ordering after coherent placement, stale-list preservation of zero targets, and repeated sleep/wake reconciliation. Envtest exercises the generated CRDs and webhook behavior against a local Kubernetes API server.

Local validation on October 8, 2026 passed `make validate`, full repository `make test` including the operator race target and Kubernetes 1.35.0 envtest, compilation of all E2E packages with the `e2e,soak` tags, 45 E2E helper tests, 37 native-probe Python tests, and the KAI infrastructure dependency test. A 30-second hibernation fuzz run passed 150,784 executions. The broader infrastructure Python suite has three unrelated configuration assertion failures that also reproduce in the unmodified upstream checkout: its expected worker counts are 30 or 5, while the checked-in configurations set zero.

Native scheduler experiments and upgrade E2E remain separate qualification steps. Do not interpret unit tests, envtest, or compilation of the E2E suite as proof of router continuity, scheduler acknowledgment, or latest-release-to-prototype serving continuity.
