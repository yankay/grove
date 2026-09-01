// Copyright 2026 The Grove Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package podgangmap

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/utils/component"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	"github.com/samber/lo"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

// buildBootstrapEntries builds the initial PodGangMap entries for a PCS replica from the PCS spec. It
// produces an anchor entry with every standalone PodClique and each PodCliqueScalingGroup's
// MinAvailable replicas, and a Tail entry for PodCliqueScalingGroup replicas above MinAvailable. Each
// entry reuses the epoch its existing PodGangs carry. It assigns a new epoch for a role that has no
// existing PodGang, or for every role when there is no anchor PodGang to reuse. It returns the entries
// and the scale-out epoch, which the caller uses to author the ScaleOut entry.
func buildBootstrapEntries(clk clock.Clock, pcs *grovecorev1alpha1.PodCliqueSet, existingPodGangs []groveschedulerv1alpha1.PodGang) ([]grovecorev1alpha1.PodGangEntry, string) {
	epochByRole := epochByRoleFromPodGangs(existingPodGangs)
	now := clk.Now().UnixNano()

	var anchorEpoch, tailEpoch, scaleOutEpoch int64
	if adopted, ok := epochByRole[grovecorev1alpha1.PodGangEntryRoleAnchor]; ok {
		anchorEpoch = adopted
		tailEpoch = epochOrDefault(epochByRole, grovecorev1alpha1.PodGangEntryRoleTail, anchorEpoch+1)
		scaleOutEpoch = epochOrDefault(epochByRole, grovecorev1alpha1.PodGangEntryRoleScaleOut, anchorEpoch+2)
	} else {
		anchorEpoch = now
		tailEpoch = now + 1
		scaleOutEpoch = now + 2
	}

	entries := make([]grovecorev1alpha1.PodGangEntry, 0, 3)
	entries = append(entries, buildBootstrapAnchorEntry(pcs, strconv.FormatInt(anchorEpoch, 10)))
	if tailEntry, ok := buildBootstrapTailEntry(pcs, strconv.FormatInt(tailEpoch, 10), strconv.FormatInt(anchorEpoch, 10)); ok {
		entries = append(entries, tailEntry)
	}

	return entries, strconv.FormatInt(scaleOutEpoch, 10)
}

// epochByRoleFromPodGangs returns the epoch each role's PodGangs carry, keyed by role, from the
// grove.io/podgang-role and grove.io/epoch labels. PodGangs of one role share an epoch, so the first
// value seen for a role is used. A role whose PodGangs carry no epoch, and a role with no PodGang, are
// omitted from the returned map.
func epochByRoleFromPodGangs(existingPodGangs []groveschedulerv1alpha1.PodGang) map[grovecorev1alpha1.PodGangEntryRole]int64 {
	byRole := make(map[grovecorev1alpha1.PodGangEntryRole]int64)
	for _, podGang := range existingPodGangs {
		labels := podGang.Labels
		epochStr, hasEpoch := labels[apicommon.LabelEpoch]
		role, hasRole := labels[apicommon.LabelPodGangRole]
		if !hasEpoch || !hasRole {
			continue
		}
		epoch, err := strconv.ParseInt(epochStr, 10, 64)
		if err != nil {
			continue
		}
		if _, seen := byRole[grovecorev1alpha1.PodGangEntryRole(role)]; !seen {
			byRole[grovecorev1alpha1.PodGangEntryRole(role)] = epoch
		}
	}
	return byRole
}

// epochOrDefault returns the epoch for the role, or defaultEpoch when the role is absent.
func epochOrDefault(epochByRole map[grovecorev1alpha1.PodGangEntryRole]int64, role grovecorev1alpha1.PodGangEntryRole, defaultEpoch int64) int64 {
	if epoch, ok := epochByRole[role]; ok {
		return epoch
	}
	return defaultEpoch
}

// buildBootstrapAnchorEntry returns the anchor entry carrying every active standalone PodClique's
// full Replicas count and every active PodCliqueScalingGroup's MinAvailable replicas (PCSG indices
// [0, MinAvailable)). Idle components are omitted. DependsOn is nil.
func buildBootstrapAnchorEntry(pcs *grovecorev1alpha1.PodCliqueSet, epoch string) grovecorev1alpha1.PodGangEntry {
	entry := newPodGangEntry(epoch, *pcs.Status.CurrentGenerationHash, nil)
	entry.Role = grovecorev1alpha1.PodGangEntryRoleAnchor
	entry.PodCliques = make(map[string]int32)
	for name, replicas := range componentutils.GetStandalonePCLQReplicasFromPCSTemplateSpec(pcs) {
		if replicas > 0 {
			entry.PodCliques[name] = replicas
		}
	}

	pcsgReplicas := componentutils.GetPCSGReplicasFromPCSTemplateSpec(pcs)
	pcsgMinAvailable := componentutils.GetPCSGMinAvailableFromPCSTemplateSpec(pcs)
	entry.PCSGReplicaIndices = make(map[string][]int32, len(pcsgMinAvailable))
	for name, minAvailable := range pcsgMinAvailable {
		if pcsgReplicas[name] == 0 {
			continue
		}
		entry.PCSGReplicaIndices[name] = lo.RangeFrom[int32](0, int(minAvailable))
	}
	return entry
}

// buildBootstrapTailEntry returns a single Tail entry for a fresh PCS replica. The entry aggregates,
// across all PodCliqueScalingGroups, each PodCliqueScalingGroup's replica indices above MinAvailable
// into a single entry. All PodCliqueScalingGroups and their indices share the same epoch value and
// depend on the anchor epoch. The PodGang materializer expands this entry into one PodGang per
// (PodCliqueScalingGroup, index). It returns false when no PodCliqueScalingGroup has replicas above
// MinAvailable.
func buildBootstrapTailEntry(pcs *grovecorev1alpha1.PodCliqueSet, epoch, anchorEpoch string) (grovecorev1alpha1.PodGangEntry, bool) {
	pcsgReplicaIndices := make(map[string][]int32)
	for _, pcsgConfig := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		replicas := *pcsgConfig.Replicas
		minAvailable := *pcsgConfig.MinAvailable
		if replicas <= minAvailable {
			continue
		}
		pcsgReplicaIndices[pcsgConfig.Name] = lo.RangeFrom(minAvailable, int(replicas-minAvailable))
	}
	if len(pcsgReplicaIndices) == 0 {
		return grovecorev1alpha1.PodGangEntry{}, false
	}
	entry := newPodGangEntry(epoch, *pcs.Status.CurrentGenerationHash, []string{anchorEpoch})
	entry.Role = grovecorev1alpha1.PodGangEntryRoleTail
	entry.PCSGReplicaIndices = pcsgReplicaIndices
	return entry, true
}

// reconcileEntries authors the desired PodGangMap entries for a PCS replica and returns them for create
// or patch. When no PodGangMap exists, or one exists with no entries, it starts from a fresh set of
// bootstrap entries, reusing the epoch the replica's existing PodGangs carry. An entry-less PodGangMap
// is bootstrapped the same way so a replica whose entries were all drained recovers instead of staying
// empty. When a PodGangMap with entries exists it starts from those entries, advancing them to the
// current generation hash unless a coherent update is in progress.
//
// Each entry keeps its identity (epoch, role, DependsOn) and its already-placed replica
// indices. Placement is not recomputed from the template. A template Replicas change does not reach an
// existing PodClique or PodCliqueScalingGroup, whose Spec.Replicas is set only at creation and changed
// only by external scaling.
//
// For each PodCliqueScalingGroup the count of its placed indices is diffed against its live
// Spec.Replicas. A scale-out appends new indices to the ScaleOut entry. A scale-in removes indices at
// or above the new replica count. Each standalone PodClique pod count on the anchor is set from its
// live Spec.Replicas. A ScaleOut entry is ensured before the diff so scale-out indices have a place to
// land, and empty entries are dropped afterward.
//
// NOTE: this reconstructs a single-anchor PodGangMap. After a coherent update the steady-state
// PodGangMap has more than one anchor entry and more than one tail entry, with a single ScaleOut entry.
// buildBootstrapEntries produces a single anchor, and each entry's DependsOn is held
// only on the PodGangMap and are not recoverable from the live PodGangs. Reconstructing a multi-anchor
// PodGangMap is deferred to the coherent update engine.
func reconcileEntries(clk clock.Clock,
	pcs *grovecorev1alpha1.PodCliqueSet,
	pcsReplicaIndex int,
	pgm *grovecorev1alpha1.PodGangMap,
	existingPodGangs []groveschedulerv1alpha1.PodGang,
	standalonePCLQs []grovecorev1alpha1.PodClique,
	pcsgs []grovecorev1alpha1.PodCliqueScalingGroup) ([]grovecorev1alpha1.PodGangEntry, error) {
	var (
		entries       []grovecorev1alpha1.PodGangEntry
		scaleOutEpoch string
	)
	if pgm == nil || len(pgm.Spec.Entries) == 0 {
		reconstructionSource := existingPodGangs
		if pgm != nil {
			// An existing entry-less PodGangMap represents an intentional idle state. Wake it with
			// fresh epochs rather than adopting a PodGang that may still be terminating.
			reconstructionSource = nil
		}
		entries, scaleOutEpoch = buildBootstrapEntries(clk, pcs, reconstructionSource)
	} else {
		// Deep-copy the existing entries so mutations here do not alias the snapshot's PodGangMap.
		entries = clonePodGangEntries(pgm.Spec.Entries)
		if shouldAdvanceEntriesGenerationHash(pcs, entries) {
			advanceEntriesGenerationHash(entries, *pcs.Status.CurrentGenerationHash)
		}
		if componentutils.IsCoherentStrategy(pcs) && !slices.ContainsFunc(entries, func(entry grovecorev1alpha1.PodGangEntry) bool {
			return entry.PodCliqueSetGenerationHash == *pcs.Status.CurrentGenerationHash
		}) {
			// Only the coherent planner may move a frozen replica to the new generation.
			return entries, nil
		}
	}

	epochs, err := newEpochAllocator(entries, clk.Now().UnixNano())
	if err != nil {
		return nil, err
	}
	currentHash := *pcs.Status.CurrentGenerationHash
	_, hasAnchor, err := componentutils.BaseAnchorEpoch(entries, &currentHash)
	if err != nil {
		return nil, err
	}
	if !hasAnchor {
		anchor := newPodGangEntry(epochs.allocate(), currentHash, nil)
		anchor.Role = grovecorev1alpha1.PodGangEntryRoleAnchor
		entries = append(entries, anchor)
	}
	if err = refreshStandalonePodCliqueCounts(entries, pcs, standalonePCLQs, pcsReplicaIndex); err != nil {
		return nil, err
	}
	entries, err = ensureScaleOutEntry(clk, entries, pcs, scaleOutEpoch)
	if err != nil {
		return nil, err
	}
	err = reconcilePCSGReplicaIndices(entries, pcs, pcsgs, pcsReplicaIndex)
	if err != nil {
		return nil, err
	}
	scaleOut, err := currentGenerationScaleOutEntry(entries, currentHash)
	if err != nil {
		return nil, err
	}
	// ensureScaleOutEntry may have allocated an epoch since the initial anchor was added.
	epochs, err = newEpochAllocator(entries, clk.Now().UnixNano())
	if err != nil {
		return nil, err
	}
	var previous []grovecorev1alpha1.PodGangEntry
	if pgm != nil {
		previous = pgm.Spec.Entries
	}
	if err := reconcileScaleOutEntryIdentity(scaleOut, entries, previous, currentHash, epochs); err != nil {
		return nil, err
	}
	return removeEmptyEntries(entries, currentHash), nil
}

func currentGenerationScaleOutEntry(entries []grovecorev1alpha1.PodGangEntry, currentHash string) (*grovecorev1alpha1.PodGangEntry, error) {
	var found *grovecorev1alpha1.PodGangEntry
	count := 0
	for i := range entries {
		entry := &entries[i]
		if entry.Role == grovecorev1alpha1.PodGangEntryRoleScaleOut && entry.PodCliqueSetGenerationHash == currentHash {
			found = entry
			count++
		}
	}
	if count > 1 {
		return nil, fmt.Errorf("current generation %q has %d ScaleOut entries", currentHash, count)
	}
	return found, nil
}

// refreshStandalonePodCliqueCounts reconciles live standalone PodClique counts across current-
// generation anchors. Waking restores membership to the highest-epoch anchor without changing its
// identity or the scheduling history used by dependent scaled PodGangs.
func refreshStandalonePodCliqueCounts(entries []grovecorev1alpha1.PodGangEntry,
	pcs *grovecorev1alpha1.PodCliqueSet,
	standalonePCLQs []grovecorev1alpha1.PodClique,
	pcsReplicaIndex int) error {
	currentHash := *pcs.Status.CurrentGenerationHash
	pcsRnr := apicommon.ResourceNameReplica{Name: pcs.Name, Replica: pcsReplicaIndex}
	anchorsHighestFirst, err := currentGenerationAnchorsByEpochDesc(entries, currentHash)
	if err != nil {
		return err
	}
	// Missing scale targets are initialized from the template on recreation. Author
	// their membership first so child creation does not depend on the child existing.
	desiredCounts := componentutils.GetStandalonePCLQReplicasFromPCSTemplateSpec(pcs)
	for _, standalonePCLQ := range standalonePCLQs {
		cliqueName := apicommon.ExtractPodCliqueNameFromStandalonePCLQFQN(standalonePCLQ.Name, pcsRnr)
		desiredCounts[cliqueName] = ptr.Deref(standalonePCLQ.Spec.Replicas, 1)
	}
	for cliqueName, replicas := range desiredCounts {
		if replicas == 0 {
			for _, anchor := range anchorsHighestFirst {
				delete(anchor.PodCliques, cliqueName)
			}
			continue
		}
		if len(anchorsHighestFirst) == 0 {
			return fmt.Errorf("current generation %q has no anchor for standalone PodClique %s", currentHash, cliqueName)
		}
		reconcileStandaloneCliqueCountAcrossAnchors(anchorsHighestFirst, cliqueName, replicas)
	}
	return nil
}

// reconcileStandaloneCliqueCountAcrossAnchors drives the clique's total pod count across the anchors
// (ordered highest epoch first) toward desiredTotal. A positive difference is added to the
// highest-epoch anchor. A negative difference drains the highest-epoch anchor first and
// moves to the next as each reaches zero.
func reconcileStandaloneCliqueCountAcrossAnchors(anchorsHighestFirst []*grovecorev1alpha1.PodGangEntry, cliqueName string, desiredTotal int32) {
	var currentTotal int32
	for _, anchor := range anchorsHighestFirst {
		currentTotal += anchor.PodCliques[cliqueName]
	}
	diff := desiredTotal - currentTotal
	switch {
	case diff > 0:
		if anchorsHighestFirst[0].PodCliques == nil {
			anchorsHighestFirst[0].PodCliques = make(map[string]int32)
		}
		anchorsHighestFirst[0].PodCliques[cliqueName] += diff
	case diff < 0:
		remaining := -diff
		for _, anchor := range anchorsHighestFirst {
			if remaining == 0 {
				return
			}
			take := min(remaining, anchor.PodCliques[cliqueName])
			if take == 0 {
				continue
			}
			anchor.PodCliques[cliqueName] -= take
			remaining -= take
		}
	}
}

// currentGenerationAnchorsByEpochDesc orders current-generation anchors by numeric epoch, newest first.
func currentGenerationAnchorsByEpochDesc(entries []grovecorev1alpha1.PodGangEntry, currentHash string) ([]*grovecorev1alpha1.PodGangEntry, error) {
	type anchorWithEpoch struct {
		entry *grovecorev1alpha1.PodGangEntry
		epochNanos int64
	}
	var paired []anchorWithEpoch
	for i := range entries {
		e := &entries[i]
		if e.Role != grovecorev1alpha1.PodGangEntryRoleAnchor || e.PodCliqueSetGenerationHash != currentHash {
			continue
		}
		epochNanos, err := entryEpochNanos(*e)
		if err != nil {
			return nil, err
		}
		paired = append(paired, anchorWithEpoch{entry: e, epochNanos: epochNanos})
	}
	slices.SortFunc(paired, func(a, b anchorWithEpoch) int {
		return cmp.Compare(b.epochNanos, a.epochNanos)
	})
	anchors := make([]*grovecorev1alpha1.PodGangEntry, len(paired))
	for i := range paired {
		anchors[i] = paired[i].entry
	}
	return anchors, nil
}

// reconcilePCSGReplicaIndices diffs each PodCliqueScalingGroup's replica-index count across all
// entries against its live Spec.Replicas. A wake restores the minimum replicas into the highest
// current-generation anchor; subsequent scale-out preserves already-placed replicas.
func reconcilePCSGReplicaIndices(entries []grovecorev1alpha1.PodGangEntry,
	pcs *grovecorev1alpha1.PodCliqueSet,
	pcsgs []grovecorev1alpha1.PodCliqueScalingGroup,
	pcsReplicaIndex int) error {
	rnr := apicommon.ResourceNameReplica{Name: pcs.Name, Replica: pcsReplicaIndex}
	currentHash := *pcs.Status.CurrentGenerationHash
	scaleOut, err := currentGenerationScaleOutEntry(entries, currentHash)
	if err != nil {
		return err
	}
	for _, pcsg := range pcsgs {
		pcsgConfigName, err := apicommon.ExtractScalingGroupNameFromPCSGFQN(pcsg.Name, rnr)
		if err != nil {
			return err
		}
		currentCount := countPCSGReplicaIndices(entries, pcsgConfigName)
		diff := int(pcsg.Spec.Replicas) - currentCount
		switch {
		case diff > 0:
			firstScaleOutIndex := int32(currentCount)
			if currentCount == 0 {
				anchors, err := currentGenerationAnchorsByEpochDesc(entries, currentHash)
				if err != nil {
					return err
				}
				if len(anchors) == 0 {
					return fmt.Errorf("current generation %q has no anchor for waking PodCliqueScalingGroup %s", currentHash, pcsg.Name)
				}
				minAvailable := ptr.Deref(pcsg.Spec.MinAvailable, 1)
				if pcsg.Spec.Replicas < minAvailable {
					return fmt.Errorf("PodCliqueScalingGroup %s has replicas %d below minAvailable %d", pcsg.Name, pcsg.Spec.Replicas, minAvailable)
				}
				if anchors[0].PCSGReplicaIndices == nil {
					anchors[0].PCSGReplicaIndices = make(map[string][]int32)
				}
				anchors[0].PCSGReplicaIndices[pcsgConfigName] = lo.RangeFrom[int32](0, int(minAvailable))
				firstScaleOutIndex = minAvailable
			}
			if firstScaleOutIndex == pcsg.Spec.Replicas {
				continue
			}
			if err := appendScaleOutReplicaIndices(scaleOut, currentHash, pcsgConfigName,
				lo.RangeFrom(firstScaleOutIndex, int(pcsg.Spec.Replicas-firstScaleOutIndex))); err != nil {
				return err
			}
		case diff < 0:
			removePCSGReplicaIndicesAtOrAbove(entries, pcsgConfigName, pcsg.Spec.Replicas)
		}
	}
	return nil
}

// reconcileScaleOutEntryIdentity gives an idle slot a fresh scheduling identity. Bootstrap and
// reconstruction retain their allocated/adopted epochs, and active entries keep their dependencies.
func reconcileScaleOutEntryIdentity(scaleOut *grovecorev1alpha1.PodGangEntry, entries, previous []grovecorev1alpha1.PodGangEntry, currentHash string, epochs *epochAllocator) error {
	if scaleOut == nil || componentutils.IsPodGangEntryEmpty(*scaleOut) {
		return nil
	}
	for _, old := range previous {
		if old.Role != scaleOut.Role || old.Epoch != scaleOut.Epoch {
			continue
		}
		if !componentutils.IsPodGangEntryEmpty(old) {
			return nil
		}
		scaleOut.Epoch = epochs.allocate()
		break
	}
	epoch, found, err := componentutils.BaseAnchorEpoch(entries, &currentHash)
	if err != nil {
		return err
	}
	if found {
		scaleOut.DependsOn = []string{epoch}
	}
	return nil
}

// countPCSGReplicaIndices returns the total number of the given PodCliqueScalingGroup's replica
// indices held across all entries.
func countPCSGReplicaIndices(entries []grovecorev1alpha1.PodGangEntry, pcsgConfigName string) int {
	count := 0
	for _, entry := range entries {
		count += len(entry.PCSGReplicaIndices[pcsgConfigName])
	}
	return count
}

// appendScaleOutReplicaIndices appends the given PodCliqueScalingGroup replica indices to the
// current-generation ScaleOut entry selected before membership reconciliation.
func appendScaleOutReplicaIndices(scaleOut *grovecorev1alpha1.PodGangEntry, currentHash, pcsgConfigName string, indices []int32) error {
	if scaleOut == nil {
		return fmt.Errorf("current generation %q has no ScaleOut entry", currentHash)
	}
	if scaleOut.PCSGReplicaIndices == nil {
		scaleOut.PCSGReplicaIndices = make(map[string][]int32)
	}
	scaleOut.PCSGReplicaIndices[pcsgConfigName] = append(scaleOut.PCSGReplicaIndices[pcsgConfigName], indices...)
	slices.Sort(scaleOut.PCSGReplicaIndices[pcsgConfigName])
	return nil
}

// removePCSGReplicaIndicesAtOrAbove removes the same replica identities as the PCSG controller,
// irrespective of the anchor or tail in which a coherent update placed them.
func removePCSGReplicaIndicesAtOrAbove(entries []grovecorev1alpha1.PodGangEntry, pcsgConfigName string, newReplicaCount int32) {
	for i := range entries {
		indices, ok := entries[i].PCSGReplicaIndices[pcsgConfigName]
		if !ok {
			continue
		}
		kept := slices.DeleteFunc(indices, func(index int32) bool { return index >= newReplicaCount })
		if len(kept) == 0 {
			delete(entries[i].PCSGReplicaIndices, pcsgConfigName)
		} else {
			entries[i].PCSGReplicaIndices[pcsgConfigName] = kept
		}
	}
}

// removeEmptyEntries follows coherent cleanup: empty anchors are removed, and an empty current
// ScaleOut slot survives only while a nonempty current anchor remains.
func removeEmptyEntries(entries []grovecorev1alpha1.PodGangEntry, currentGenerationHash string) []grovecorev1alpha1.PodGangEntry {
	keepScaleOut := hasNonEmptyCurrentGenerationAnchor(entries, currentGenerationHash)
	return slices.DeleteFunc(entries, func(entry grovecorev1alpha1.PodGangEntry) bool {
		if keepScaleOut && entry.PodCliqueSetGenerationHash == currentGenerationHash &&
			entry.Role == grovecorev1alpha1.PodGangEntryRoleScaleOut {
			return false
		}
		return componentutils.IsPodGangEntryEmpty(entry)
	})
}

func hasNonEmptyCurrentGenerationAnchor(entries []grovecorev1alpha1.PodGangEntry, currentHash string) bool {
	return slices.ContainsFunc(entries, func(entry grovecorev1alpha1.PodGangEntry) bool {
		return entry.Role == grovecorev1alpha1.PodGangEntryRoleAnchor &&
			entry.PodCliqueSetGenerationHash == currentHash && !componentutils.IsPodGangEntryEmpty(entry)
	})
}

// ensureScaleOutEntry creates the current generation's slot with a dependency on its base anchor.
func ensureScaleOutEntry(clk clock.Clock, entries []grovecorev1alpha1.PodGangEntry, pcs *grovecorev1alpha1.PodCliqueSet, scaleOutEpoch string) ([]grovecorev1alpha1.PodGangEntry, error) {
	if len(pcs.Spec.Template.PodCliqueScalingGroupConfigs) == 0 {
		return entries, nil
	}
	currentHash := *pcs.Status.CurrentGenerationHash
	scaleOut, err := currentGenerationScaleOutEntry(entries, currentHash)
	if err != nil || scaleOut != nil {
		return entries, err
	}
	anchorEpoch, found, err := componentutils.BaseAnchorEpoch(entries, &currentHash)
	if err != nil || !found {
		return entries, err
	}
	if scaleOutEpoch == "" {
		epochs, err := newEpochAllocator(entries, clk.Now().UnixNano())
		if err != nil {
			return nil, err
		}
		scaleOutEpoch = epochs.allocate()
	}
	entry := newPodGangEntry(scaleOutEpoch, currentHash, []string{anchorEpoch})
	entry.Role = grovecorev1alpha1.PodGangEntryRoleScaleOut
	return append(entries, entry), nil
}
