// /*
// Copyright 2025 The Grove Authors.
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
// */

package podclique

import (
	"testing"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// TestIsCurrentReplicaUpdateComplete_Issue315StaleRead documents the cross-controller half
// of the rolling-update deadlock reported in
// https://github.com/ai-dynamo/grove/issues/315.
//
// The PodClique-level `mutateCurrentHashes` gate (reconcilestatus.go:93) early-returns
// whenever the *previous* reconcile's UpdatedReplicas != Replicas. That gate fires
// exactly once on the reconcile that promotes the last NEW pod to Ready, because
// UpdatedReplicas is computed AFTER mutateCurrentHashes within the same reconcile loop.
// The PCLQ self-heals on its NEXT reconcile pass.
//
// The cross-controller question is: during the gated reconcile (and any window where
// the PodCliqueScalingGroup informer cache still serves the stale PCLQ), what does
// `isCurrentReplicaUpdateComplete` (rollingupdate.go:208) report? PCSG uses that
// answer to decide whether to advance to the next replica or to ContinueReconcileAndRequeue
// (rollingupdate.go:69-75). If it always returns false in the stale window, PCSG never
// advances, so:
//   - markRollingUpdateEnd is never called on the PCSG
//   - PCSG status never transitions to "update completed"
//   - The PodCliqueSet replica is never marked as updated either
//
// This test exhibits two PodClique snapshots that match the stale state captured in
// the diagnostic dump attached to issue #315 — `CurrentPodTemplateHash` / `CurrentPodCliqueSetGenerationHash`
// still pointing at the OLD hashes even though every Pod is NEW and Ready — and asserts
// that `isCurrentReplicaUpdateComplete` returns false. That is the lower jaw of the
// deadlock; the upper jaw is the PodClique gate above.
//
// The PodClique controller will eventually issue a status patch that flips both hashes
// to NEW (because UpdatedReplicas catches up on the gated reconcile and the next mutator
// pass clears the gate), and the watch event will wake the PCSG. But:
//   - if the PodClique status patch is elided because every other field already matches
//     (reconcilestatus.go:98-100 `equality.Semantic.DeepEqual` short-circuit), no event
//     is fired and the PCSG is never woken;
//   - or if the PodClique controller experiences any patch-failure / informer-lag /
//     restart in that window, the PCSG never sees the corrected status either.
//
// In both of those windows, this test's "false" result is the wedge that keeps the
// rolling update permanently stuck in the configuration shown by the issue dump.
func TestIsCurrentReplicaUpdateComplete_Issue315StaleRead(t *testing.T) {
	const (
		oldHash = "9f945b7b659c5f4cb8f"
		newHash = "574f6fb86d49bfdfbd9d"
		oldGen  = "f6c5fd949cb444c6558"
		newGen  = "c947687c4d79f9cfb59"
		pcsgIdx = 0
	)
	minAvail := int32(2)
	now := metav1.Now()

	pcs := &grovecorev1alpha1.PodCliqueSet{
		Status: grovecorev1alpha1.PodCliqueSetStatus{
			CurrentGenerationHash: ptr.To(newGen),
		},
	}
	pcsg := &grovecorev1alpha1.PodCliqueScalingGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "workload1-0-pcsg-a", Namespace: "default"},
		Spec: grovecorev1alpha1.PodCliqueScalingGroupSpec{
			Replicas:    1,
			MinAvailable: ptr.To(int32(1)),
		},
		Status: grovecorev1alpha1.PodCliqueScalingGroupStatus{
			UpdateProgress: &grovecorev1alpha1.PodCliqueScalingGroupUpdateProgress{
				ReadyReplicaIndicesSelectedToUpdate: &grovecorev1alpha1.PodCliqueScalingGroupReplicaUpdateProgress{
					Current: int32(pcsgIdx),
				},
				UpdateStartedAt: now,
			},
		},
	}

	// stalePCLQ: UpdateEndedAt is set (PCLQ controller's markRollingUpdateEnd ran), every
	// pod is NEW & Ready (UpdatedReplicas==Replicas==ReadyReplicas), but the gate held the
	// CurrentPod*Hash fields at OLD because UpdatedReplicas was stale on entry. This is the
	// EXACT shape of the diagnostic dump attached to the issue.
	stalePCLQ := grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "workload1-0-pcsg-a-0-pc-x",
			Namespace: "default",
		},
		Spec: grovecorev1alpha1.PodCliqueSpec{
			MinAvailable: &minAvail,
		},
		Status: grovecorev1alpha1.PodCliqueStatus{
			Replicas:                          2,
			ReadyReplicas:                     2,
			UpdatedReplicas:                   2,
			CurrentPodTemplateHash:            ptr.To(oldHash), // <-- gated, still OLD
			CurrentPodCliqueSetGenerationHash: ptr.To(oldGen),  // <-- gated, still OLD
			UpdateProgress: &grovecorev1alpha1.PodCliqueUpdateProgress{
				PodTemplateHash:            newHash,
				PodCliqueSetGenerationHash: newGen,
				UpdateStartedAt:            now,
				UpdateEndedAt:              ptr.To(now),
			},
		},
	}

	sc := &syncContext{
		pcs:  pcs,
		pcsg: pcsg,
		existingPCLQs: []grovecorev1alpha1.PodClique{stalePCLQ},
		expectedPCLQFQNsPerPCSGReplica: map[int][]string{
			pcsgIdx: {stalePCLQ.Name},
		},
		expectedPCLQPodTemplateHashMap: map[string]string{
			stalePCLQ.Name: newHash,
		},
	}

	// Wire the PCSG label-grouping helper that GroupPCLQsByPCSGReplicaIndex relies on.
	// Mirror what the production code expects: the PCLQ carries the PCSG replica-index label.
	sc.existingPCLQs[0].Labels = map[string]string{
		"grove.io/podcliquescalinggroup-replica-index": "0",
	}

	got := isCurrentReplicaUpdateComplete(sc)
	assert.False(t, got,
		"isCurrentReplicaUpdateComplete must return false while CurrentPodTemplateHash / "+
			"CurrentPodCliqueSetGenerationHash still point at the OLD hashes. This is the "+
			"PCSG-side wedge of the issue #315 deadlock: PCLQ gate held the hashes at OLD "+
			"for one reconcile, and PCSG must report 'not complete' so it can requeue and "+
			"wait for the PCLQ self-heal. The danger is that no event ever wakes either "+
			"controller (PCLQ status patch elision, watch loss, etc.), at which point this "+
			"'false' becomes a permanent state.")
}
