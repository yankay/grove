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
	"time"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// TestMutateUpdatedReplica tests the mutateUpdatedReplica function across different PodClique states
func TestMutateUpdatedReplica(t *testing.T) {
	tests := []struct {
		name                    string
		pclq                    *grovecorev1alpha1.PodClique
		existingPods            []*corev1.Pod
		expectedUpdatedReplicas int32
	}{
		{
			name: "rolling update in progress - count pods with new hash",
			pclq: &grovecorev1alpha1.PodClique{
				Status: grovecorev1alpha1.PodCliqueStatus{
					UpdateProgress: &grovecorev1alpha1.PodCliqueUpdateProgress{
						PodTemplateHash: "new-hash-v2",
						UpdateStartedAt: metav1.Time{Time: time.Now()},
						UpdateEndedAt:   nil, // Still in progress
					},
					CurrentPodTemplateHash: ptr.To("old-hash-v1"),
				},
			},
			existingPods: []*corev1.Pod{
				// 3 pods updated to new version
				createPodWithHash("pod-1", "new-hash-v2"),
				createPodWithHash("pod-2", "new-hash-v2"),
				createPodWithHash("pod-3", "new-hash-v2"),
				// 7 pods still on old version
				createPodWithHash("pod-4", "old-hash-v1"),
				createPodWithHash("pod-5", "old-hash-v1"),
				createPodWithHash("pod-6", "old-hash-v1"),
				createPodWithHash("pod-7", "old-hash-v1"),
				createPodWithHash("pod-8", "old-hash-v1"),
				createPodWithHash("pod-9", "old-hash-v1"),
				createPodWithHash("pod-10", "old-hash-v1"),
			},
			expectedUpdatedReplicas: 3, // Only the 3 pods with new hash
		},
		{
			name: "update just completed - use RollingUpdateProgress hash (edge case)",
			pclq: &grovecorev1alpha1.PodClique{
				Status: grovecorev1alpha1.PodCliqueStatus{
					UpdateProgress: &grovecorev1alpha1.PodCliqueUpdateProgress{
						PodTemplateHash: "new-hash-v2",
						UpdateStartedAt: metav1.Time{Time: time.Now().Add(-5 * time.Minute)},
						UpdateEndedAt:   &metav1.Time{Time: time.Now()}, // Just completed
					},
					// CurrentPodTemplateHash not updated yet - still has old hash
					CurrentPodTemplateHash: ptr.To("old-hash-v1"),
				},
			},
			existingPods: []*corev1.Pod{
				// All pods updated to new version
				createPodWithHash("pod-1", "new-hash-v2"),
				createPodWithHash("pod-2", "new-hash-v2"),
				createPodWithHash("pod-3", "new-hash-v2"),
				createPodWithHash("pod-4", "new-hash-v2"),
				createPodWithHash("pod-5", "new-hash-v2"),
				createPodWithHash("pod-6", "new-hash-v2"),
				createPodWithHash("pod-7", "new-hash-v2"),
				createPodWithHash("pod-8", "new-hash-v2"),
				createPodWithHash("pod-9", "new-hash-v2"),
				createPodWithHash("pod-10", "new-hash-v2"),
			},
			expectedUpdatedReplicas: 10, // All 10 pods should be counted as updated
		},
		{
			name: "steady state - no rolling update",
			pclq: &grovecorev1alpha1.PodClique{
				Status: grovecorev1alpha1.PodCliqueStatus{
					UpdateProgress:         nil, // No rolling update
					CurrentPodTemplateHash: ptr.To("stable-hash"),
				},
			},
			existingPods: []*corev1.Pod{
				createPodWithHash("pod-1", "stable-hash"),
				createPodWithHash("pod-2", "stable-hash"),
				createPodWithHash("pod-3", "stable-hash"),
				createPodWithHash("pod-4", "stable-hash"),
				createPodWithHash("pod-5", "stable-hash"),
			},
			expectedUpdatedReplicas: 5, // All pods match current hash
		},
		{
			name: "never reconciled - empty hash",
			pclq: &grovecorev1alpha1.PodClique{
				Status: grovecorev1alpha1.PodCliqueStatus{
					UpdateProgress:         nil,
					CurrentPodTemplateHash: nil, // Never set
				},
			},
			existingPods: []*corev1.Pod{
				createPodWithHash("pod-1", "some-hash"),
				createPodWithHash("pod-2", "some-hash"),
			},
			expectedUpdatedReplicas: 0, // Should not count any pods when hash is unknown - no rolling update
		},
		{
			name: "mixed state - some pods match, some don't",
			pclq: &grovecorev1alpha1.PodClique{
				Status: grovecorev1alpha1.PodCliqueStatus{
					UpdateProgress:         nil,
					CurrentPodTemplateHash: ptr.To("current-hash"),
				},
			},
			existingPods: []*corev1.Pod{
				createPodWithHash("pod-1", "current-hash"),
				createPodWithHash("pod-2", "current-hash"),
				createPodWithHash("pod-3", "old-hash"),
				createPodWithHash("pod-4", "old-hash"),
				createPodWithHash("pod-5", "old-hash"),
			},
			expectedUpdatedReplicas: 2, // Only pods with current hash
		},
		{
			name: "no pods exist",
			pclq: &grovecorev1alpha1.PodClique{
				Status: grovecorev1alpha1.PodCliqueStatus{
					CurrentPodTemplateHash: ptr.To("some-hash"),
				},
			},
			existingPods:            []*corev1.Pod{},
			expectedUpdatedReplicas: 0,
		},
		{
			name: "rolling update progress exists but pods have different hash",
			pclq: &grovecorev1alpha1.PodClique{
				Status: grovecorev1alpha1.PodCliqueStatus{
					UpdateProgress: &grovecorev1alpha1.PodCliqueUpdateProgress{
						PodTemplateHash: "target-hash",
						UpdateStartedAt: metav1.Time{Time: time.Now()},
					},
					CurrentPodTemplateHash: ptr.To("old-hash"),
				},
			},
			existingPods: []*corev1.Pod{
				createPodWithHash("pod-1", "completely-different-hash"),
				createPodWithHash("pod-2", "another-hash"),
			},
			expectedUpdatedReplicas: 0, // None match the target hash
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call the function
			mutateUpdatedReplica(tt.pclq, tt.existingPods)

			// Assert the result
			assert.Equal(t, tt.expectedUpdatedReplicas, tt.pclq.Status.UpdatedReplicas,
				"UpdatedReplicas should match expected value")
		})
	}
}

// createPodWithHash creates a test pod with the specified template hash label
func createPodWithHash(name string, templateHash string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				apicommon.LabelPodTemplateHash: templateHash,
			},
		},
	}
}

// TestMirrorUpdateProgressToRollingUpdateProgressPCLQ tests the mirrorUpdateProgressToRollingUpdateProgress function for PodClique
func TestMirrorUpdateProgressToRollingUpdateProgressPCLQ(t *testing.T) {
	updateStartedAt := metav1.Now()
	updateEndedAt := metav1.NewTime(updateStartedAt.Add(1))
	tests := []struct {
		name                          string
		pclq                          *grovecorev1alpha1.PodClique
		expectedRollingUpdateProgress *grovecorev1alpha1.PodCliqueRollingUpdateProgress
	}{
		{
			name: "nil UpdateProgress results in nil RollingUpdateProgress",
			pclq: &grovecorev1alpha1.PodClique{
				Status: grovecorev1alpha1.PodCliqueStatus{
					UpdateProgress: nil,
				},
			},
			expectedRollingUpdateProgress: nil,
		},
		{
			name: "UpdateProgress with no ReadyPodsSelectedToUpdate",
			pclq: &grovecorev1alpha1.PodClique{
				Status: grovecorev1alpha1.PodCliqueStatus{
					UpdateProgress: &grovecorev1alpha1.PodCliqueUpdateProgress{
						UpdateStartedAt:            updateStartedAt,
						PodCliqueSetGenerationHash: "gen-hash-123",
						PodTemplateHash:            "pod-hash-456",
					},
				},
			},
			expectedRollingUpdateProgress: &grovecorev1alpha1.PodCliqueRollingUpdateProgress{
				UpdateStartedAt:            updateStartedAt,
				PodCliqueSetGenerationHash: "gen-hash-123",
				PodTemplateHash:            "pod-hash-456",
			},
		},
		{
			name: "UpdateProgress with ReadyPodsSelectedToUpdate",
			pclq: &grovecorev1alpha1.PodClique{
				Status: grovecorev1alpha1.PodCliqueStatus{
					UpdateProgress: &grovecorev1alpha1.PodCliqueUpdateProgress{
						UpdateStartedAt:            updateStartedAt,
						UpdateEndedAt:              ptr.To(updateEndedAt),
						PodCliqueSetGenerationHash: "gen-hash-789",
						PodTemplateHash:            "pod-hash-012",
						ReadyPodsSelectedToUpdate: &grovecorev1alpha1.PodsSelectedToUpdate{
							Current:   "pod-1",
							Completed: []string{"pod-0"},
						},
					},
				},
			},
			expectedRollingUpdateProgress: &grovecorev1alpha1.PodCliqueRollingUpdateProgress{
				UpdateStartedAt:            updateStartedAt,
				UpdateEndedAt:              ptr.To(updateEndedAt),
				PodCliqueSetGenerationHash: "gen-hash-789",
				PodTemplateHash:            "pod-hash-012",
				ReadyPodsSelectedToUpdate: &grovecorev1alpha1.PodsSelectedToUpdate{
					Current:   "pod-1",
					Completed: []string{"pod-0"},
				},
			},
		},
		{
			name: "clears existing RollingUpdateProgress when UpdateProgress is nil",
			pclq: &grovecorev1alpha1.PodClique{
				Status: grovecorev1alpha1.PodCliqueStatus{
					UpdateProgress: nil,
					RollingUpdateProgress: &grovecorev1alpha1.PodCliqueRollingUpdateProgress{
						UpdateStartedAt:            updateStartedAt,
						UpdateEndedAt:              ptr.To(updateEndedAt),
						PodCliqueSetGenerationHash: "old-gen-hash",
						PodTemplateHash:            "old-pod-hash",
						ReadyPodsSelectedToUpdate: &grovecorev1alpha1.PodsSelectedToUpdate{
							Current:   "pod-1",
							Completed: []string{"pod-0"},
						},
					},
				},
			},
			expectedRollingUpdateProgress: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call the function
			mirrorUpdateProgressToRollingUpdateProgress(tt.pclq)

			// Assert the result
			if tt.expectedRollingUpdateProgress == nil {
				assert.Nil(t, tt.pclq.Status.RollingUpdateProgress,
					"RollingUpdateProgress should be nil")
			} else {
				assert.NotNil(t, tt.pclq.Status.RollingUpdateProgress,
					"RollingUpdateProgress should not be nil")
				assert.Equal(t, tt.expectedRollingUpdateProgress.UpdateStartedAt,
					tt.pclq.Status.RollingUpdateProgress.UpdateStartedAt,
					"UpdateStartedAt should match")
				assert.Equal(t, tt.expectedRollingUpdateProgress.UpdateEndedAt,
					tt.pclq.Status.RollingUpdateProgress.UpdateEndedAt,
					"UpdateEndedAt should match")
				assert.Equal(t, tt.expectedRollingUpdateProgress.PodCliqueSetGenerationHash,
					tt.pclq.Status.RollingUpdateProgress.PodCliqueSetGenerationHash,
					"PodCliqueSetGenerationHash should match")
				assert.Equal(t, tt.expectedRollingUpdateProgress.PodTemplateHash,
					tt.pclq.Status.RollingUpdateProgress.PodTemplateHash,
					"PodTemplateHash should match")

				// Check ReadyPodsSelectedToUpdate
				if tt.expectedRollingUpdateProgress.ReadyPodsSelectedToUpdate == nil {
					assert.Nil(t, tt.pclq.Status.RollingUpdateProgress.ReadyPodsSelectedToUpdate,
						"ReadyPodsSelectedToUpdate should be nil")
				} else {
					assert.NotNil(t, tt.pclq.Status.RollingUpdateProgress.ReadyPodsSelectedToUpdate,
						"ReadyPodsSelectedToUpdate should not be nil")
					assert.Equal(t, tt.expectedRollingUpdateProgress.ReadyPodsSelectedToUpdate.Current,
						tt.pclq.Status.RollingUpdateProgress.ReadyPodsSelectedToUpdate.Current,
						"Current pod should match")
					assert.Equal(t, tt.expectedRollingUpdateProgress.ReadyPodsSelectedToUpdate.Completed,
						tt.pclq.Status.RollingUpdateProgress.ReadyPodsSelectedToUpdate.Completed,
						"Completed pods should match")
				}
			}
		})
	}
}

// runStatusMutationChain replays the subset of reconcileStatus that issue #315 cares about,
// in the same order as reconcileStatus(): mutateCurrentHashes -> mutateReplicas -> mutateUpdatedReplica.
// It is a unit-level harness — no client, no API server — driven by the caller-provided pclq + pods.
//
// Note: mutateCurrentHashes only dereferences pcs in its UpdateProgress == nil branch
// (via GetExpectedPCLQPodTemplateHash). All scenarios below set UpdateProgress != nil, so passing a
// zero-valued PodCliqueSet is safe.
func runStatusMutationChain(t *testing.T, pclq *grovecorev1alpha1.PodClique, pods []*corev1.Pod) {
	t.Helper()
	pcs := &grovecorev1alpha1.PodCliqueSet{}
	require.NoError(t, mutateCurrentHashes(logr.Discard(), pcs, pclq))
	// mutateReplicas needs a podCategories map. The deadlock scenario does not depend on
	// PodReady / Scheduled counters — only Replicas matters — so we synthesize an empty map
	// and set Replicas directly to mirror what mutateReplicas would compute when none of the
	// pods are categorized as Terminating.
	pclq.Status.Replicas = int32(len(pods))
	mutateUpdatedReplica(pclq, pods)
}

// TestRollingUpdateDeadlock_Issue315_SinglePassGate is a regression test for
// https://github.com/ai-dynamo/grove/issues/315.
//
// Scenario reproduced: markRollingUpdateEnd() has just set UpdateEndedAt because no
// old-hash pods remain, but at that instant UpdatedReplicas (computed last reconcile)
// still trails Replicas — a brand-new replacement pod has not become Ready yet, so the
// previous reconcile recorded UpdatedReplicas < Replicas. On the next reconcile,
// mutateCurrentHashes runs first and reads those stale counters; because
// UpdatedReplicas != Replicas, it early-returns at reconcilestatus.go:112-114 and
// refuses to advance CurrentPodTemplateHash to the new hash, even though the update is
// effectively done.
//
// This is the precise gating behavior the issue calls out. The test pins it so that any
// change to the gate condition trips a test failure.
func TestRollingUpdateDeadlock_Issue315_SinglePassGate(t *testing.T) {
	const (
		oldHash = "old-hash-v1"
		newHash = "new-hash-v2"
		oldGen  = "old-gen"
		newGen  = "new-gen"
	)
	now := metav1.Now()
	startedAt := metav1.NewTime(now.Add(-time.Minute))

	pclq := &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: "pclq-a", Namespace: "default"},
		Status: grovecorev1alpha1.PodCliqueStatus{
			// Stale counters from the previous reconcile pass: at the moment
			// markRollingUpdateEnd patched UpdateEndedAt, only one of the two replacement
			// pods had reached Ready, so the previous reconcile wrote UpdatedReplicas=1.
			Replicas:                          2,
			UpdatedReplicas:                   1,
			CurrentPodTemplateHash:            ptr.To(oldHash),
			CurrentPodCliqueSetGenerationHash: ptr.To(oldGen),
			UpdateProgress: &grovecorev1alpha1.PodCliqueUpdateProgress{
				PodTemplateHash:            newHash,
				PodCliqueSetGenerationHash: newGen,
				UpdateStartedAt:            startedAt,
				// UpdateEndedAt is set: markRollingUpdateEnd just ran because no old-hash
				// pods remain. IsPCLQAutoUpdateInProgress() now returns false.
				UpdateEndedAt: ptr.To(now),
			},
		},
	}
	// The world contains two new-hash pods, but for the gate-only test we don't need to
	// drive mutateUpdatedReplica — we just want to assert that mutateCurrentHashes
	// early-returns based on stale Replicas/UpdatedReplicas. Keeping the pods inline
	// as documentation of the scenario.
	_ = []*corev1.Pod{
		createPodWithHash("pod-1", newHash),
		createPodWithHash("pod-2", newHash),
	}

	pcs := &grovecorev1alpha1.PodCliqueSet{}
	require.NoError(t, mutateCurrentHashes(logr.Discard(), pcs, pclq))

	// The deadlock gate: mutateCurrentHashes saw UpdatedReplicas (1) != Replicas (2)
	// and bailed out. CurrentPodTemplateHash is NOT advanced to newHash this pass.
	require.NotNil(t, pclq.Status.CurrentPodTemplateHash)
	assert.Equal(t, oldHash, *pclq.Status.CurrentPodTemplateHash,
		"issue #315: mutateCurrentHashes early-returns because of stale UpdatedReplicas "+
			"(reconcilestatus.go gate), leaving CurrentPodTemplateHash on the old value "+
			"even though UpdateEndedAt is set and all live pods carry the new hash")
	require.NotNil(t, pclq.Status.CurrentPodCliqueSetGenerationHash)
	assert.Equal(t, oldGen, *pclq.Status.CurrentPodCliqueSetGenerationHash,
		"CurrentPodCliqueSetGenerationHash is gated by the same early return")
}

// TestRollingUpdateDeadlock_Issue315_HEADSelfHeals demonstrates that on current HEAD the
// stale-counter anomaly is transient: even though mutateCurrentHashes bails out on the
// first reconcile pass (see TestRollingUpdateDeadlock_Issue315_SinglePassGate),
// mutateUpdatedReplica recomputes against UpdateProgress.PodTemplateHash (the NEW hash)
// rather than CurrentPodTemplateHash (still the OLD hash). That recomputation makes
// UpdatedReplicas == Replicas at the end of pass 1, so on pass 2 the gate falls through
// and CurrentPodTemplateHash finally moves to the new hash.
//
// In other words: the *literal* permanent-deadlock mechanism described in issue #315
// (mutateUpdatedReplica counting against CurrentPodTemplateHash and producing 0) does
// not reproduce on HEAD, because reconcilestatus.go:148-149 short-circuits to the
// UpdateProgress hash when UpdateProgress != nil. This test documents that behavior so
// any future regression that re-binds mutateUpdatedReplica to CurrentPodTemplateHash —
// recreating the original deadlock — will be caught.
func TestRollingUpdateDeadlock_Issue315_HEADSelfHeals(t *testing.T) {
	const (
		oldHash = "old-hash-v1"
		newHash = "new-hash-v2"
		oldGen  = "old-gen"
		newGen  = "new-gen"
	)
	now := metav1.Now()
	startedAt := metav1.NewTime(now.Add(-time.Minute))

	pclq := &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: "pclq-a", Namespace: "default"},
		Status: grovecorev1alpha1.PodCliqueStatus{
			Replicas:                          2,
			UpdatedReplicas:                   1, // stale
			CurrentPodTemplateHash:            ptr.To(oldHash),
			CurrentPodCliqueSetGenerationHash: ptr.To(oldGen),
			UpdateProgress: &grovecorev1alpha1.PodCliqueUpdateProgress{
				PodTemplateHash:            newHash,
				PodCliqueSetGenerationHash: newGen,
				UpdateStartedAt:            startedAt,
				UpdateEndedAt:              ptr.To(now),
			},
		},
	}
	pods := []*corev1.Pod{
		createPodWithHash("pod-1", newHash),
		createPodWithHash("pod-2", newHash),
	}

	// --- Pass 1: gated by stale counters ---
	runStatusMutationChain(t, pclq, pods)

	// CurrentPodTemplateHash did NOT advance this pass (the gate held).
	require.NotNil(t, pclq.Status.CurrentPodTemplateHash)
	assert.Equal(t, oldHash, *pclq.Status.CurrentPodTemplateHash,
		"pass 1: gate holds, CurrentPodTemplateHash still on old hash")

	// But mutateUpdatedReplica saw UpdateProgress != nil and counted pods against the
	// NEW hash, so UpdatedReplicas is now 2. This is the HEAD-only behavior introduced
	// by reconcilestatus.go:148-149 that breaks the closed feedback loop the issue
	// describes. If this assertion ever flips back to 0, issue #315's permanent
	// deadlock has been reintroduced.
	assert.Equal(t, int32(2), pclq.Status.UpdatedReplicas,
		"pass 1: mutateUpdatedReplica counts against UpdateProgress.PodTemplateHash, "+
			"so the stale count self-corrects in the same reconcile pass")

	// --- Pass 2: with counters now fresh, the gate falls through ---
	runStatusMutationChain(t, pclq, pods)

	require.NotNil(t, pclq.Status.CurrentPodTemplateHash)
	assert.Equal(t, newHash, *pclq.Status.CurrentPodTemplateHash,
		"pass 2: UpdatedReplicas (2) == Replicas (2), so mutateCurrentHashes takes the "+
			"IsLastPCLQUpdateCompleted branch and copies UpdateProgress.PodTemplateHash "+
			"into CurrentPodTemplateHash")
	require.NotNil(t, pclq.Status.CurrentPodCliqueSetGenerationHash)
	assert.Equal(t, newGen, *pclq.Status.CurrentPodCliqueSetGenerationHash,
		"pass 2: CurrentPodCliqueSetGenerationHash is also synced from UpdateProgress")
	assert.Equal(t, int32(2), pclq.Status.UpdatedReplicas)
}

// TestRollingUpdateDeadlock_Issue315_PermanentDeadlockIfUpdateProgressNiled shows what
// would happen if any code path nilled out Status.UpdateProgress while the system was
// still in the bad precondition (UpdateEndedAt set, but counters stale and
// CurrentPodTemplateHash on the old value). With UpdateProgress == nil,
// mutateUpdatedReplica falls back to CurrentPodTemplateHash (old hash),
// counts 0 matching pods, and writes UpdatedReplicas=0 — exactly the closed feedback
// loop issue #315 describes.
//
// No HEAD code path actually does this today, so this test is a guardrail rather than
// a live-bug reproducer: it pins the invariant "do not nil UpdateProgress before
// CurrentPodTemplateHash has been synced" by demonstrating the deadlock that would
// otherwise occur.
func TestRollingUpdateDeadlock_Issue315_PermanentDeadlockIfUpdateProgressNiled(t *testing.T) {
	const (
		oldHash = "old-hash-v1"
		newHash = "new-hash-v2"
	)

	pclq := &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: "pclq-a", Namespace: "default"},
		Status: grovecorev1alpha1.PodCliqueStatus{
			Replicas:               2,
			UpdatedReplicas:        1, // stale
			CurrentPodTemplateHash: ptr.To(oldHash),
			// UpdateProgress nilled out prematurely. This is the hypothetical precondition.
			UpdateProgress: nil,
		},
	}
	pods := []*corev1.Pod{
		createPodWithHash("pod-1", newHash),
		createPodWithHash("pod-2", newHash),
	}

	// Drive several reconcile passes. In a deadlocked system the state must not change.
	// We avoid runStatusMutationChain here because the UpdateProgress == nil branch of
	// mutateCurrentHashes calls GetExpectedPCLQPodTemplateHash(pcs, pclq.ObjectMeta),
	// which needs a real PodCliqueSet. Skipping mutateCurrentHashes is fine: the issue
	// is mutateUpdatedReplica's behavior given an already-stuck CurrentPodTemplateHash.
	for i := 0; i < 5; i++ {
		pclq.Status.Replicas = int32(len(pods))
		mutateUpdatedReplica(pclq, pods)
	}

	assert.Equal(t, int32(0), pclq.Status.UpdatedReplicas,
		"deadlock: with UpdateProgress nil and CurrentPodTemplateHash still old, "+
			"mutateUpdatedReplica counts pods against the old hash and finds none — "+
			"the closed feedback loop described in issue #315")
	assert.Equal(t, oldHash, *pclq.Status.CurrentPodTemplateHash,
		"deadlock: CurrentPodTemplateHash never advances")
}

// TestRollingUpdateDeadlock_Issue315_RU9LikeSequence simulates the multi-pass reconcile
// sequence that test RU9 (Test_RU9_RollingUpdateAllPodCliques) drives end-to-end. It
// chains mutateCurrentHashes -> mutateReplicas -> mutateUpdatedReplica in the SAME order
// as reconcileStatus, advancing the world (pod creation / readiness / deletion) between
// passes. The goal is to look for a state where the chain stops making forward progress
// despite all old-hash pods being gone — a true permanent deadlock at HEAD.
//
// Outcome at HEAD: the chain converges within 2 passes after the bad precondition
// (UpdateEndedAt set + stale UpdatedReplicas != Replicas), confirming that the literal
// permanent deadlock from the issue does not reproduce here. This test is therefore a
// negative-result reproducer + watchdog: if any future change extends convergence past
// 3 passes, this test fails and points at a deadlock candidate.
func TestRollingUpdateDeadlock_Issue315_RU9LikeSequence(t *testing.T) {
	const (
		oldHash = "old-hash-v1"
		newHash = "new-hash-v2"
		oldGen  = "old-gen"
		newGen  = "new-gen"
	)
	now := metav1.Now()
	startedAt := metav1.NewTime(now.Add(-time.Minute))

	// Initial state of the world right after markRollingUpdateEnd patched UpdateEndedAt.
	// Two pods, both already swapped to the new hash, but the second one is still not
	// Ready when markRollingUpdateEnd ran, so the previous reconcile recorded
	// UpdatedReplicas=1 (only the first new pod was Ready then). This is the exact
	// precondition issue #315 calls out as the trigger for the deadlock.
	pclq := &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: "pclq-a", Namespace: "default"},
		Status: grovecorev1alpha1.PodCliqueStatus{
			Replicas:                          2,
			UpdatedReplicas:                   1, // stale: from previous reconcile
			CurrentPodTemplateHash:            ptr.To(oldHash),
			CurrentPodCliqueSetGenerationHash: ptr.To(oldGen),
			UpdateProgress: &grovecorev1alpha1.PodCliqueUpdateProgress{
				PodTemplateHash:            newHash,
				PodCliqueSetGenerationHash: newGen,
				UpdateStartedAt:            startedAt,
				UpdateEndedAt:              ptr.To(now),
			},
		},
	}
	// Both pods carry the new hash. Between passes we don't change the pod set — the
	// missing change is only the propagation of UpdatedReplicas through the chain.
	pods := []*corev1.Pod{
		createPodWithHash("pod-1", newHash),
		createPodWithHash("pod-2", newHash),
	}

	// Drive up to 10 reconcile passes. The system should converge well before that.
	const maxPasses = 10
	var converged bool
	var convergedAtPass int
	for pass := 1; pass <= maxPasses; pass++ {
		runStatusMutationChain(t, pclq, pods)
		t.Logf("pass %d: CurrentPodTemplateHash=%s UpdatedReplicas=%d Replicas=%d",
			pass, ptr.Deref(pclq.Status.CurrentPodTemplateHash, "<nil>"),
			pclq.Status.UpdatedReplicas, pclq.Status.Replicas)

		if pclq.Status.CurrentPodTemplateHash != nil &&
			*pclq.Status.CurrentPodTemplateHash == newHash &&
			pclq.Status.UpdatedReplicas == pclq.Status.Replicas {
			converged = true
			convergedAtPass = pass
			break
		}
	}

	assert.True(t, converged,
		"PCLQ status mutation chain failed to converge in %d passes — possible deadlock",
		maxPasses)
	t.Logf("converged at pass %d (HEAD short-circuit makes this 2; any regression to >2 is suspicious)", convergedAtPass)
	assert.LessOrEqual(t, convergedAtPass, 3,
		"convergence should take 1-2 passes at HEAD; >3 means the live half of issue #315 has worsened")
}

// TestRollingUpdateDeadlock_Issue315_DiagDumpReplay reproduces the EXACT state captured
// in the diagnostic dump attached to issue #315:
//
//	status:
//	  replicas: 2
//	  updatedReplicas: 0                            # not 1, not 2
//	  currentPodTemplateHash: 9f945b... (OLD)
//	  rollingUpdateProgress:
//	    podTemplateHash: 574f6f... (NEW)
//	    updateStartedAt:  ...                       # equal to updateEndedAt
//	    updateEndedAt:    ... same instant
//
// All live pods carry the NEW hash (RU9 finished deleting all old-hash pods before
// markRollingUpdateEnd ran). Then we ask: starting from this exact state, does the
// chain converge? The dump shows UpdatedReplicas=0, which the prose part of the issue
// glosses over — the real bug-on-the-wire is "0", not "1".
func TestRollingUpdateDeadlock_Issue315_DiagDumpReplay(t *testing.T) {
	const (
		oldHash = "9f945b7b659c5f4cb8f"
		newHash = "574f6fb86d49bfdfbd9d"
		oldGen  = "f6c5fd949cb444c6558"
		newGen  = "c947687c4d79f9cfb59"
	)
	now := metav1.Now()
	// updateStartedAt == updateEndedAt — exactly as in the dump (00:38:19Z / 00:38:19Z).
	pclq := &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: "workload1-0-pc-a", Namespace: "default"},
		Status: grovecorev1alpha1.PodCliqueStatus{
			Replicas:                          2,
			UpdatedReplicas:                   0, // <-- DUMP VALUE, not the prose "1"
			CurrentPodTemplateHash:            ptr.To(oldHash),
			CurrentPodCliqueSetGenerationHash: ptr.To(oldGen),
			UpdateProgress: &grovecorev1alpha1.PodCliqueUpdateProgress{
				PodTemplateHash:            newHash,
				PodCliqueSetGenerationHash: newGen,
				UpdateStartedAt:            now,
				UpdateEndedAt:              ptr.To(now),
			},
		},
	}
	pods := []*corev1.Pod{
		createPodWithHash("pod-1", newHash),
		createPodWithHash("pod-2", newHash),
	}

	const maxPasses = 10
	var converged bool
	var convergedAtPass int
	for pass := 1; pass <= maxPasses; pass++ {
		runStatusMutationChain(t, pclq, pods)
		t.Logf("pass %d: CurrentPodTemplateHash=%s UpdatedReplicas=%d Replicas=%d",
			pass, ptr.Deref(pclq.Status.CurrentPodTemplateHash, "<nil>"),
			pclq.Status.UpdatedReplicas, pclq.Status.Replicas)

		if pclq.Status.CurrentPodTemplateHash != nil &&
			*pclq.Status.CurrentPodTemplateHash == newHash &&
			pclq.Status.UpdatedReplicas == pclq.Status.Replicas {
			converged = true
			convergedAtPass = pass
			break
		}
	}

	if converged {
		t.Logf("converged at pass %d", convergedAtPass)
	}
	// We do not strictly assert convergence here because the goal is to LOG what the
	// chain does given the dumped state. If this test FAILS to converge in 10 passes,
	// we have reproduced the live HEAD deadlock and need to investigate immediately.
	assert.True(t, converged,
		"chain failed to converge starting from the issue #315 diagnostic-dump state — "+
			"if you see this failure, you've reproduced the actual deadlock at HEAD; "+
			"capture the final pclq.Status and compare against the dump")
}
