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
	"strconv"
	"testing"
	"time"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

func TestReconcileHibernatingComponents(t *testing.T) {
	pcs := testutils.NewPodCliqueSetBuilder(testPCSName, testNamespace, testPCSUID).
		WithStandaloneCliqueReplicas("router", 1).
		WithStandaloneCliqueReplicas("worker", 0).
		WithScalingGroupConfig(testPCSGName, []string{"c"}, 0, 2).
		WithPodCliqueSetGenerationHash(ptr.To(testGenHash)).Build()
	tests := []struct {
		name            string
		entries         []grovecorev1alpha1.PodGangEntry
		cliques         []grovecorev1alpha1.PodClique
		groups          []grovecorev1alpha1.PodCliqueScalingGroup
		wantAnchorCount int
		wantAnchorIndex int32
		wantWorker      int32
		wantIndices     []int32
		wantEpoch       string
		wantDependsOn   []string
	}{
		{
			name: "standalone wake restores the running anchor",
			entries: []grovecorev1alpha1.PodGangEntry{
				anchorEntry(map[string]int32{"router": 1}, nil), scaleOutEntry(nil),
			},
			cliques:         []grovecorev1alpha1.PodClique{standalonePCLQ("worker", 3)},
			wantAnchorCount: 1,
			wantWorker:      3,
			wantEpoch:       "102",
			wantDependsOn:   []string{"100"},
		},
		{
			name: "PCSG wake places every replica in scaleout behind the running anchor",
			entries: []grovecorev1alpha1.PodGangEntry{
				anchorEntry(map[string]int32{"router": 1}, nil), scaleOutEntry(nil),
			},
			groups:          []grovecorev1alpha1.PodCliqueScalingGroup{pcsg(3)},
			wantAnchorCount: 1,
			wantIndices:     []int32{0, 1, 2},
			wantEpoch:       "5000",
			wantDependsOn:   []string{"100"},
		},
		{
			name: "PCSG-only wake does not depend on an unmaterialized anchor",
			entries: []grovecorev1alpha1.PodGangEntry{
				anchorEntry(nil, nil), scaleOutEntry(nil),
			},
			groups:          []grovecorev1alpha1.PodCliqueScalingGroup{pcsg(2)},
			wantAnchorCount: 1,
			wantIndices:     []int32{0, 1},
			wantEpoch:       "5000",
		},
		{
			name: "concurrent wake restores standalone anchor and independently scales PCSG replicas",
			entries: []grovecorev1alpha1.PodGangEntry{
				anchorEntry(nil, nil), scaleOutEntry(nil),
			},
			cliques:         []grovecorev1alpha1.PodClique{standalonePCLQ("worker", 3)},
			groups:          []grovecorev1alpha1.PodCliqueScalingGroup{pcsg(2)},
			wantAnchorCount: 1,
			wantWorker:      3,
			wantIndices:     []int32{0, 1},
			wantEpoch:       "5000",
			wantDependsOn:   []string{"100"},
		},
		{
			name: "standalone wake after PCSG wake leaves active scaleout dependencies unchanged",
			entries: []grovecorev1alpha1.PodGangEntry{
				anchorEntry(nil, nil),
				testutils.NewPodGangEntryBuilder(testGenHash, "102").
					WithRole(grovecorev1alpha1.PodGangEntryRoleScaleOut).
					WithPCSGReplicaIndices(map[string][]int32{testPCSGName: {0, 1}}).Build(),
			},
			cliques:         []grovecorev1alpha1.PodClique{standalonePCLQ("worker", 3)},
			groups:          []grovecorev1alpha1.PodCliqueScalingGroup{pcsg(3)},
			wantAnchorCount: 1,
			wantWorker:      3,
			wantIndices:     []int32{0, 1, 2},
			wantEpoch:       "102",
		},
		{
			name: "post-coherent wake selects the highest anchor and preserves empty anchor slots",
			entries: []grovecorev1alpha1.PodGangEntry{
				anchorEntry(nil, nil),
				testutils.NewPodGangEntryBuilder(testGenHash, "200").
					WithRole(grovecorev1alpha1.PodGangEntryRoleAnchor).WithAnchorIndex(2).Build(),
				scaleOutEntry(nil),
			},
			cliques:         []grovecorev1alpha1.PodClique{standalonePCLQ("worker", 3)},
			groups:          []grovecorev1alpha1.PodCliqueScalingGroup{pcsg(2)},
			wantAnchorCount: 2,
			wantAnchorIndex: 2,
			wantWorker:      3,
			wantIndices:     []int32{0, 1},
			wantEpoch:       "5000",
			wantDependsOn:   []string{"200"},
		},
		{
			name: "anchor scale-in and PCSG wake do not leave a dangling dependency",
			entries: []grovecorev1alpha1.PodGangEntry{
				anchorEntry(map[string]int32{"router": 1}, nil), scaleOutEntry(nil),
			},
			cliques:         []grovecorev1alpha1.PodClique{standalonePCLQ("router", 0)},
			groups:          []grovecorev1alpha1.PodCliqueScalingGroup{pcsg(2)},
			wantAnchorCount: 1,
			wantIndices:     []int32{0, 1},
			wantEpoch:       "5000",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pgm := &grovecorev1alpha1.PodGangMap{Spec: grovecorev1alpha1.PodGangMapSpec{Entries: tt.entries}}
			before := pgm.DeepCopy()
			clk := clocktesting.NewFakeClock(time.Unix(0, 5000))
			entries, err := reconcileEntries(clk, pcs, 0, pgm, nil, tt.cliques, tt.groups)
			require.NoError(t, err)
			assert.Equal(t, before, pgm, "reconciliation must not mutate the cached object")
			anchors := currentGenerationAnchorsByIndexDesc(entries, testGenHash)
			require.Len(t, anchors, tt.wantAnchorCount)
			assert.Equal(t, tt.wantAnchorIndex, *anchors[0].AnchorIndex)
			assert.Equal(t, tt.wantWorker, anchors[0].PodCliques["worker"])
			originalAnchors := currentGenerationAnchorsByIndexDesc(tt.entries, testGenHash)
			require.Len(t, anchors, len(originalAnchors))
			for i, anchor := range anchors {
				assert.Empty(t, anchor.PCSGReplicaIndices)
				assert.Equal(t, originalAnchors[i].AnchorIndex, anchor.AnchorIndex)
				assert.Equal(t, originalAnchors[i].Epoch, anchor.Epoch)
			}
			scaleOut := testutils.EntryByRole(entries, grovecorev1alpha1.PodGangEntryRoleScaleOut)
			require.Equal(t, grovecorev1alpha1.PodGangEntryRoleScaleOut, scaleOut.Role)
			assert.Equal(t, tt.wantIndices, scaleOut.PCSGReplicaIndices[testPCSGName])
			assert.Equal(t, tt.wantEpoch, scaleOut.Epoch)
			assert.Equal(t, tt.wantDependsOn, scaleOut.DependsOn)

			pgm.Spec.Entries = entries
			repeated, err := reconcileEntries(clk, pcs, 0, pgm, nil, tt.cliques, tt.groups)
			require.NoError(t, err)
			assert.Equal(t, entries, repeated, "steady state must not churn epochs or membership")
		})
	}
}

func TestRepeatedHibernationAfterGenerationChange(t *testing.T) {
	pcs := testutils.NewPodCliqueSetBuilder(testPCSName, testNamespace, testPCSUID).
		WithStandaloneCliqueReplicas("worker", 0).
		WithScalingGroupConfig(testPCSGName, []string{"c"}, 0, 2).
		WithPodCliqueSetGenerationHash(ptr.To("new-hash")).Build()
	pgm := &grovecorev1alpha1.PodGangMap{Spec: grovecorev1alpha1.PodGangMapSpec{
		Entries: []grovecorev1alpha1.PodGangEntry{anchorEntry(nil, nil), scaleOutEntry(nil)},
	}}
	clk := clocktesting.NewFakeClock(time.Unix(0, 50))
	previousEpoch := int64(102)
	for range 3 {
		woken, err := reconcileEntries(clk, pcs, 0, pgm, nil,
			[]grovecorev1alpha1.PodClique{standalonePCLQ("worker", 2)},
			[]grovecorev1alpha1.PodCliqueScalingGroup{pcsg(2)})
		require.NoError(t, err)
		require.Len(t, woken, 2)
		scaleOut := testutils.EntryByRole(woken, grovecorev1alpha1.PodGangEntryRoleScaleOut)
		assert.Equal(t, []int32{0, 1}, scaleOut.PCSGReplicaIndices[testPCSGName])
		assert.Equal(t, []string{"100"}, scaleOut.DependsOn)
		assert.Equal(t, "new-hash", scaleOut.PodCliqueSetGenerationHash)
		epoch, err := strconv.ParseInt(scaleOut.Epoch, 10, 64)
		require.NoError(t, err)
		assert.Greater(t, epoch, previousEpoch)
		previousEpoch = epoch
		pgm.Spec.Entries = woken

		idle, err := reconcileEntries(clk, pcs, 0, pgm, nil,
			[]grovecorev1alpha1.PodClique{standalonePCLQ("worker", 0)},
			[]grovecorev1alpha1.PodCliqueScalingGroup{pcsg(0)})
		require.NoError(t, err)
		require.Len(t, idle, 2)
		for _, entry := range idle {
			assert.True(t, componentutils.IsPodGangEntryEmpty(entry))
		}
		assert.Equal(t, "100", testutils.EntryByRole(idle, grovecorev1alpha1.PodGangEntryRoleAnchor).Epoch)
		pgm.Spec.Entries = idle
	}
}

func TestMultiplePCSGWakesShareScaleOutWithoutJoiningAnchor(t *testing.T) {
	pcs := testutils.NewPodCliqueSetBuilder(testPCSName, testNamespace, testPCSUID).
		WithStandaloneCliqueReplicas("router", 1).
		WithScalingGroupConfig(testPCSGName, []string{"prefill"}, 0, 2).
		WithScalingGroupConfig("decode", []string{"decode"}, 0, 2).
		WithPodCliqueSetGenerationHash(ptr.To(testGenHash)).Build()
	pgm := &grovecorev1alpha1.PodGangMap{Spec: grovecorev1alpha1.PodGangMapSpec{
		Entries: []grovecorev1alpha1.PodGangEntry{
			anchorEntry(map[string]int32{"router": 1}, nil), scaleOutEntry(nil),
		},
	}}
	decode := pcsg(2)
	decode.Name = testPCSName + "-0-decode"
	clk := clocktesting.NewFakeClock(time.Unix(0, 5000))
	groups := []grovecorev1alpha1.PodCliqueScalingGroup{pcsg(3), decode}
	entries, err := reconcileEntries(clk, pcs, 0, pgm, nil, nil, groups)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, pgm.Spec.Entries[0], testutils.EntryByRole(entries, grovecorev1alpha1.PodGangEntryRoleAnchor))
	scaleOut := testutils.EntryByRole(entries, grovecorev1alpha1.PodGangEntryRoleScaleOut)
	require.Equal(t, grovecorev1alpha1.PodGangEntryRoleScaleOut, scaleOut.Role)
	assert.Equal(t, map[string][]int32{testPCSGName: {0, 1, 2}, "decode": {0, 1}}, scaleOut.PCSGReplicaIndices)
	assert.Equal(t, "5000", scaleOut.Epoch)
	assert.Equal(t, []string{"100"}, scaleOut.DependsOn)

	reversed, err := reconcileEntries(clk, pcs, 0, pgm, nil, nil,
		[]grovecorev1alpha1.PodCliqueScalingGroup{decode, pcsg(3)})
	require.NoError(t, err)
	assert.Equal(t, entries, reversed, "PCSG observation order must not change wake placement")
}
