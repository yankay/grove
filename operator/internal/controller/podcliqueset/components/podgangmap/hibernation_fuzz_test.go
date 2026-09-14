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
	"slices"
	"strconv"
	"testing"
	"time"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

func FuzzHibernationMembership(f *testing.F) {
	f.Add([]byte{0, 255, 0, 255})
	f.Add([]byte{1, 2, 3, 4, 5, 32, 64, 128, 255, 0})
	f.Add([]byte{255, 254, 253, 252, 128, 64, 32, 16, 0})
	f.Fuzz(func(t *testing.T, operations []byte) {
		if len(operations) > 128 {
			operations = operations[:128]
		}
		pcs := testutils.NewPodCliqueSetBuilder(testPCSName, testNamespace, testPCSUID).
			WithStandaloneCliqueReplicas("worker", 0).
			WithScalingGroupConfig(testPCSGName, []string{"prefill"}, 0, 2).
			WithScalingGroupConfig("decode", []string{"decode"}, 0, 2).
			WithPodCliqueSetGenerationHash(ptr.To(testGenHash)).Build()
		pgm := &grovecorev1alpha1.PodGangMap{Spec: grovecorev1alpha1.PodGangMapSpec{
			Entries: []grovecorev1alpha1.PodGangEntry{anchorEntry(nil, nil), scaleOutEntry(nil)},
		}}
		clk := clocktesting.NewFakeClock(time.Unix(0, 50))
		for step, op := range operations {
			workerReplicas := int32(op & 3)
			groupReplicas := []int32{0, 2, 3, 4}
			first, second := groupReplicas[(op>>2)&3], groupReplicas[(op>>4)&3]
			decode := pcsg(second)
			decode.Name = testPCSName + "-0-decode"
			groups := []grovecorev1alpha1.PodCliqueScalingGroup{pcsg(first), decode}
			cliques := []grovecorev1alpha1.PodClique{standalonePCLQ("worker", workerReplicas)}
			before := pgm.DeepCopy()
			entries, err := reconcileEntries(clk, pcs, 0, pgm, nil, cliques, groups)
			require.NoError(t, err, "step %d, op %d", step, op)
			require.Equal(t, before, pgm, "cached input mutated at step %d", step)
			require.Len(t, entries, 2)
			anchor := testutils.EntryByRole(entries, grovecorev1alpha1.PodGangEntryRoleAnchor)
			extra := testutils.EntryByRole(entries, grovecorev1alpha1.PodGangEntryRoleScaleOut)
			require.Equal(t, "100", anchor.Epoch)
			require.Equal(t, workerReplicas, anchor.PodCliques["worker"])
			require.Equal(t, workerReplicas > 0, len(anchor.PodCliques) == 1)
			require.Empty(t, extra.PodCliques)
			for name, replicas := range map[string]int32{testPCSGName: first, "decode": second} {
				var anchorIndices, extraIndices []int32
				for index := range replicas {
					if index < 2 {
						anchorIndices = append(anchorIndices, index)
					} else {
						extraIndices = append(extraIndices, index)
					}
				}
				require.Equal(t, anchorIndices, anchor.PCSGReplicaIndices[name])
				require.Equal(t, extraIndices, extra.PCSGReplicaIndices[name])
			}
			oldExtra := testutils.EntryByRole(before.Spec.Entries, grovecorev1alpha1.PodGangEntryRoleScaleOut)
			if componentutils.IsPodGangEntryEmpty(oldExtra) && !componentutils.IsPodGangEntryEmpty(extra) {
				oldEpoch, err := strconv.ParseInt(oldExtra.Epoch, 10, 64)
				require.NoError(t, err)
				epoch, err := strconv.ParseInt(extra.Epoch, 10, 64)
				require.NoError(t, err)
				require.Greater(t, epoch, oldEpoch)
			} else {
				require.Equal(t, oldExtra.Epoch, extra.Epoch)
			}
			slices.Reverse(groups)
			permuted, err := reconcileEntries(clk, pcs, 0, pgm, nil, cliques, groups)
			require.NoError(t, err)
			require.Equal(t, entries, permuted, "observation order changed placement")
			pgm.Spec.Entries = entries
			repeated, err := reconcileEntries(clk, pcs, 0, pgm, nil, cliques, groups)
			require.NoError(t, err)
			require.Equal(t, entries, repeated, "steady state churn at step %d", step)
		}
	})
}
