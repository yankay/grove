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

package pod

import (
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
)

func TestResolveStartupDependenciesFromCurrentMembership(t *testing.T) {
	for _, tc := range []struct {
		name       string
		startup    grovecorev1alpha1.CliqueStartupType
		clique     string
		groupIndex string
		gang       string
		want       []string
		wantError  bool
	}{
		{name: "in order skips idle predecessor and includes newly active predecessor", startup: grovecorev1alpha1.CliqueStartupTypeInOrder,
			clique: "prefill", groupIndex: "0", gang: "pcs-0-anchor", want: []string{"pcs-0-worker"}},
		{name: "explicit skips idle and preserves active dependencies", startup: grovecorev1alpha1.CliqueStartupTypeExplicit,
			clique: "prefill", groupIndex: "0", gang: "pcs-0-anchor", want: []string{"pcs-0-worker"}},
		{name: "scale out waits only for its own group replica", startup: grovecorev1alpha1.CliqueStartupTypeInOrder,
			clique: "decode", groupIndex: "2", gang: "pcs-0-scale-workers-2", want: []string{"pcs-0-workers-2-prefill"}},
		{name: "standalone skips missing predecessor", startup: grovecorev1alpha1.CliqueStartupTypeInOrder,
			clique: "worker", gang: "pcs-0-anchor"},
		{name: "any order has no dependency", startup: grovecorev1alpha1.CliqueStartupTypeAnyOrder,
			clique: "decode", groupIndex: "0", gang: "pcs-0-anchor"},
		{name: "unknown gang fails closed", startup: grovecorev1alpha1.CliqueStartupTypeInOrder,
			clique: "prefill", groupIndex: "0", gang: "missing", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").
				WithPodCliqueParameters("worker", 0, nil).
				WithPodCliqueParameters("guarded", 2, nil).
				WithPodCliqueParameters("prefill", 1, nil).
				WithPodCliqueParameters("decode", 1, nil).Build()
			pcs.Spec.Template.StartupType = &tc.startup
			pcs.Spec.Template.Cliques[2].Spec.StartsAfter = []string{"guarded", "worker"}
			pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
				Name: "workers", CliqueNames: []string{"prefill", "decode"}, Replicas: ptr.To(int32(1)), MinAvailable: ptr.To(int32(1)),
			}}
			pclq := testutils.NewPodCliqueBuilder(pcs.Name, pcs.UID, tc.clique, pcs.Namespace, 0).Build()
			pclq.Spec.StartsAfter = []string{"pcs-0-guarded"}
			pclq.Labels[apicommon.LabelPodCliqueScalingGroupReplicaIndex] = tc.groupIndex
			ss := &syncSnapshot{
				pcs: pcs, pclq: pclq, cliqueName: tc.clique,
				pgm: &grovecorev1alpha1.PodGangMap{Spec: grovecorev1alpha1.PodGangMapSpec{
					Entries: []grovecorev1alpha1.PodGangEntry{
						{Role: grovecorev1alpha1.PodGangEntryRoleAnchor, Epoch: "anchor",
							PodCliques: map[string]int32{"worker": 4}, PCSGReplicaIndices: map[string][]int32{"workers": {0}}},
						{Role: grovecorev1alpha1.PodGangEntryRoleScaleOut, Epoch: "scale",
							PCSGReplicaIndices: map[string][]int32{"workers": {2}}},
					},
				}},
			}
			dependencies, err := resolveStartupDependencies(ss, tc.gang)
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, dependencies)
			assert.Equal(t, []string{"pcs-0-guarded"}, pclq.Spec.StartsAfter, "creation must not mutate cached scale objects")
		})
	}
}
