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

package kube

import (
	"context"
	"testing"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
)

func TestBackend_PreparePod(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	profile := configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKube}
	b := New(cl, cl.Scheme(), recorder, profile)

	pod := testutils.NewPodBuilder("test-pod", "default").Build()

	require.NoError(t, b.PreparePod(pod))

	assert.Equal(t, string(configv1alpha1.SchedulerNameKube), pod.Spec.SchedulerName)
}

func preferredConstraint() *grovecorev1alpha1.TopologyConstraint {
	return &grovecorev1alpha1.TopologyConstraint{
		Pack: &grovecorev1alpha1.TopologyPackConstraint{PreferredDomain: "rack"},
	}
}

func requiredConstraint() *grovecorev1alpha1.TopologyConstraint {
	return &grovecorev1alpha1.TopologyConstraint{
		Pack: &grovecorev1alpha1.TopologyPackConstraint{RequiredDomain: "rack"},
	}
}

// TestValidatePodCliqueSet_NoOpWithoutGangScheduling verifies validation is
// skipped when gang scheduling is off, even for preferred constraints.
func TestValidatePodCliqueSet_NoOpWithoutGangScheduling(t *testing.T) {
	b := &schedulerBackend{gangSchedulingEnabled: false}
	pcs := &grovecorev1alpha1.PodCliqueSet{ObjectMeta: metav1.ObjectMeta{Name: "pcs"}}
	pcs.Spec.Template.TopologyConstraint = preferredConstraint()
	require.NoError(t, b.ValidatePodCliqueSet(context.Background(), pcs))
}

// TestValidatePodCliqueSet_RejectsPreferred verifies preferred topology
// constraints fail closed at every level, while required constraints pass.
func TestValidatePodCliqueSet_RejectsPreferred(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*grovecorev1alpha1.PodCliqueSet)
		wantErrPart string
	}{
		{
			name:        "no constraints",
			mutate:      func(*grovecorev1alpha1.PodCliqueSet) {},
			wantErrPart: "",
		},
		{
			name: "required on PodCliqueSet",
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.TopologyConstraint = requiredConstraint()
			},
			wantErrPart: "",
		},
		{
			name: "preferred on PodCliqueSet",
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.TopologyConstraint = preferredConstraint()
			},
			wantErrPart: "PodCliqueSet",
		},
		{
			name: "preferred on PodCliqueScalingGroup",
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{
					{Name: "sg-a", TopologyConstraint: preferredConstraint()},
				}
			},
			wantErrPart: "PodCliqueScalingGroup",
		},
		{
			name: "preferred on PodClique",
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
					{Name: "clique-a", TopologyConstraint: preferredConstraint()},
				}
			},
			wantErrPart: "PodClique",
		},
	}
	b := &schedulerBackend{gangSchedulingEnabled: true}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pcs := &grovecorev1alpha1.PodCliqueSet{ObjectMeta: metav1.ObjectMeta{Name: "pcs"}}
			tt.mutate(pcs)
			err := b.ValidatePodCliqueSet(context.Background(), pcs)
			if tt.wantErrPart == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErrPart)
		})
	}
}
