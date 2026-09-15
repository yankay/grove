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
	"encoding/json"
	"errors"
	"testing"
	"time"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

func TestBackend_Init(t *testing.T) {
	workloadUnavailable := &meta.NoKindMatchError{
		GroupKind: schema.GroupKind{Group: "scheduling.k8s.io", Kind: "Workload"},
	}
	podGroupUnavailable := apierrors.NewNotFound(schema.GroupResource{Group: "scheduling.k8s.io", Resource: "podgroups"}, "")
	compositeUnavailable := &meta.NoKindMatchError{
		GroupKind: schema.GroupKind{Group: "scheduling.k8s.io", Kind: "CompositePodGroup"},
	}
	forbidden := apierrors.NewForbidden(schema.GroupResource{Group: "scheduling.k8s.io", Resource: "workloads"}, "", errors.New("list denied"))
	tests := []struct {
		name      string
		config    *runtime.RawExtension
		failKind  string
		cause     error
		wantCalls []string
	}{
		{name: "omitted config"},
		{name: "empty config", config: &runtime.RawExtension{}},
		{name: "disabled", config: &runtime.RawExtension{Raw: []byte(`{"gangScheduling":false}`)}},
		{
			name:      "enabled",
			config:    &runtime.RawExtension{Raw: []byte(`{"gangScheduling":true}`)},
			wantCalls: []string{"WorkloadList", "PodGroupList", "CompositePodGroupList"},
		},
		{
			name: "Workload unavailable", config: &runtime.RawExtension{Raw: []byte(`{"gangScheduling":true}`)},
			failKind: "WorkloadList", cause: workloadUnavailable, wantCalls: []string{"WorkloadList"},
		},
		{
			name: "PodGroup unavailable", config: &runtime.RawExtension{Raw: []byte(`{"gangScheduling":true}`)},
			failKind: "PodGroupList", cause: podGroupUnavailable, wantCalls: []string{"WorkloadList", "PodGroupList"},
		},
		{
			name: "CompositePodGroup unavailable", config: &runtime.RawExtension{Raw: []byte(`{"gangScheduling":true}`)},
			failKind: "CompositePodGroupList", cause: compositeUnavailable, wantCalls: []string{"WorkloadList", "PodGroupList", "CompositePodGroupList"},
		},
		{
			name: "RBAC denied", config: &runtime.RawExtension{Raw: []byte(`{"gangScheduling":true}`)},
			failKind: "WorkloadList", cause: forbidden, wantCalls: []string{"WorkloadList"},
		},
		{
			name: "request timeout", config: &runtime.RawExtension{Raw: []byte(`{"gangScheduling":true}`)},
			failKind: "WorkloadList", cause: context.DeadlineExceeded, wantCalls: []string{"WorkloadList"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			var calls []string
			directClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
				List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					deadline, ok := ctx.Deadline()
					require.True(t, ok, "startup probes must have a deadline")
					require.Positive(t, time.Until(deadline))
					require.LessOrEqual(t, time.Until(deadline), workloadAPICheckTimeout)
					listOptions := &client.ListOptions{}
					listOptions.ApplyOptions(opts)
					assert.Equal(t, int64(1), listOptions.Limit)
					gvk, err := cl.GroupVersionKindFor(list)
					require.NoError(t, err)
					calls = append(calls, gvk.Kind)
					if gvk.Kind == tt.failKind {
						return tt.cause
					}
					return cl.List(ctx, list, opts...)
				},
			}).Build()
			b := New(nil, scheme, nil, configv1alpha1.SchedulerProfile{
				Name: configv1alpha1.SchedulerNameKube, Config: tt.config,
			})
			err := b.Init(directClient)
			resources := b.(interface{ PodGangResources() []client.Object }).PodGangResources()
			if tt.cause != nil {
				require.ErrorIs(t, err, tt.cause)
				assert.Empty(t, resources)
			} else {
				require.NoError(t, err)
				if len(tt.wantCalls) > 0 {
					var kinds []string
					for _, resource := range resources {
						gvk, err := directClient.GroupVersionKindFor(resource)
						require.NoError(t, err)
						kinds = append(kinds, gvk.Kind)
					}
					assert.ElementsMatch(t, []string{"Workload", "CompositePodGroup", "PodGroup"}, kinds)
				} else {
					assert.Empty(t, resources)
				}
			}
			assert.Equal(t, tt.wantCalls, calls)
		})
	}
}

func TestBackend_InitRejectsInvalidConfig(t *testing.T) {
	b := New(nil, runtime.NewScheme(), nil, configv1alpha1.SchedulerProfile{
		Name:   configv1alpha1.SchedulerNameKube,
		Config: &runtime.RawExtension{Raw: []byte(`{"gangScheduling":`)},
	})
	var syntaxError *json.SyntaxError
	require.ErrorAs(t, b.Init(nil), &syntaxError)
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
