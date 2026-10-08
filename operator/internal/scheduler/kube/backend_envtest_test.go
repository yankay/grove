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
	"testing"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	schedulertest "github.com/ai-dynamo/grove/operator/test/utils/scheduler"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestBackend_InitWithWorkloadAPIs(t *testing.T) {
	tests := []struct {
		name    string
		gates   string
		enabled bool
	}{
		{
			name:  "WAS disabled",
			gates: "GenericWorkload=false,CompositePodGroup=false,TopologyAwareWorkloadScheduling=false",
		},
		{
			name:  "composite support disabled",
			gates: "GenericWorkload=true,CompositePodGroup=false,TopologyAwareWorkloadScheduling=true",
		},
		{
			name:    "hierarchical WAS enabled",
			gates:   "GenericWorkload=true,CompositePodGroup=true,TopologyAwareWorkloadScheduling=true",
			enabled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := schedulertest.StartWorkloadEnvtest(t, tt.gates)
			scheme := runtime.NewScheme()
			directClient, err := client.New(config, client.Options{Scheme: scheme})
			require.NoError(t, err)

			disabled := New(nil, scheme, nil, configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKube})
			require.NoError(t, disabled.Init(directClient))
			enabled := New(nil, scheme, nil, configv1alpha1.SchedulerProfile{
				Name:   configv1alpha1.SchedulerNameKube,
				Config: &runtime.RawExtension{Raw: []byte(`{"gangScheduling":true}`)},
			})
			err = enabled.Init(directClient)
			if tt.enabled {
				require.NoError(t, err)
			} else {
				require.True(t, meta.IsNoMatchError(err), "missing APIs must fail initialization, got %v", err)
			}
		})
	}
}

func TestSyncPodGang_TemplateListLimitsWithWorkloadAPIs(t *testing.T) {
	config := schedulertest.StartWorkloadEnvtest(t, "GenericWorkload=true,CompositePodGroup=true,TopologyAwareWorkloadScheduling=true")
	scheme := newWorkloadTestScheme(t)
	require.NoError(t, corev1.AddToScheme(scheme))
	directClient, err := client.New(config, client.Options{Scheme: scheme})
	require.NoError(t, err)
	require.NoError(t, directClient.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}}))
	backend := New(directClient, scheme, nil, configv1alpha1.SchedulerProfile{
		Name:   configv1alpha1.SchedulerNameKube,
		Config: &runtime.RawExtension{Raw: []byte(`{"gangScheduling":true}`)},
	})
	require.NoError(t, backend.Init(directClient))
	podGang := newTestPodGang(withTemplateLists(8, 8))
	require.NoError(t, backend.SyncPodGang(t.Context(), podGang))
	composites := &schedulingv1alpha3.CompositePodGroupList{}
	require.NoError(t, directClient.List(t.Context(), composites, client.InNamespace(testNamespace)))
	require.Len(t, composites.Items, 9)
	leaves := &schedulingv1beta1.PodGroupList{}
	require.NoError(t, directClient.List(t.Context(), leaves, client.InNamespace(testNamespace)))
	require.Len(t, leaves.Items, 16)
}
