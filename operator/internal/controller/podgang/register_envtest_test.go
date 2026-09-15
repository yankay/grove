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

package podgang

import (
	"context"
	"testing"
	"time"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler/registry"
	schedulertest "github.com/ai-dynamo/grove/operator/test/utils/scheduler"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerconfig "sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

const (
	workloadReconcileTimeout = 30 * time.Second
	workloadPollInterval     = 100 * time.Millisecond
)

func TestPodGangRecreatesDeletedWorkloadResources(t *testing.T) {
	config := schedulertest.StartWorkloadEnvtest(t,
		"GenericWorkload=true,CompositePodGroup=true,TopologyAwareWorkloadScheduling=true",
		"../../../../scheduler/api/core/v1alpha1/crds")
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, groveschedulerv1alpha1.AddToScheme(scheme))
	directClient, err := client.New(config, client.Options{Scheme: scheme})
	require.NoError(t, err)
	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		// Controller names remain process-global between go test -count runs.
		Controller: controllerconfig.Controller{SkipNameValidation: ptr.To(true)},
	})
	require.NoError(t, err)
	schedulers, err := registry.New(mgr.GetClient(), directClient, scheme, nil, configv1alpha1.SchedulerConfiguration{
		DefaultProfileName: string(configv1alpha1.SchedulerNameKube),
		Profiles: []configv1alpha1.SchedulerProfile{{
			Name:   configv1alpha1.SchedulerNameKube,
			Config: &runtime.RawExtension{Raw: []byte(`{"gangScheduling":true}`)},
		}},
	})
	require.NoError(t, err)
	reconciler := NewReconciler(mgr, configv1alpha1.PodGangControllerConfiguration{ConcurrentSyncs: ptr.To(1)}, schedulers)
	require.NoError(t, reconciler.RegisterWithManager(mgr))
	ctx, cancel := context.WithCancel(t.Context())
	managerDone := make(chan error, 1)
	go func() { managerDone <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-managerDone:
			require.NoError(t, err)
		case <-time.After(workloadReconcileTimeout):
			t.Error("manager did not stop")
		}
	})
	syncCtx, syncCancel := context.WithTimeout(ctx, workloadReconcileTimeout)
	defer syncCancel()
	require.True(t, mgr.GetCache().WaitForCacheSync(syncCtx))
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "was-owner-watch"}}
	require.NoError(t, directClient.Create(ctx, ns))
	gang := &groveschedulerv1alpha1.PodGang{
		ObjectMeta: metav1.ObjectMeta{
			Name: "was-gang", Namespace: ns.Name,
			Labels: map[string]string{apicommon.LabelManagedByKey: apicommon.LabelManagedByValue},
		},
		Spec: groveschedulerv1alpha1.PodGangSpec{
			PodGroups: []groveschedulerv1alpha1.PodGroup{{
				Name: "was-leaf", MinReplicas: 2, PodReferences: []groveschedulerv1alpha1.NamespacedName{},
			}},
			TopologyConstraintGroupConfigs: []groveschedulerv1alpha1.TopologyConstraintGroupConfig{{
				Name: "rack", PodGroupNames: []string{"was-leaf"},
			}},
		},
	}
	require.NoError(t, directClient.Create(ctx, gang))
	resources := []client.Object{
		&schedulingv1beta1.Workload{ObjectMeta: metav1.ObjectMeta{Name: gang.Name, Namespace: ns.Name}},
		&schedulingv1alpha3.CompositePodGroup{ObjectMeta: metav1.ObjectMeta{Name: gang.Name, Namespace: ns.Name}},
		&schedulingv1alpha3.CompositePodGroup{ObjectMeta: metav1.ObjectMeta{Name: gang.Name + "-rack", Namespace: ns.Name}},
		&schedulingv1beta1.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "was-leaf", Namespace: ns.Name}},
	}
	for _, resource := range resources {
		require.Eventually(t, func() bool {
			return directClient.Get(ctx, client.ObjectKeyFromObject(resource), resource) == nil
		}, workloadReconcileTimeout, workloadPollInterval)
		require.True(t, metav1.IsControlledBy(resource, gang))
	}

	// Releasing the Grove minimum must not prevent recovery of any one missing
	// generated object, and recovery must be triggered without a PodGang edit.
	require.NoError(t, directClient.Get(ctx, client.ObjectKeyFromObject(gang), gang))
	gang.Spec.PodGroups[0].MinReplicas = 0
	require.NoError(t, directClient.Update(ctx, gang))
	generation := gang.Generation
	for _, resource := range resources {
		gvk, err := directClient.GroupVersionKindFor(resource)
		require.NoError(t, err)
		t.Run(gvk.Kind+"/"+resource.GetName(), func(t *testing.T) {
			require.NoError(t, directClient.Get(ctx, client.ObjectKeyFromObject(resource), resource))
			uid := resource.GetUID()
			require.NoError(t, directClient.Delete(ctx, resource))
			if _, ok := resource.(*schedulingv1beta1.PodGroup); ok {
				require.NoError(t, directClient.Get(ctx, client.ObjectKeyFromObject(resource), resource))
				require.False(t, resource.GetDeletionTimestamp().IsZero())
				require.Equal(t, []string{"scheduling.k8s.io/podgroup-protection"}, resource.GetFinalizers())
				// envtest has no controller-manager. Emulate its protection
				// controller only after verifying that no member Pods exist.
				pods := &corev1.PodList{}
				require.NoError(t, directClient.List(ctx, pods, client.InNamespace(ns.Name)))
				require.Empty(t, pods.Items)
				resource.SetFinalizers(nil)
				require.NoError(t, directClient.Update(ctx, resource))
			}
			require.Eventually(t, func() bool {
				if err := directClient.Get(ctx, client.ObjectKeyFromObject(resource), resource); err != nil {
					return false
				}
				return resource.GetUID() != uid && metav1.IsControlledBy(resource, gang)
			}, workloadReconcileTimeout, workloadPollInterval, "owner watch must recreate %T", resource)
		})
	}
	require.NoError(t, directClient.Get(ctx, client.ObjectKeyFromObject(gang), gang))
	assert.Equal(t, generation, gang.Generation)
	assert.Empty(t, gang.Status.Conditions, "backend must leave Pod-derived status to the PCS controller")
	leaf := &schedulingv1beta1.PodGroup{}
	require.NoError(t, directClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "was-leaf"}, leaf))
	require.NotNil(t, leaf.Spec.SchedulingPolicy.Gang)
	assert.Equal(t, int32(2), leaf.Spec.SchedulingPolicy.Gang.MinCount)
	assert.Equal(t, ptr.To(gang.Name+"-rack"), leaf.Spec.ParentCompositePodGroupName)
}
