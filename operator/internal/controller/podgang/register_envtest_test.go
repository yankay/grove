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
	"os"
	"testing"
	"time"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"
	"github.com/ai-dynamo/grove/operator/internal/scheduler/kai"
	"github.com/ai-dynamo/grove/operator/internal/scheduler/lpx"
	"github.com/ai-dynamo/grove/operator/internal/scheduler/volcano"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"
	schedulertest "github.com/ai-dynamo/grove/operator/test/utils/scheduler"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	kaischedulingv2alpha2 "github.com/kai-scheduler/KAI-scheduler/pkg/apis/scheduling/v2alpha2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	volcanov1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

func TestNativePodGroupWatchRepair(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	const watchTimeout = 30 * time.Second
	const watchInterval = 100 * time.Millisecond
	testEnv := &envtest.Environment{
		CRDDirectoryPaths: []string{"../../../api/core/v1alpha1/crds", "../../../../scheduler/api/core/v1alpha1/crds"},
		CRDs: []*apiextensionsv1.CustomResourceDefinition{
			schedulertest.NewPodGroupWatchCRD(kaischedulingv2alpha2.SchemeGroupVersion),
			schedulertest.NewPodGroupWatchCRD(volcanov1beta1.SchemeGroupVersion),
		},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := testEnv.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testEnv.Stop()) })
	scheme := schedulertest.NewKAIScheme(t)
	require.NoError(t, volcanov1beta1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}})
	require.NoError(t, err)
	registry := &testutils.FakeSchedulerRegistry{Backends: map[string]scheduler.Backend{
		string(configv1alpha1.SchedulerNameKai):     kai.New(mgr.GetClient(), scheme, nil, configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKai}),
		string(configv1alpha1.SchedulerNameVolcano): volcano.New(mgr.GetClient(), scheme, nil, configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameVolcano}),
	}}
	registry.Backends[string(configv1alpha1.SchedulerNameLPX)] = lpx.New(mgr.GetClient(),
		configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameLPX},
		registry.Get(string(configv1alpha1.SchedulerNameKai)))
	r := NewReconciler(mgr, configv1alpha1.PodGangControllerConfiguration{ConcurrentSyncs: ptr.To(2)}, registry)
	require.NoError(t, r.RegisterWithManager(mgr))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(watchTimeout):
			t.Error("manager shutdown timed out")
		}
	})
	cacheCtx, stopCacheWait := context.WithTimeout(ctx, watchTimeout)
	defer stopCacheWait()
	require.True(t, mgr.GetCache().WaitForCacheSync(cacheCtx))
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	pcs := testutils.NewPodCliqueSetBuilder("watch-pcs", "default", "").
		WithReplicas(1).WithPodCliqueParameters("worker", 1, nil).Build()
	pcs.Labels = map[string]string{"kai.scheduler/queue": "team"}
	require.NoError(t, cl.Create(ctx, pcs))
	pclq := testutils.NewPodCliqueBuilder(pcs.Name, pcs.UID, "worker", pcs.Namespace, 0).WithReplicas(2).Build()
	pclq.Name = "worker"
	require.NoError(t, cl.Create(ctx, pclq))
	for _, name := range []configv1alpha1.SchedulerName{configv1alpha1.SchedulerNameKai, configv1alpha1.SchedulerNameVolcano, configv1alpha1.SchedulerNameLPX} {
		t.Run(string(name), func(t *testing.T) {
			gang := testutils.NewPodGangBuilder("watch-"+string(name), pcs.Namespace).
				WithManaged(true).WithSchedulerName(string(name)).WithPodGroup("worker", 2).Build()
			gang.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(pcs, grovecorev1alpha1.SchemeGroupVersion.WithKind("PodCliqueSet"))}
			gang.Spec.PodGroups[0].PodReferences = []groveschedulerv1alpha1.NamespacedName{}
			require.NoError(t, cl.Create(ctx, gang))
			resource := registry.Get(string(name)).(scheduler.PodGangResourceBackend)
			native := resource.PodGangResource()
			key := client.ObjectKeyFromObject(gang)
			require.Eventually(t, func() bool {
				return cl.Get(ctx, key, native) == nil && metav1.IsControlledBy(native, gang)
			}, watchTimeout, watchInterval)
			uid := native.GetUID()
			switch pg := native.(type) {
			case *kaischedulingv2alpha2.PodGroup:
				pg.Spec.MinMember = ptr.To(int32(999))
			case *volcanov1beta1.PodGroup:
				pg.Spec.MinMember = 999
			}
			require.NoError(t, cl.Update(ctx, native))
			require.Eventually(t, func() bool {
				if cl.Get(ctx, key, native) != nil {
					return false
				}
				switch pg := native.(type) {
				case *kaischedulingv2alpha2.PodGroup:
					return ptr.Deref(pg.Spec.MinMember, 0) == 2
				case *volcanov1beta1.PodGroup:
					return pg.Spec.MinMember == 2
				}
				return false
			}, watchTimeout, watchInterval, "child drift must enqueue its PodGang without a parent edit")
			assert.Equal(t, uid, native.GetUID())
			require.NoError(t, cl.Delete(ctx, native))
			require.Eventually(t, func() bool {
				return cl.Get(ctx, key, native) == nil && native.GetUID() != uid && metav1.IsControlledBy(native, gang)
			}, watchTimeout, watchInterval, "child deletion must reconstruct native policy without a parent edit")
		})
	}
}
