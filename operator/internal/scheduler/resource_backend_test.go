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

package scheduler_test

import (
	"context"
	"errors"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"
	"github.com/ai-dynamo/grove/operator/internal/scheduler/kai"
	"github.com/ai-dynamo/grove/operator/internal/scheduler/volcano"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"
	schedulertest "github.com/ai-dynamo/grove/operator/test/utils/scheduler"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	kaischedulingv2alpha2 "github.com/kai-scheduler/KAI-scheduler/pkg/apis/scheduling/v2alpha2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	volcanov1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

func TestResourceBackendMembershipHandoff(t *testing.T) {
	for _, name := range []configv1alpha1.SchedulerName{configv1alpha1.SchedulerNameKai, configv1alpha1.SchedulerNameVolcano} {
		t.Run(string(name), func(t *testing.T) {
			ctx := context.Background()
			cl, gang := resourceBackendFixture(t, name)
			backend := newResourceBackend(cl, name)
			resource := backend.(scheduler.PodGangResourceBackend)
			synced, err := resource.IsPodGangSynced(ctx, gang)
			require.NoError(t, err)
			assert.False(t, synced, "missing native resource must hold scheduling gates")
			require.NoError(t, backend.SyncPodGang(ctx, gang))

			native := resource.PodGangResource()
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(gang), native))
			native.SetUID("native-uid")
			native.SetAnnotations(map[string]string{"scheduler.example/runtime": "keep"})
			require.NoError(t, cl.Update(ctx, native))
			assertNativeMinimum(t, native, 2, map[string]int32{"router": 1, "worker": 1})

			for _, groups := range [][]groveschedulerv1alpha1.PodGroup{
				{{Name: "router", MinReplicas: 1}},
				{{Name: "router", MinReplicas: 1}, {Name: "worker", MinReplicas: 2}},
				{{Name: "router", MinReplicas: 1}, {Name: "replacement", MinReplicas: 2}},
			} {
				gang.Spec.PodGroups = groups
				synced, err = resource.IsPodGangSynced(ctx, gang)
				require.NoError(t, err)
				assert.False(t, synced, "membership changes, including equal totals, must wait for native policy")
				require.NoError(t, backend.SyncPodGang(ctx, gang))
				synced, err = resource.IsPodGangSynced(ctx, gang)
				require.NoError(t, err)
				require.True(t, synced)
				require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(gang), native))
				minima := make(map[string]int32, len(groups))
				var total int32
				for _, group := range groups {
					minima[group.Name] = group.MinReplicas
					total += group.MinReplicas
				}
				assertNativeMinimum(t, native, total, minima)
				assert.Equal(t, "keep", native.GetAnnotations()["scheduler.example/runtime"])
				assert.EqualValues(t, "native-uid", native.GetUID())
				assert.True(t, metav1.IsControlledBy(native, gang))
				rv := native.GetResourceVersion()
				require.NoError(t, backend.SyncPodGang(ctx, gang))
				require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(gang), native))
				assert.Equal(t, rv, native.GetResourceVersion(), "an in-sync policy must not generate a write")
			}

			require.NoError(t, cl.Delete(ctx, native))
			synced, err = resource.IsPodGangSynced(ctx, gang)
			require.NoError(t, err)
			assert.False(t, synced)
			require.NoError(t, backend.SyncPodGang(ctx, gang))
			synced, err = resource.IsPodGangSynced(ctx, gang)
			require.NoError(t, err)
			assert.True(t, synced, "deleted native resource can be reconstructed")
		})
	}
}

func TestResourceBackendRejectsUnsafeChildren(t *testing.T) {
	for _, name := range []configv1alpha1.SchedulerName{configv1alpha1.SchedulerNameKai, configv1alpha1.SchedulerNameVolcano} {
		for _, state := range []string{"unowned", "old-owner", "terminating"} {
			t.Run(string(name)+"/"+state, func(t *testing.T) {
				ctx := context.Background()
				cl, gang := resourceBackendFixture(t, name)
				backend := newResourceBackend(cl, name)
				resource := backend.(scheduler.PodGangResourceBackend)
				require.NoError(t, backend.SyncPodGang(ctx, gang))
				native := resource.PodGangResource()
				require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(gang), native))
				switch state {
				case "unowned":
					native.SetOwnerReferences(nil)
				case "old-owner":
					owners := native.GetOwnerReferences()
					owners[0].UID = "old-gang"
					native.SetOwnerReferences(owners)
				case "terminating":
					native.SetFinalizers([]string{"test.grove.io/hold"})
				}
				require.NoError(t, cl.Update(ctx, native))
				if state == "terminating" {
					require.NoError(t, cl.Delete(ctx, native))
				}
				synced, err := resource.IsPodGangSynced(ctx, gang)
				require.NoError(t, err)
				assert.False(t, synced)
				require.Error(t, backend.SyncPodGang(ctx, gang), "must neither adopt nor mutate an unsafe native child")
			})
		}
	}
}

func TestResourceBackendOptimisticHandoff(t *testing.T) {
	for _, name := range []configv1alpha1.SchedulerName{configv1alpha1.SchedulerNameKai, configv1alpha1.SchedulerNameVolcano} {
		t.Run(string(name), func(t *testing.T) {
			ctx := context.Background()
			cl, gang := resourceBackendFixture(t, name)
			backend := newResourceBackend(cl, name)
			require.NoError(t, backend.SyncPodGang(ctx, gang))
			gang.Spec.PodGroups[1].MinReplicas = 3
			var writes int
			intercepted := interceptor.NewClient(cl, interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					writes++
					current := obj.DeepCopyObject().(client.Object)
					require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), current))
					current.SetLabels(map[string]string{"scheduler.example/runtime": "concurrent"})
					require.NoError(t, c.Update(ctx, current))
					return c.Patch(ctx, obj, patch, opts...)
				},
			})
			conflicting := newResourceBackend(intercepted, name)
			err := conflicting.SyncPodGang(ctx, gang)
			require.True(t, apierrors.IsConflict(err), "concurrent native update must conflict: %v", err)
			assert.Equal(t, 1, writes)
			resource := backend.(scheduler.PodGangResourceBackend)
			synced, err := resource.IsPodGangSynced(ctx, gang)
			require.NoError(t, err)
			assert.False(t, synced)
			require.NoError(t, backend.SyncPodGang(ctx, gang))
			native := resource.PodGangResource()
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(gang), native))
			assert.Equal(t, "concurrent", native.GetLabels()["scheduler.example/runtime"])
			assertNativeMinimum(t, native, 4, map[string]int32{"router": 1, "worker": 3})
		})
	}
}

func TestResourceBackendReadFailure(t *testing.T) {
	for _, name := range []configv1alpha1.SchedulerName{configv1alpha1.SchedulerNameKai, configv1alpha1.SchedulerNameVolcano} {
		t.Run(string(name), func(t *testing.T) {
			ctx := context.Background()
			cl, gang := resourceBackendFixture(t, name)
			readErr := errors.New("native API unavailable")
			intercepted := interceptor.NewClient(cl, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					switch obj.(type) {
					case *kaischedulingv2alpha2.PodGroup, *volcanov1beta1.PodGroup:
						return readErr
					default:
						return c.Get(ctx, key, obj, opts...)
					}
				},
			})
			backend := newResourceBackend(intercepted, name)
			synced, err := backend.(scheduler.PodGangResourceBackend).IsPodGangSynced(ctx, gang)
			require.ErrorIs(t, err, readErr)
			assert.False(t, synced)
			require.ErrorIs(t, backend.SyncPodGang(ctx, gang), readErr)
		})
	}
}

func resourceBackendFixture(t *testing.T, name configv1alpha1.SchedulerName) (client.WithWatch, *groveschedulerv1alpha1.PodGang) {
	t.Helper()
	pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").Build()
	pcs.Labels = map[string]string{"kai.scheduler/queue": "team"}
	gang := testutils.NewPodGangBuilder("anchor", pcs.Namespace).WithLastScheduled().
		WithSchedulerName(string(name)).WithPodGroup("router", 1).WithPodGroup("worker", 1).Build()
	gang.UID = "gang-uid"
	gang.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(pcs, grovecorev1alpha1.SchemeGroupVersion.WithKind("PodCliqueSet"))}
	gang.Annotations = map[string]string{"kai.scheduler/skip-podgrouper": "true"}
	scheme := schedulertest.NewKAIScheme(t)
	require.NoError(t, volcanov1beta1.AddToScheme(scheme))
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pcs, gang).
		WithStatusSubresource(&kaischedulingv2alpha2.PodGroup{}, &volcanov1beta1.PodGroup{}).Build()
	return cl, gang
}

func newResourceBackend(cl client.Client, name configv1alpha1.SchedulerName) scheduler.Backend {
	profile := configv1alpha1.SchedulerProfile{Name: name}
	if name == configv1alpha1.SchedulerNameKai {
		return kai.New(cl, cl.Scheme(), nil, profile)
	}
	return volcano.New(cl, cl.Scheme(), nil, profile)
}

func assertNativeMinimum(t *testing.T, obj client.Object, total int32, minima map[string]int32) {
	t.Helper()
	got := make(map[string]int32)
	switch native := obj.(type) {
	case *kaischedulingv2alpha2.PodGroup:
		assert.Equal(t, total, ptr.Deref(native.Spec.MinMember, -1))
		for _, group := range native.Spec.SubGroups {
			got[group.Name] = ptr.Deref(group.MinMember, -1)
		}
	case *volcanov1beta1.PodGroup:
		assert.Equal(t, total, native.Spec.MinMember)
		for _, group := range native.Spec.SubGroupPolicy {
			assert.Equal(t, map[string]string{apicommon.LabelPodClique: group.Name}, group.LabelSelector.MatchLabels)
			assert.Equal(t, []string{apicommon.LabelPodClique}, group.MatchLabelKeys)
			assert.Equal(t, int32(1), ptr.Deref(group.MinSubGroups, -1))
			got[group.Name] = ptr.Deref(group.SubGroupSize, -1)
		}
	default:
		t.Fatalf("unexpected backend resource %T", obj)
	}
	assert.Equal(t, minima, got)
}
