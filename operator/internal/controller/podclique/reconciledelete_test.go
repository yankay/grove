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

package podclique

import (
	"context"
	"errors"
	"testing"

	"github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/controller/podclique/expectations"
	"github.com/ai-dynamo/grove/operator/internal/expect"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// TestTriggerDeletionFlow verifies finalizer release once no owned Pods remain.
func TestTriggerDeletionFlow(t *testing.T) {
	tests := []struct {
		name              string
		pclq              *grovecorev1alpha1.PodClique
		expectFinalizer   bool
		expectRequeue     bool
		expectErrors      bool
		seedExpectationOf string
	}{
		{
			name: "removes_finalizer_and_completes",
			pclq: &grovecorev1alpha1.PodClique{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-pclq",
					Namespace:  "default",
					Finalizers: []string{constants.FinalizerPodClique},
				},
			},
			seedExpectationOf: "default/test-pclq/test-pclq-pg-0",
			expectFinalizer:   false,
			expectRequeue:     false,
			expectErrors:      false,
		},
		{
			name: "no_finalizer_is_a_noop",
			pclq: &grovecorev1alpha1.PodClique{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pclq",
					Namespace: "default",
				},
			},
			expectFinalizer: false,
			expectRequeue:   false,
			expectErrors:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, grovecorev1alpha1.AddToScheme(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.pclq).
				Build()

			expectationsStore := expect.NewExpectationsStore()
			require.NoError(t, expectationsStore.AddIndexers(expectations.PodCliqueExpectationsIndexers()))
			if tc.seedExpectationOf != "" {
				require.NoError(t, expectationsStore.ExpectCreations(logr.Discard(), tc.seedExpectationOf, "uid-1"))
			}

			r := &Reconciler{
				client:            fakeClient,
				apiReader:         fakeClient,
				expectationsStore: expectationsStore,
			}

			result := r.triggerDeletionFlow(context.Background(), logr.Discard(), tc.pclq)

			assert.Equal(t, tc.expectRequeue, result.NeedsRequeue())
			assert.Equal(t, tc.expectErrors, result.HasErrors())

			if tc.seedExpectationOf != "" {
				_, exists, err := expectationsStore.GetExpectations(tc.seedExpectationOf)
				require.NoError(t, err)
				assert.False(t, exists, "expectations entry should have been cleared")
			}

			fetched := &grovecorev1alpha1.PodClique{}
			err := fakeClient.Get(context.Background(), types.NamespacedName{Name: tc.pclq.Name, Namespace: tc.pclq.Namespace}, fetched)
			require.NoError(t, err)
			if tc.expectFinalizer {
				assert.Contains(t, fetched.Finalizers, constants.FinalizerPodClique)
			} else {
				assert.NotContains(t, fetched.Finalizers, constants.FinalizerPodClique)
			}
		})
	}
}

func TestDeletionWaitsForOwnedPodsAcrossRestart(t *testing.T) {
	ctx := context.Background()
	pclq := &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{
		Name: "worker", Namespace: "default", UID: "clique-uid",
		Finalizers: []string{constants.FinalizerPodClique},
	}}
	owned := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "worker-0", Namespace: pclq.Namespace, UID: "pod-uid",
		Finalizers: []string{"example.com/hold"},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: grovecorev1alpha1.SchemeGroupVersion.String(), Kind: constants.KindPodClique,
			Name: pclq.Name, UID: pclq.UID, Controller: ptr.To(true),
		}},
	}}
	foreign := owned.DeepCopy()
	foreign.Name, foreign.UID = "foreign", "foreign-uid"
	foreign.OwnerReferences[0].UID = "other-clique-uid"
	scheme := runtime.NewScheme()
	require.NoError(t, grovecorev1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pclq, owned, foreign).Build()
	require.NoError(t, cl.Delete(ctx, pclq))
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pclq), pclq))
	// The informer has not observed the Pod. Finalizer release must use the API.
	cached := interceptor.NewClient(cl, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if pods, ok := list.(*corev1.PodList); ok {
				pods.Items = nil
				return nil
			}
			return c.List(ctx, list, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			options := (&client.DeleteOptions{}).ApplyOptions(opts)
			require.NotNil(t, options.Preconditions)
			require.Equal(t, &owned.UID, options.Preconditions.UID)
			return c.Delete(ctx, obj, opts...)
		},
	})
	for range 2 {
		r := &Reconciler{client: cached, apiReader: cl, expectationsStore: expect.NewExpectationsStore()}
		require.NoError(t, r.expectationsStore.AddIndexers(componentutils.PodCliqueExpectationsIndexers()))
		result := r.triggerDeletionFlow(ctx, logr.Discard(), pclq)
		require.False(t, result.HasErrors())
		require.True(t, result.NeedsRequeue())
		require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pclq), pclq))
		require.Contains(t, pclq.Finalizers, constants.FinalizerPodClique)
		require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(owned), owned))
		require.False(t, owned.DeletionTimestamp.IsZero())
	}
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(foreign), foreign))
	require.True(t, foreign.DeletionTimestamp.IsZero())
	owned.Finalizers = nil
	require.NoError(t, cl.Update(ctx, owned))
	r := &Reconciler{client: cl, apiReader: cl, expectationsStore: expect.NewExpectationsStore()}
	require.NoError(t, r.expectationsStore.AddIndexers(componentutils.PodCliqueExpectationsIndexers()))
	result := r.triggerDeletionFlow(ctx, logr.Discard(), pclq)
	require.False(t, result.HasErrors())
	require.False(t, result.NeedsRequeue())
	require.True(t, apierrors.IsNotFound(cl.Get(ctx, client.ObjectKeyFromObject(pclq), pclq)))
}

func TestDeletionRetainsFinalizerOnPodErrors(t *testing.T) {
	for _, method := range []testutils.ClientMethod{testutils.ClientMethodList, testutils.ClientMethodDelete} {
		t.Run(string(method), func(t *testing.T) {
			ctx := context.Background()
			pclq := &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{
				Name: "worker", Namespace: "default", UID: "clique-uid",
				Finalizers: []string{constants.FinalizerPodClique},
			}}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "worker-0", Namespace: pclq.Namespace, UID: "pod-uid",
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: grovecorev1alpha1.SchemeGroupVersion.String(), Kind: constants.KindPodClique,
					Name: pclq.Name, UID: pclq.UID, Controller: ptr.To(true),
				}},
			}}
			scheme := runtime.NewScheme()
			require.NoError(t, grovecorev1alpha1.AddToScheme(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pclq, pod).Build()
			failure := errors.New("API unavailable")
			intercepted := interceptor.NewClient(cl, interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if method == testutils.ClientMethodList {
						return failure
					}
					return c.List(ctx, list, opts...)
				},
				Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error { return failure },
			})
			r := &Reconciler{client: intercepted, apiReader: intercepted, expectationsStore: expect.NewExpectationsStore()}
			require.NoError(t, r.expectationsStore.AddIndexers(componentutils.PodCliqueExpectationsIndexers()))
			result := r.triggerDeletionFlow(ctx, logr.Discard(), pclq)
			require.True(t, result.HasErrors())
			_, err := result.Result()
			require.ErrorIs(t, err, failure)
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pclq), pclq))
			require.Contains(t, pclq.Finalizers, constants.FinalizerPodClique)
		})
	}
}

func TestDeletionPreservesOrphanedPods(t *testing.T) {
	ctx := context.Background()
	pclq := &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{
		Name: "worker", Namespace: "default", UID: "clique-uid",
		Finalizers: []string{constants.FinalizerPodClique, metav1.FinalizerOrphanDependents},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "worker-0", Namespace: pclq.Namespace, UID: "pod-uid",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: grovecorev1alpha1.SchemeGroupVersion.String(), Kind: constants.KindPodClique,
			Name: pclq.Name, UID: pclq.UID, Controller: ptr.To(true),
		}},
	}}
	cl := testutils.NewTestClientBuilder().WithObjects(pclq, pod).Build()
	require.NoError(t, cl.Delete(ctx, pclq))
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pclq), pclq))
	r := &Reconciler{client: cl, apiReader: cl, expectationsStore: expect.NewExpectationsStore()}
	require.NoError(t, r.expectationsStore.AddIndexers(componentutils.PodCliqueExpectationsIndexers()))
	result := r.triggerDeletionFlow(ctx, logr.Discard(), pclq)
	require.False(t, result.HasErrors())
	require.False(t, result.NeedsRequeue())
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pclq), pclq))
	require.NotContains(t, pclq.Finalizers, constants.FinalizerPodClique)
	require.Contains(t, pclq.Finalizers, metav1.FinalizerOrphanDependents)
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pod), pod))
	require.True(t, pod.DeletionTimestamp.IsZero(), "orphan propagation must not delete Pods")
}
