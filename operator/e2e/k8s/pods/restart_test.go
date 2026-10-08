//go:build e2e

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

package pods

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	restartTestTimeout  = time.Second
	restartTestInterval = time.Millisecond
)

func TestRestartAndWaitRequiresExplicitSingleTarget(t *testing.T) {
	for _, test := range []struct {
		name, namespace, selector string
		count                     int
	}{
		{name: "missing namespace", selector: "app=operator", count: 1},
		{name: "missing selector", namespace: "test", count: 1},
		{name: "no target", namespace: "test", selector: "app=operator"},
		{name: "multiple targets", namespace: "test", selector: "app=operator", count: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			deletes := 0
			cl := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
				List: func(_ context.Context, _ client.WithWatch, list client.ObjectList, _ ...client.ListOption) error {
					list.(*corev1.PodList).Items = make([]corev1.Pod, test.count)
					return nil
				},
				Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.DeleteOption) error {
					deletes++
					return nil
				},
			}).Build()
			err := NewPodManager(cl, nil).RestartAndWait(context.Background(), test.namespace,
				test.selector, restartTestTimeout, restartTestInterval)
			require.Error(t, err)
			require.Zero(t, deletes)
		})
	}
}

func TestRestartAndWaitRequiresOldRemovalAndNewReadiness(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	old := corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "old", Namespace: "test", UID: "old-uid", Labels: map[string]string{"app": "operator"},
	}}
	ready := old.DeepCopy()
	ready.Name, ready.UID = "new", "new-uid"
	ready.Status.Phase = corev1.PodRunning
	ready.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	unready := ready.DeepCopy()
	unready.Status.Conditions[0].Status = corev1.ConditionFalse
	terminating := ready.DeepCopy()
	now := metav1.Now()
	terminating.DeletionTimestamp = &now
	observations := [][]corev1.Pod{{old, *ready}, {*unready}, {*terminating}, {*ready}}
	polls, deletes := 0, 0
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&old).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if deletes == 0 {
					return cl.List(ctx, list, opts...)
				}
				options := (&client.ListOptions{}).ApplyOptions(opts)
				require.Equal(t, "test", options.Namespace)
				require.Equal(t, "app=operator", options.Raw.LabelSelector)
				list.(*corev1.PodList).Items = observations[min(polls, len(observations)-1)]
				polls++
				return nil
			},
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				deletes++
				options := (&client.DeleteOptions{}).ApplyOptions(opts)
				require.NotNil(t, options.Preconditions)
				require.Equal(t, old.UID, *options.Preconditions.UID)
				return cl.Delete(ctx, obj, opts...)
			},
		}).Build()
	require.NoError(t, NewPodManager(cl, nil).RestartAndWait(context.Background(), "test",
		"app=operator", restartTestTimeout, restartTestInterval))
	require.Equal(t, 1, deletes)
	require.Equal(t, len(observations), polls)
}
