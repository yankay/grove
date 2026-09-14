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

package workload

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	kedaWaitTestTimeout  = time.Second
	kedaWaitTestInterval = time.Millisecond
)

func TestWaitForScaleReplicasObservesDesiredNotReadyCount(t *testing.T) {
	target := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "test"}}
	observations := []autoscalingv1.Scale{
		{Spec: autoscalingv1.ScaleSpec{Replicas: 0}, Status: autoscalingv1.ScaleStatus{Replicas: 2}},
		{Spec: autoscalingv1.ScaleSpec{Replicas: 2}, Status: autoscalingv1.ScaleStatus{Replicas: 0}},
	}
	polls := 0
	cl := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		SubResourceGet: func(_ context.Context, _ client.Client, name string, obj, sub client.Object, _ ...client.SubResourceGetOption) error {
			require.Equal(t, "scale", name)
			require.Equal(t, client.ObjectKeyFromObject(target), client.ObjectKeyFromObject(obj))
			*sub.(*autoscalingv1.Scale) = observations[min(polls, len(observations)-1)]
			polls++
			return nil
		},
	}).Build()
	wm := &WorkloadManager{cl: cl}
	require.NoError(t, wm.WaitForScaleReplicas(context.Background(), target, 2, kedaWaitTestTimeout, kedaWaitTestInterval))
	require.Equal(t, 2, polls)
}

func TestWaitForKEDAHPARequiresObservedFloorAndOwnedTarget(t *testing.T) {
	scaled := KEDAScaledObject("test", "demand", "PodClique", "target", "redis:6379", 2)
	scaled.SetUID("scaled-object-uid")
	desired := autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name: "keda-hpa-demand", Namespace: "test",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(scaled, scaled.GroupVersionKind())},
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			MinReplicas: ptr.To(int32(2)),
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "grove.io/v1alpha1", Kind: "PodClique", Name: "target",
			},
		},
	}
	observations := make([]autoscalingv2.HorizontalPodAutoscaler, 5)
	for i := range observations {
		observations[i] = *desired.DeepCopy()
	}
	observations[0].OwnerReferences[0].UID = "unrelated-owner"
	observations[1].Spec.ScaleTargetRef.Name = "unrelated-target"
	observations[2].Spec.MinReplicas = ptr.To(int32(1))
	observations[3].Spec.ScaleTargetRef.APIVersion = "another.io/v1alpha1"
	polls := 0
	cl := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
			require.Equal(t, client.ObjectKeyFromObject(&desired), key)
			*obj.(*autoscalingv2.HorizontalPodAutoscaler) = observations[min(polls, len(observations)-1)]
			polls++
			return nil
		},
	}).Build()
	wm := &WorkloadManager{cl: cl}
	require.NoError(t, wm.WaitForKEDAHPA(context.Background(), scaled, "PodClique", "target", 2,
		kedaWaitTestTimeout, kedaWaitTestInterval))
	require.Equal(t, len(observations), polls)
}
