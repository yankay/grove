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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ai-dynamo/grove/operator/e2e/k8s/k8sclient"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const queueCommandTimeout = 15 * time.Second

// WaitForKEDAHPA waits for the real KEDA reconciler to apply its active floor.
func (wm *WorkloadManager) WaitForKEDAHPA(ctx context.Context, scaled *unstructured.Unstructured, kind, target string, floor int32, timeout, interval time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		key := client.ObjectKey{Namespace: scaled.GetNamespace(), Name: "keda-hpa-" + scaled.GetName()}
		if err := wm.cl.Get(ctx, key, hpa); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		return metav1.IsControlledBy(hpa, scaled) && hpa.Spec.MinReplicas != nil &&
			*hpa.Spec.MinReplicas == floor && hpa.Spec.ScaleTargetRef.APIVersion == "grove.io/v1alpha1" &&
			hpa.Spec.ScaleTargetRef.Kind == kind && hpa.Spec.ScaleTargetRef.Name == target, nil
	})
}

// DeleteKEDAScaledObject waits for KEDA to stop its scaling loop before the test
// deletes the queue or Grove workload.
func (wm *WorkloadManager) DeleteKEDAScaledObject(ctx context.Context, scaled *unstructured.Unstructured, timeout, interval time.Duration) error {
	if err := client.IgnoreNotFound(wm.cl.Delete(ctx, scaled)); err != nil {
		return err
	}
	return wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		err := wm.cl.Get(ctx, client.ObjectKeyFromObject(scaled), scaled.DeepCopy())
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	})
}

// WaitForScaleReplicas observes asynchronous writers through the scale subresource.
func (wm *WorkloadManager) WaitForScaleReplicas(ctx context.Context, target client.Object, replicas int32, timeout, interval time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		scale := &autoscalingv1.Scale{}
		if err := wm.cl.SubResource("scale").Get(ctx, target, scale); err != nil {
			return false, err
		}
		return scale.Spec.Replicas == replicas, nil
	})
}

// KEDARedisResources creates isolated, ephemeral queue infrastructure. Its labels
// deliberately exclude Grove's workload selector so it cannot count as a replica.
func KEDARedisResources(namespace, name, image string) (*corev1.Pod, *corev1.Service) {
	labels := map[string]string{"e2e.grove.io/redis": name}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "redis", Image: image,
				Args:  []string{"redis-server", "--save", "", "--appendonly", "no", "--protected-mode", "no"},
				Ports: []corev1.ContainerPort{{Name: "redis", ContainerPort: 6379}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler:  corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"redis-cli", "ping"}}},
					PeriodSeconds: 1,
				},
			}},
		},
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.ServiceSpec{Selector: labels, Ports: []corev1.ServicePort{{
			Name: "redis", Port: 6379, TargetPort: intstr.FromInt32(6379),
		}}},
	}
	return pod, service
}

// SetKEDAQueueLength atomically replaces demand so KEDA cannot observe an
// unintended zero between separate DEL and LPUSH commands.
func SetKEDAQueueLength(ctx context.Context, cl *k8sclient.Client, namespace, pod, queue string, count int) error {
	ctx, cancel := context.WithTimeout(ctx, queueCommandTimeout)
	defer cancel()
	script := `redis.call('DEL', KEYS[1]); for i=1,tonumber(ARGV[1]) do redis.call('LPUSH', KEYS[1], 'work') end; return redis.call('LLEN', KEYS[1])`
	output, err := cl.Exec(ctx, namespace, pod, "redis", []string{"redis-cli", "--raw", "EVAL", script, "1", queue, strconv.Itoa(count)})
	if err != nil {
		return err
	}
	if strings.TrimSpace(output) != strconv.Itoa(count) {
		return fmt.Errorf("queue length: got %q, want %d", output, count)
	}
	return nil
}

// KEDAScaledObject exercises the real KEDA and HPA writers without adding KEDA
// API dependencies to the operator's production module.
func KEDAScaledObject(namespace, name, kind, target, redisAddress string, minReplicas int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "keda.sh/v1alpha1",
		"kind":       "ScaledObject",
		"metadata":   map[string]any{"name": name, "namespace": namespace},
		"spec": map[string]any{
			"scaleTargetRef":  map[string]any{"apiVersion": "grove.io/v1alpha1", "kind": kind, "name": target},
			"pollingInterval": int64(2), "cooldownPeriod": int64(5),
			"minReplicaCount": minReplicas, "idleReplicaCount": int64(0), "maxReplicaCount": int64(4),
			"advanced": map[string]any{"horizontalPodAutoscalerConfig": map[string]any{
				"behavior": map[string]any{
					"scaleDown": map[string]any{"stabilizationWindowSeconds": int64(0)},
					"scaleUp":   map[string]any{"stabilizationWindowSeconds": int64(0)},
				},
			}},
			"triggers": []any{map[string]any{
				"type":     "redis",
				"metadata": map[string]any{"address": redisAddress, "listName": name, "listLength": "1"},
			}},
		},
	}}
}
