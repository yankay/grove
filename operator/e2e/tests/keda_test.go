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

package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/e2e/grove/workload"
	"github.com/ai-dynamo/grove/operator/e2e/k8s/pods"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const kedaIdleObservationWindow = 20 * time.Second

func Test_KEDA_ActiveFloorAndHibernation(t *testing.T) {
	if os.Getenv("GROVE_E2E_KEDA") != "true" {
		t.Skip("GROVE_E2E_KEDA=true requires KEDA installed in the test cluster")
	}
	for _, kind := range []string{"PodClique", "PodCliqueScalingGroup"} {
		t.Run(kind, func(t *testing.T) {
			const pcsName = "keda-hibernation"
			const queue = "grove-demand"
			ctx := context.Background()
			tc, cleanup := prepareIdleWorkload(t, ctx, 6, pcsName, 1, func(pcs *grovecorev1alpha1.PodCliqueSet) {
				idlePCSGConfig(t, pcs).MinAvailable = ptr.To(int32(2))
			})
			defer cleanup()
			wm := workload.NewWorkloadManager(tc.Client, Logger)
			waitForPodCountAndReady(t, tc, 1)
			survivors := podUIDLocations(podsForClique(t, tc, pcsName+"-0-worker"))
			var target client.Object
			memberCount := 1
			if kind == "PodClique" {
				target = waitForPCLQ(t, ctx, tc, pcsName+"-0-guarded")
			} else {
				target = &grovecorev1alpha1.PodCliqueScalingGroup{ObjectMeta: metav1.ObjectMeta{
					Name: pcsName + "-0-workers", Namespace: tc.Namespace,
				}}
				if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(target), target); err != nil {
					t.Fatal(err)
				}
				memberCount = 2
			}
			redisImage := os.Getenv("GROVE_E2E_REDIS_IMAGE")
			if redisImage == "" {
				redisImage = "redis:7.4.2"
			}
			redis, service := workload.KEDARedisResources(tc.Namespace, queue, redisImage)
			for _, resource := range []client.Object{redis, service} {
				if err := tc.Client.Create(ctx, resource); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := client.IgnoreNotFound(tc.Client.Delete(ctx, resource, client.GracePeriodSeconds(0))); err != nil {
						t.Errorf("cleanup queue resource: %v", err)
					}
				}()
			}
			if err := pods.NewPodManager(tc.Client, Logger).WaitForReady(ctx, []string{tc.Namespace},
				"e2e.grove.io/redis="+queue, 1, tc.Timeout, tc.Interval); err != nil {
				t.Fatal(err)
			}
			scaled := workload.KEDAScaledObject(tc.Namespace, queue, kind, target.GetName(),
				fmt.Sprintf("%s.%s.svc:6379", queue, tc.Namespace), 2)
			if err := tc.Client.Create(ctx, scaled); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := wm.DeleteKEDAScaledObject(ctx, scaled, tc.Timeout, tc.Interval); err != nil {
					t.Errorf("ScaledObject cleanup: %v", err)
				}
			}()
			if err := wm.WaitForKEDAHPA(ctx, scaled, kind, target.GetName(), 2, tc.Timeout, tc.Interval); err != nil {
				t.Fatalf("KEDA did not create the expected HPA: %v", err)
			}
			for cycle := range 2 {
				for _, demand := range []int{1, 4, 1, 0} {
					expected := int32(demand)
					if demand == 1 {
						expected = 2
					}
					t.Logf("cycle %d: demand %d, target %d", cycle, demand, expected)
					if err := workload.SetKEDAQueueLength(ctx, tc.Client, tc.Namespace, queue, queue, demand); err != nil {
						t.Fatal(err)
					}
					if err := wm.WaitForScaleReplicas(ctx, target, expected, tc.Timeout, tc.Interval); err != nil {
						t.Fatalf("KEDA target did not reach %d: %v", expected, err)
					}
					waitForPodCountAndReady(t, tc, 1+int(expected)*memberCount)
					assertScaleStatus(t, ctx, tc, target, expected, true)
					assertRetainedPodLocation(t, podsForClique(t, tc, pcsName+"-0-worker"), survivors)
					current := target.DeepCopyObject().(client.Object)
					if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(target), current); err != nil {
						t.Fatal(err)
					}
					if current.GetUID() != target.GetUID() {
						t.Fatal("autoscaling replaced its scale target")
					}
					if demand == 0 {
						restartOperator(t, ctx, tc)
						if cycle == 0 {
							kedaNamespace := os.Getenv("GROVE_E2E_KEDA_NAMESPACE")
							if kedaNamespace == "" {
								kedaNamespace = "keda"
							}
							if err := pods.NewPodManager(tc.Client, Logger).RestartAndWait(ctx, kedaNamespace,
								"app.kubernetes.io/name=keda-operator", tc.Timeout, tc.Interval); err != nil {
								t.Fatalf("KEDA did not recover after restart: %v", err)
							}
						}
						assertStableFor(t, ctx, kedaIdleObservationWindow, func(ctx context.Context) error {
							scale := &autoscalingv1.Scale{}
							if err := tc.Client.SubResource("scale").Get(ctx, target, scale); err != nil {
								return err
							}
							if scale.Spec.Replicas != 0 || scale.Status.Replicas != 0 {
								return fmt.Errorf("KEDA idle target bounced to %+v", scale)
							}
							return nil
						})
					}
				}
			}
			// A misconfigured writer must see rejection, not a persisted
			// below-quorum replica target. Restore the compatible floor afterward.
			if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(scaled), scaled); err != nil {
				t.Fatal(err)
			}
			before := scaled.DeepCopy()
			if err := unstructured.SetNestedField(scaled.Object, int64(1), "spec", "minReplicaCount"); err != nil {
				t.Fatal(err)
			}
			if err := tc.Client.Patch(ctx, scaled, client.MergeFrom(before)); err != nil {
				t.Fatal(err)
			}
			if err := wm.WaitForKEDAHPA(ctx, scaled, kind, target.GetName(), 1, tc.Timeout, tc.Interval); err != nil {
				t.Fatalf("KEDA did not observe the incompatible active floor: %v", err)
			}
			if err := workload.SetKEDAQueueLength(ctx, tc.Client, tc.Namespace, queue, queue, 1); err != nil {
				t.Fatal(err)
			}
			if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
				events := &corev1.EventList{}
				if err := tc.Client.List(ctx, events, client.InNamespace(tc.Namespace)); err != nil {
					return false, err
				}
				for _, event := range events.Items {
					if event.InvolvedObject.UID == scaled.GetUID() && event.Reason == "KEDAScaleTargetActivationFailed" {
						return true, nil
					}
				}
				return false, nil
			}); err != nil {
				t.Fatalf("misconfigured active floor did not surface activation failure: %v", err)
			}
			assertScaleStatus(t, ctx, tc, target, 0, true)
			if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(scaled), scaled); err != nil {
				t.Fatal(err)
			}
			before = scaled.DeepCopy()
			if err := unstructured.SetNestedField(scaled.Object, int64(2), "spec", "minReplicaCount"); err != nil {
				t.Fatal(err)
			}
			if err := tc.Client.Patch(ctx, scaled, client.MergeFrom(before)); err != nil {
				t.Fatal(err)
			}
			if err := wm.WaitForScaleReplicas(ctx, target, 2, tc.Timeout, tc.Interval); err != nil {
				t.Fatalf("corrected KEDA active floor did not wake target: %v", err)
			}
			waitForPodCountAndReady(t, tc, 1+2*memberCount)
		})
	}
}
