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
	"github.com/ai-dynamo/grove/operator/e2e/k8s"
	"github.com/ai-dynamo/grove/operator/e2e/k8s/pods"
	"github.com/ai-dynamo/grove/operator/e2e/testctx"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	kedaIdleObservationWindow = 20 * time.Second
	kedaMetricFailureTimeout  = 2 * time.Minute
	kedaPCSName               = "keda-hibernation"
	kedaQueueName             = "grove-demand"
)

func Test_KEDA_ActiveFloorAndHibernation(t *testing.T) {
	if os.Getenv("GROVE_E2E_KEDA") != "true" {
		t.Skip("GROVE_E2E_KEDA=true requires KEDA installed in the test cluster")
	}
	for _, kind := range []string{"PodClique", "PodCliqueScalingGroup"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			tc, target, scaled, memberCount := prepareKEDAWorkload(ctx, t, kind)
			survivors := podUIDLocations(podsForClique(t, tc, kedaPCSName+"-0-worker"))
			for cycle := range 2 {
				for _, demand := range []int{1, 4, 1, 0} {
					expected := int32(demand)
					if demand == 1 {
						expected = 2
					}
					t.Logf("cycle %d: demand %d, target %d", cycle, demand, expected)
					setKEDADemandAndWait(ctx, t, tc, target, memberCount, demand, expected)
					assertRetainedPodLocation(t, podsForClique(t, tc, kedaPCSName+"-0-worker"), survivors)
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
			setKEDAActiveFloor(ctx, t, tc, scaled, kind, target.GetName(), 1)
			if err := workload.SetKEDAQueueLength(ctx, tc.Client, tc.Namespace, kedaQueueName, kedaQueueName, 1); err != nil {
				t.Fatal(err)
			}
			if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
				attempts, err := k8s.CountWarningEventOccurrences(ctx, tc.Client, scaled, "KEDAScaleTargetActivationFailed")
				return attempts >= 1, err
			}); err != nil {
				t.Fatalf("misconfigured active floor did not surface activation failure: %v", err)
			}
			assertScaleStatus(t, ctx, tc, target, 0, true)
			setKEDAActiveFloor(ctx, t, tc, scaled, kind, target.GetName(), 2)
			setKEDADemandAndWait(ctx, t, tc, target, memberCount, 1, 2)
		})
	}
}

func Test_KEDA_ActiveDownscaleRejected(t *testing.T) {
	for _, kind := range []string{"PodClique", "PodCliqueScalingGroup"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			tc, target, scaled, memberCount := prepareKEDAWorkload(ctx, t, kind)
			wm := workload.NewWorkloadManager(tc.Client, Logger)
			setKEDADemandAndWait(ctx, t, tc, target, memberCount, 1, 2)
			beforePods, err := tc.ListPods()
			if err != nil {
				t.Fatal(err)
			}
			beforeLocations := podUIDLocations(beforePods.Items)
			// HPA failure conditions also cover transient server errors. Verify
			// the below-quorum value is rejected specifically by API validation.
			invalidScale := &autoscalingv1.Scale{
				ObjectMeta: metav1.ObjectMeta{Name: target.GetName(), Namespace: tc.Namespace},
				Spec:       autoscalingv1.ScaleSpec{Replicas: 1},
			}
			requireInvalidCause(t, tc.Client.SubResource("scale").Update(ctx, target, client.WithSubResourceBody(invalidScale)), "spec")
			assertScaleStatus(t, ctx, tc, target, 2, true)
			setKEDAActiveFloor(ctx, t, tc, scaled, kind, target.GetName(), 1)
			hpa := &autoscalingv2.HorizontalPodAutoscaler{}
			hpaKey := client.ObjectKey{Namespace: tc.Namespace, Name: "keda-hpa-" + scaled.GetName()}
			if err := tc.Client.Get(ctx, hpaKey, hpa); err != nil {
				t.Fatal(err)
			}
			if err := wm.WaitForHPACondition(ctx, hpaKey, autoscalingv2.AbleToScale, corev1.ConditionFalse,
				"FailedUpdateScale", tc.Timeout, tc.Interval); err != nil {
				t.Fatalf("HPA did not report the rejected active downscale: %v", err)
			}
			if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
				attempts, err := k8s.CountWarningEventOccurrences(ctx, tc.Client, hpa, "FailedRescale")
				assertScaleStatus(t, ctx, tc, target, 2, true)
				return attempts >= 2, err
			}); err != nil {
				t.Fatalf("HPA did not retry the rejected downscale: %v", err)
			}
			waitForPodCountAndReady(t, tc, 1+2*memberCount)
			afterPods, err := tc.ListPods()
			if err != nil {
				t.Fatal(err)
			}
			assertRetainedPodLocation(t, afterPods.Items, beforeLocations)
			setKEDAActiveFloor(ctx, t, tc, scaled, kind, target.GetName(), 2)
			setKEDADemandAndWait(ctx, t, tc, target, memberCount, 4, 4)
			setKEDADemandAndWait(ctx, t, tc, target, memberCount, 0, 0)
		})
	}
}

func Test_KEDA_MetricFailureRecovery(t *testing.T) {
	for _, kind := range []string{"PodClique", "PodCliqueScalingGroup"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			tc, target, scaled, memberCount := prepareKEDAWorkload(ctx, t, kind)
			wm := workload.NewWorkloadManager(tc.Client, Logger)
			for _, replicas := range []int32{4, 0} {
				t.Run(fmt.Sprintf("replicas-%d", replicas), func(t *testing.T) {
					setKEDADemandAndWait(ctx, t, tc, target, memberCount, int(replicas), replicas)
					beforePods, err := tc.ListPods()
					if err != nil {
						t.Fatal(err)
					}
					beforeLocations := podUIDLocations(beforePods.Items)
					// Deny only the scaler's LLEN operation. Changing the trigger
					// address would test configuration validation, not read failure.
					faultCtx, cancel := context.WithTimeout(ctx, kedaMetricFailureTimeout)
					defer cancel()
					if err := workload.SetKEDAQueueReadable(faultCtx, tc.Client, tc.Namespace, kedaQueueName, false); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := workload.SetKEDAQueueReadable(ctx, tc.Client, tc.Namespace, kedaQueueName, true); err != nil {
							t.Errorf("restore Redis read permission: %v", err)
						}
					})
					if err := wm.WaitForKEDACondition(faultCtx, scaled, "Ready", metav1.ConditionFalse,
						"TriggerError", tc.Timeout, tc.Interval); err != nil {
						t.Fatalf("KEDA did not observe Redis failure: %v", err)
					}
					if replicas > 0 {
						hpaKey := client.ObjectKey{Namespace: tc.Namespace, Name: "keda-hpa-" + scaled.GetName()}
						if err := wm.WaitForHPACondition(faultCtx, hpaKey, autoscalingv2.ScalingActive, corev1.ConditionFalse,
							"FailedGetExternalMetric", tc.Timeout, tc.Interval); err != nil {
							t.Fatalf("HPA did not observe Redis failure: %v", err)
						}
					}
					assertStableFor(t, faultCtx, kedaIdleObservationWindow, func(ctx context.Context) error {
						assertScaleStatus(t, ctx, tc, target, replicas, true)
						current, err := tc.ListPods()
						if err != nil {
							return err
						}
						if len(current.Items) != len(beforeLocations) {
							return fmt.Errorf("metric failure changed Pod count: got %d, want %d", len(current.Items), len(beforeLocations))
						}
						for _, pod := range current.Items {
							if !pod.DeletionTimestamp.IsZero() {
								return fmt.Errorf("metric failure started deleting Pod %s", pod.Name)
							}
						}
						assertRetainedPodLocation(t, current.Items, beforeLocations)
						return nil
					})
					if err := workload.SetKEDAQueueReadable(ctx, tc.Client, tc.Namespace, kedaQueueName, true); err != nil {
						t.Fatal(err)
					}
					if err := wm.WaitForKEDACondition(ctx, scaled, "Ready", metav1.ConditionTrue,
						"ScaledObjectReady", tc.Timeout, tc.Interval); err != nil {
						t.Fatalf("KEDA did not recover after restoring Redis read permission: %v", err)
					}
					setKEDADemandAndWait(ctx, t, tc, target, memberCount, 1, 2)
				})
			}
		})
	}
}

func prepareKEDAWorkload(ctx context.Context, t *testing.T, kind string) (*testctx.TestContext, client.Object, *unstructured.Unstructured, int) {
	t.Helper()
	if os.Getenv("GROVE_E2E_KEDA") != "true" {
		t.Skip("GROVE_E2E_KEDA=true requires KEDA installed in the test cluster")
	}
	tc, cleanup := prepareIdleWorkload(t, ctx, 6, kedaPCSName, 1, func(pcs *grovecorev1alpha1.PodCliqueSet) {
		idlePCSGConfig(t, pcs).MinAvailable = ptr.To(int32(2))
	})
	t.Cleanup(cleanup)
	waitForPodCountAndReady(t, tc, 1)
	var target client.Object
	memberCount := 1
	if kind == "PodClique" {
		target = waitForPCLQ(t, ctx, tc, kedaPCSName+"-0-guarded")
	} else {
		target = &grovecorev1alpha1.PodCliqueScalingGroup{ObjectMeta: metav1.ObjectMeta{
			Name: kedaPCSName + "-0-workers", Namespace: tc.Namespace,
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
	redis, service := workload.KEDARedisResources(tc.Namespace, kedaQueueName, redisImage)
	for _, resource := range []client.Object{redis, service} {
		if err := tc.Client.Create(ctx, resource); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := client.IgnoreNotFound(tc.Client.Delete(ctx, resource, client.GracePeriodSeconds(0))); err != nil {
				t.Errorf("cleanup queue resource: %v", err)
			}
		})
	}
	if err := pods.NewPodManager(tc.Client, Logger).WaitForReady(ctx, []string{tc.Namespace},
		"e2e.grove.io/redis="+kedaQueueName, 1, tc.Timeout, tc.Interval); err != nil {
		t.Fatal(err)
	}
	scaled := workload.KEDAScaledObject(tc.Namespace, kedaQueueName, kind, target.GetName(),
		fmt.Sprintf("%s.%s.svc:6379", kedaQueueName, tc.Namespace), 2)
	if err := tc.Client.Create(ctx, scaled); err != nil {
		t.Fatal(err)
	}
	wm := workload.NewWorkloadManager(tc.Client, Logger)
	t.Cleanup(func() {
		if err := wm.DeleteKEDAScaledObject(ctx, scaled, tc.Timeout, tc.Interval); err != nil {
			t.Errorf("ScaledObject cleanup: %v", err)
		}
	})
	if err := wm.WaitForKEDAHPA(ctx, scaled, kind, target.GetName(), 2, tc.Timeout, tc.Interval); err != nil {
		t.Fatalf("KEDA did not create the expected HPA: %v", err)
	}
	return tc, target, scaled, memberCount
}

func setKEDAActiveFloor(ctx context.Context, t *testing.T, tc *testctx.TestContext, scaled *unstructured.Unstructured, kind, target string, floor int64) {
	t.Helper()
	if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(scaled), scaled); err != nil {
		t.Fatal(err)
	}
	before := scaled.DeepCopy()
	if err := unstructured.SetNestedField(scaled.Object, floor, "spec", "minReplicaCount"); err != nil {
		t.Fatal(err)
	}
	if err := tc.Client.Patch(ctx, scaled, client.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}
	wm := workload.NewWorkloadManager(tc.Client, Logger)
	if err := wm.WaitForKEDAHPA(ctx, scaled, kind, target, int32(floor), tc.Timeout, tc.Interval); err != nil {
		t.Fatalf("KEDA did not observe active floor %d: %v", floor, err)
	}
}

func setKEDADemandAndWait(ctx context.Context, t *testing.T, tc *testctx.TestContext, target client.Object, memberCount, demand int, replicas int32) {
	t.Helper()
	if err := workload.SetKEDAQueueLength(ctx, tc.Client, tc.Namespace, kedaQueueName, kedaQueueName, demand); err != nil {
		t.Fatal(err)
	}
	wm := workload.NewWorkloadManager(tc.Client, Logger)
	if err := wm.WaitForScaleReplicas(ctx, target, replicas, tc.Timeout, tc.Interval); err != nil {
		t.Fatalf("demand %d did not produce %d replicas: %v", demand, replicas, err)
	}
	waitForPodCountAndReady(t, tc, 1+int(replicas)*memberCount)
	assertScaleStatus(t, ctx, tc, target, replicas, true)
	current := target.DeepCopyObject().(client.Object)
	if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(target), current); err != nil {
		t.Fatal(err)
	}
	if current.GetUID() != target.GetUID() {
		t.Fatal("autoscaling replaced its scale target")
	}
}
