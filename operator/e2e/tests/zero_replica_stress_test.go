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
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/e2e/grove/podgroup"
	"github.com/ai-dynamo/grove/operator/e2e/grove/workload"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func Test_ZR11_RapidPCSGScaleWithRetainedPods(t *testing.T) {
	for _, target := range []int32{2, 0} {
		t.Run(fmt.Sprintf("scale-in-to-%d", target), func(t *testing.T) {
			const pcsName = "zero-rapid-scale"
			ctx := context.Background()
			tc, cleanup := prepareIdleWorkload(t, ctx, 6, pcsName, 1, func(pcs *grovecorev1alpha1.PodCliqueSet) {
				config := idlePCSGConfig(t, pcs)
				config.Replicas, config.MinAvailable = ptr.To(int32(2)), ptr.To(int32(2))
			})
			defer cleanup()
			waitForPodCountAndReady(t, tc, 5)
			group := &grovecorev1alpha1.PodCliqueScalingGroup{}
			if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: pcsName + "-0-workers"}, group); err != nil {
				t.Fatal(err)
			}
			survivors := podUIDLocations(podsForClique(t, tc, pcsName+"-0-worker"))
			updateScale(t, ctx, tc, group, 3)
			waitForPodCountAndReady(t, tc, 7)
			old := podsForPCSGIndex(t, tc, "2")
			oldLocations := podUIDLocations(old)
			held := old[0].DeepCopy()
			heldKey := client.ObjectKeyFromObject(held)
			oldGang := held.Labels[apicommon.LabelPodGang]
			heldOwner := metav1.GetControllerOf(held)
			if heldOwner == nil {
				t.Fatal("old Pod has no controller")
			}
			if err := tc.Client.Patch(ctx, held, client.RawPatch(types.MergePatchType,
				[]byte(`{"metadata":{"finalizers":["e2e.grove.io/hold-rapid-scale"]}}`))); err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = client.IgnoreNotFound(tc.Client.Patch(ctx, held,
					client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"finalizers":[]}}`))))
			}()
			updateScale(t, ctx, tc, group, target)
			if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
				current := &corev1.Pod{}
				if err := tc.Client.Get(ctx, heldKey, current); err != nil {
					return false, err
				}
				pgm := getPGM(t, ctx, tc, pcsName, 0)
				for _, entry := range pgm.Spec.Entries {
					if entry.Role == grovecorev1alpha1.PodGangEntryRoleScaleOut &&
						len(entry.PCSGReplicaIndices["workers"]) == 0 && !current.DeletionTimestamp.IsZero() {
						return true, nil
					}
				}
				return false, nil
			}); err != nil {
				t.Fatalf("real scale-in did not empty the slot and start draining: %v", err)
			}
			// The scale subresource, not a manual PGM edit or synthetic trigger,
			// must drive this second epoch while the old Pod still exists.
			updateScale(t, ctx, tc, group, 3)
			waitForPCSGMembership(t, ctx, tc, pcsName, "workers", []int32{0, 1, 2})
			restartOperator(t, ctx, tc)
			assertStableFor(t, ctx, zeroReplicaObservationWindow, func(ctx context.Context) error {
				current := &corev1.Pod{}
				if err := tc.Client.Get(ctx, heldKey, current); err != nil {
					return err
				}
				if current.UID != held.UID || current.Labels[apicommon.LabelPodGang] != oldGang {
					return fmt.Errorf("retiring Pod was replaced or relabeled before drain")
				}
				member := &grovecorev1alpha1.PodClique{}
				if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: heldOwner.Name}, member); err != nil {
					return err
				}
				if member.UID != heldOwner.UID || member.DeletionTimestamp.IsZero() {
					return fmt.Errorf("retiring member disappeared before its Pod")
				}
				return nil
			})
			if err := tc.Client.Patch(ctx, held, client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"finalizers":[]}}`))); err != nil {
				t.Fatal(err)
			}
			waitForPodCountAndReady(t, tc, 7)
			var newGang string
			for _, pod := range podsForPCSGIndex(t, tc, "2") {
				newGang = pod.Labels[apicommon.LabelPodGang]
				if _, retained := oldLocations[pod.UID]; retained || newGang == oldGang {
					t.Fatalf("scale-out Pod %s reused the old scheduling identity", pod.Name)
				}
			}
			gang := &groveschedulerv1alpha1.PodGang{}
			if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: newGang}, gang); err != nil {
				t.Fatal(err)
			}
			native, err := podgroup.GetNativePodGroup(ctx, tc.Client, gang)
			if err != nil {
				t.Fatal(err)
			}
			if err := podgroup.VerifyFlatMembership(gang, native); err != nil {
				t.Fatal(err)
			}
			current := &grovecorev1alpha1.PodCliqueScalingGroup{}
			if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(group), current); err != nil {
				t.Fatal(err)
			}
			if current.UID != group.UID || current.Spec.Replicas != 3 {
				t.Fatal("scale-in/out or restart replaced the scale target")
			}
			assertRetainedPodLocation(t, podsForClique(t, tc, pcsName+"-0-worker"), survivors)
		})
	}
}

func Test_ZR12_ReconstructIdleTargetsAndMembership(t *testing.T) {
	const pcsName = "zero-reconstruct"
	ctx := context.Background()
	tc, cleanup := prepareIdleWorkload(t, ctx, 3, pcsName, 1, func(pcs *grovecorev1alpha1.PodCliqueSet) {
		idleClique(t, pcs, "guarded").Spec.Replicas = ptr.To[int32](2)
	})
	defer cleanup()
	waitForPodCountAndReady(t, tc, 3)
	guarded := waitForPCLQ(t, ctx, tc, pcsName+"-0-guarded")
	survivors := podUIDLocations(podsForClique(t, tc, pcsName+"-0-worker"))
	updateScale(t, ctx, tc, guarded, 0)
	waitForPodCountAndReady(t, tc, 1)
	pgm := getPGM(t, ctx, tc, pcsName, 0)
	if err := tc.Client.Delete(ctx, pgm); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		current := &grovecorev1alpha1.PodGangMap{}
		if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(pgm), current); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		if current.UID == pgm.UID {
			return false, nil
		}
		for _, entry := range current.Spec.Entries {
			if entry.PodCliques["guarded"] != 0 {
				return false, fmt.Errorf("membership reconstruction reset a live idle target")
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	assertScaleStatus(t, ctx, tc, guarded, 0, true)
	if err := tc.Client.Delete(ctx, guarded); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		current := &grovecorev1alpha1.PodClique{}
		if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(guarded), current); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		return current.UID != guarded.UID && ptr.Deref(current.Spec.Replicas, 1) == 2, nil
	}); err != nil {
		t.Fatalf("explicitly deleted target did not get template-initialized: %v", err)
	}
	waitForPodCountAndReady(t, tc, 3)
	waitForStandaloneMembership(t, ctx, tc, pcsName, "guarded", 2)
	assertRetainedPodLocation(t, podsForClique(t, tc, pcsName+"-0-worker"), survivors)
}

func Test_ZR13_StaleScaleWriterPreservesAcceptedIntent(t *testing.T) {
	for _, kind := range []string{"PodClique", "PodCliqueScalingGroup"} {
		t.Run(kind, func(t *testing.T) {
			const pcsName = "zero-scale-conflict"
			ctx := context.Background()
			tc, cleanup := prepareIdleWorkload(t, ctx, 6, pcsName, 1, func(pcs *grovecorev1alpha1.PodCliqueSet) {
				idlePCSGConfig(t, pcs).MinAvailable = ptr.To(int32(2))
			})
			defer cleanup()
			waitForPodCountAndReady(t, tc, 1)
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
			survivors := podUIDLocations(podsForClique(t, tc, pcsName+"-0-worker"))
			wm := workload.NewWorkloadManager(tc.Client, Logger)
			for _, accepted := range []int32{2, 0} {
				staleTarget := 2 - accepted
				updateScale(t, ctx, tc, target, staleTarget)
				waitForPodCountAndReady(t, tc, 1+int(staleTarget)*memberCount)
				stale := &autoscalingv1.Scale{}
				if err := tc.Client.SubResource("scale").Get(ctx, target, stale); err != nil {
					t.Fatal(err)
				}
				if stale.ResourceVersion == "" {
					t.Fatal("scale read omitted the optimistic concurrency token")
				}
				updateScale(t, ctx, tc, target, accepted)
				stale.Spec.Replicas = staleTarget
				err := tc.Client.SubResource("scale").Update(ctx, target, client.WithSubResourceBody(stale))
				if !apierrors.IsConflict(err) {
					t.Fatalf("stale %d-replica write after accepted %d: want Conflict, got %v", staleTarget, accepted, err)
				}
				restartOperator(t, ctx, tc)
				waitForPodCountAndReady(t, tc, 1+int(accepted)*memberCount)
				assertScaleStatus(t, ctx, tc, target, accepted, true)
				assertRetainedPodLocation(t, podsForClique(t, tc, pcsName+"-0-worker"), survivors)
				// A writer must refetch after Conflict; a fresh valid write is
				// still allowed to change the accepted runtime intent.
				if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
					current := &autoscalingv1.Scale{}
					if err := tc.Client.SubResource("scale").Get(ctx, target, current); err != nil {
						return err
					}
					current.Spec.Replicas = staleTarget
					return tc.Client.SubResource("scale").Update(ctx, target, client.WithSubResourceBody(current))
				}); err != nil {
					t.Fatalf("refetched scale write did not recover from Conflict: %v", err)
				}
				if err := wm.WaitForScaleReplicas(ctx, target, staleTarget, tc.Timeout, tc.Interval); err != nil {
					t.Fatal(err)
				}
				waitForPodCountAndReady(t, tc, 1+int(staleTarget)*memberCount)
				current := target.DeepCopyObject().(client.Object)
				if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(target), current); err != nil {
					t.Fatal(err)
				}
				if current.GetUID() != target.GetUID() {
					t.Fatal("scale conflict or operator restart replaced the target")
				}
			}
		})
	}
}
