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
	"strings"
	"testing"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/e2e/grove/podgroup"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func Test_ZR14_MemberScalingLifecycle(t *testing.T) {
	for _, strategy := range []grovecorev1alpha1.UpdateStrategyType{
		grovecorev1alpha1.RollingRecreateStrategy, grovecorev1alpha1.OnDeleteStrategy,
	} {
		t.Run(string(strategy), func(t *testing.T) {
			ctx := context.Background()
			name := "member-scale-" + strings.ToLower(string(strategy))
			tc, cleanup := prepareIdleWorkload(t, ctx, 3, name, 1, func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.UpdateStrategy = &grovecorev1alpha1.PodCliqueSetUpdateStrategy{Type: strategy}
				idlePCSGConfig(t, pcs).Replicas = ptr.To(int32(1))
				member := idleClique(t, pcs, "decode")
				member.Spec.Replicas, member.Spec.MinAvailable = ptr.To[int32](3), ptr.To(int32(3))
			})
			defer cleanup()
			waitForPodCountAndReady(t, tc, 5)
			member := waitForPCLQ(t, ctx, tc, name+"-0-workers-0-decode")
			sibling := waitForPCLQ(t, ctx, tc, name+"-0-workers-0-prefill")
			routerName := name + "-0-worker"
			routerPods := podUIDLocations(podsForClique(t, tc, routerName))
			siblingPods := podUIDLocations(podsForClique(t, tc, sibling.Name))
			group := &grovecorev1alpha1.PodCliqueScalingGroup{}
			require.NoError(t, tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: name + "-0-workers"}, group))
			groupUID, memberUID, siblingUID := group.UID, member.UID, sibling.UID

			checkMember := func(replicas int32) {
				t.Helper()
				waitForPodCountAndReady(t, tc, int(replicas)+2)
				require.Len(t, podsForClique(t, tc, member.Name), int(replicas))
				require.NoError(t, tc.Client.Get(ctx, client.ObjectKeyFromObject(member), member))
				require.Equal(t, memberUID, member.UID)
				require.Equal(t, replicas, ptr.Deref(member.Spec.Replicas, 1))
				require.EqualValues(t, 3, *member.Spec.MinAvailable)
				require.NoError(t, tc.Client.Get(ctx, client.ObjectKeyFromObject(group), group))
				require.Equal(t, groupUID, group.UID)
				require.EqualValues(t, 1, group.Spec.Replicas)
				require.NoError(t, tc.Client.Get(ctx, client.ObjectKeyFromObject(sibling), sibling))
				require.Equal(t, siblingUID, sibling.UID)
				assertRetainedPodLocation(t, podsForClique(t, tc, sibling.Name), siblingPods)
				assertRetainedPodLocation(t, podsForClique(t, tc, routerName), routerPods)
				require.NoError(t, wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
					gangs := listPodGangs(t, ctx, tc, name)
					for i := range gangs.Items {
						gang := &gangs.Items[i]
						for _, podGroup := range gang.Spec.PodGroups {
							if podGroup.Name != member.Name {
								continue
							}
							if podGroup.MinReplicas != 3 || len(podGroup.PodReferences) != int(replicas) {
								return false, nil
							}
							native, err := podgroup.GetNativePodGroup(ctx, tc.Client, gang)
							if err != nil {
								return false, client.IgnoreNotFound(err)
							}
							return podgroup.VerifyFlatMembership(gang, native) == nil, nil
						}
					}
					return false, nil
				}))
			}
			checkMember(3)
			initialPods := podUIDLocations(podsForClique(t, tc, member.Name))
			updateScale(t, ctx, tc, member, 4)
			checkMember(4)
			scaledPods := podUIDLocations(podsForClique(t, tc, member.Name))
			for uid, node := range initialPods {
				require.Equal(t, node, scaledPods[uid], "scale-out must retain existing member Pods")
			}
			updateScale(t, ctx, tc, member, 3)
			checkMember(3)
			assertRetainedPodLocation(t, podsForClique(t, tc, member.Name), scaledPods)

			updateScale(t, ctx, tc, member, 4)
			checkMember(4)
			beforeRestart := podUIDLocations(podsForClique(t, tc, member.Name))
			restartOperator(t, ctx, tc)
			checkMember(4)
			assertRetainedPodLocation(t, podsForClique(t, tc, member.Name), beforeRestart)

			// Group hibernation deletes its members. A later wake creates new logical
			// members from their templates; this is not object-retaining recovery.
			updateScale(t, ctx, tc, group, 0)
			waitForPodCountAndReady(t, tc, 1)
			waitForPCSGChildrenAbsent(t, ctx, tc, group.Name, 0)
			waitForPCSGMembership(t, ctx, tc, name, "workers", nil)
			restartOperator(t, ctx, tc)
			assertScaleStatus(t, ctx, tc, group, 0, false)
			updateScale(t, ctx, tc, group, 1)
			waitForPodCountAndReady(t, tc, 5)
			woken := waitForPCLQ(t, ctx, tc, member.Name)
			wokenSibling := waitForPCLQ(t, ctx, tc, sibling.Name)
			require.NotEqual(t, memberUID, woken.UID)
			require.NotEqual(t, siblingUID, wokenSibling.UID)
			for _, pod := range podsForClique(t, tc, woken.Name) {
				require.NotContains(t, beforeRestart, pod.UID)
			}
			for _, pod := range podsForClique(t, tc, wokenSibling.Name) {
				require.NotContains(t, siblingPods, pod.UID)
			}
			memberUID, siblingUID = woken.UID, wokenSibling.UID
			siblingPods = podUIDLocations(podsForClique(t, tc, sibling.Name))
			checkMember(3)
		})
	}
}
