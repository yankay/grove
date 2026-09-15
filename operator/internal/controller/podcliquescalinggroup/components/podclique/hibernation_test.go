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

package podclique

import (
	"context"
	"fmt"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	groveerr "github.com/ai-dynamo/grove/operator/internal/errors"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestScaleOutEpochRecreatesSurvivingMembers(t *testing.T) {
	for _, strategy := range []grovecorev1alpha1.UpdateStrategyType{
		grovecorev1alpha1.RollingRecreateStrategy,
		grovecorev1alpha1.OnDeleteStrategy,
	} {
		t.Run(string(strategy), func(t *testing.T) {
			ctx := context.Background()
			pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").
				WithReplicas(1).WithPodCliqueSetGenerationHash(ptr.To("generation")).
				WithScalingGroupConfig("group", []string{"worker"}, 2, 2).
				WithCliqueStartupType(ptr.To(grovecorev1alpha1.CliqueStartupTypeAnyOrder)).Build()
			pcs.Spec.UpdateStrategy = &grovecorev1alpha1.PodCliqueSetUpdateStrategy{Type: strategy}
			pcsg := &grovecorev1alpha1.PodCliqueScalingGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pcs-0-group", Namespace: pcs.Namespace, UID: "group-uid",
					Labels: apicommon.GetDefaultLabelsForPodCliqueSetManagedResources(pcs.Name),
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: grovecorev1alpha1.SchemeGroupVersion.String(), Kind: "PodCliqueSet",
						Name: pcs.Name, UID: pcs.UID, Controller: ptr.To(true),
					}},
				},
				Spec: grovecorev1alpha1.PodCliqueScalingGroupSpec{
					Replicas: 3, MinAvailable: ptr.To(int32(2)), CliqueNames: []string{"worker"},
				},
			}
			pgm := testutils.NewPodGangMapBuilder(pcs.Name, pcs.Namespace, pcs.UID, 0).Build()
			pgm.Spec.Entries = []grovecorev1alpha1.PodGangEntry{
				{Epoch: "100", Role: grovecorev1alpha1.PodGangEntryRoleAnchor, AnchorIndex: ptr.To(int32(0)), PodCliqueSetGenerationHash: "generation", PCSGReplicaIndices: map[string][]int32{"group": {0, 1}}},
				{Epoch: "102", Role: grovecorev1alpha1.PodGangEntryRoleScaleOut, PodCliqueSetGenerationHash: "generation", PCSGReplicaIndices: map[string][]int32{"group": {2}}},
			}
			cl := testutils.NewTestClientBuilder().WithObjects(pcs, pcsg).Build()
			r := _resource{client: cl, scheme: cl.Scheme(), eventRecorder: record.NewFakeRecorder(50)}
			ss := &syncSnapshot{
				pcs: pcs, pcsg: pcsg, pgm: pgm,
				expectedPCLQFQNsPerPCSGReplica: getExpectedPodCliqueFQNsByPCSGReplica(pcsg),
			}
			for index := range 3 {
				pclq := emptyPodClique(client.ObjectKey{Name: fmt.Sprintf("pcs-0-group-%d-worker", index), Namespace: pcs.Namespace})
				require.NoError(t, r.buildResource(logr.Discard(), ss, index, pclq, false))
				pclq.UID = types.UID(pclq.Name + "-uid")
				require.NoError(t, cl.Create(ctx, pclq))
				ss.existingPCLQs = append(ss.existingPCLQs, *pclq)
			}
			ss.existingPCLQNameSet = componentutils.PodCliqueNameSet(ss.existingPCLQs)
			old := ss.existingPCLQs[2].DeepCopy()
			anchor := ss.existingPCLQs[0].DeepCopy()
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: old.Name + "-0", Namespace: pcs.Namespace, UID: "old-pod-uid",
				Labels: map[string]string{apicommon.LabelPodGang: old.Labels[apicommon.LabelPodGang]},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: grovecorev1alpha1.SchemeGroupVersion.String(), Kind: "PodClique",
					Name: old.Name, UID: old.UID, Controller: ptr.To(true),
				}},
			}}
			require.NoError(t, cl.Create(ctx, pod))
			// A blocked extra must not prevent an independently missing anchor
			// member from being recreated.
			missingAnchor := ss.existingPCLQs[1].DeepCopy()
			missingAnchor.Finalizers = nil
			require.NoError(t, cl.Update(ctx, missingAnchor))
			require.NoError(t, cl.Delete(ctx, missingAnchor))
			ss.existingPCLQs = []grovecorev1alpha1.PodClique{*anchor, *old}
			ss.existingPCLQNameSet = componentutils.PodCliqueNameSet(ss.existingPCLQs)
			// The PCS observed 3 -> 2 -> 3 before the PCSG could remove index 2.
			pgm.Spec.Entries[1].Epoch = "5000"
			for range 2 {
				err := r.runSyncFlow(ctx, logr.Discard(), ss)
				var requeue *groveerr.GroveError
				require.ErrorAs(t, err, &requeue)
				require.Equal(t, groveerr.ErrCodeRequeueAfter, requeue.Code)
				require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(old), old))
				require.False(t, old.DeletionTimestamp.IsZero())
				require.Equal(t, "pcs-0-102-group-2", old.Labels[apicommon.LabelPodGang])
				// A deleting child is still present on the next controller pass.
				ss.existingPCLQs, err = r.getExistingPCLQs(ctx, pcsg)
				require.NoError(t, err)
				ss.existingPCLQNameSet = componentutils.PodCliqueNameSet(ss.existingPCLQs)
				recreatedAnchor := &grovecorev1alpha1.PodClique{}
				require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(missingAnchor), recreatedAnchor))
				require.NotEqual(t, missingAnchor.UID, recreatedAnchor.UID)
				require.Equal(t, anchor.Labels[apicommon.LabelPodGang], recreatedAnchor.Labels[apicommon.LabelPodGang])
			}
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pod), pod))
			require.Equal(t, "pcs-0-102-group-2", pod.Labels[apicommon.LabelPodGang], "old Pods must not be relabeled into the new scheduler gang")
			// Simulate PodClique deletion completion only after descendants drain.
			require.NoError(t, cl.Delete(ctx, pod))
			old.Finalizers = nil
			require.NoError(t, cl.Update(ctx, old))
			require.True(t, apierrors.IsNotFound(cl.Get(ctx, client.ObjectKeyFromObject(old), &grovecorev1alpha1.PodClique{})))
			var err error
			ss.existingPCLQs, err = r.getExistingPCLQs(ctx, pcsg)
			require.NoError(t, err)
			ss.existingPCLQNameSet = componentutils.PodCliqueNameSet(ss.existingPCLQs)
			require.NoError(t, r.runSyncFlow(ctx, logr.Discard(), ss))
			created := &grovecorev1alpha1.PodClique{}
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(old), created))
			require.True(t, created.DeletionTimestamp.IsZero())
			require.NotEqual(t, old.UID, created.UID)
			require.Equal(t, "pcs-0-5000-group-2", created.Labels[apicommon.LabelPodGang])
			require.EqualValues(t, 3, pcsg.Spec.Replicas)
			actualAnchor := &grovecorev1alpha1.PodClique{}
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(anchor), actualAnchor))
			require.Equal(t, anchor.UID, actualAnchor.UID)
			require.True(t, actualAnchor.DeletionTimestamp.IsZero())
			require.Equal(t, anchor.Labels[apicommon.LabelPodGang], actualAnchor.Labels[apicommon.LabelPodGang])
		})
	}
}

func TestStaleScaleOutDeletionRequiresOwnership(t *testing.T) {
	for _, invalid := range []string{"clique-owner", "group-owner", "map-owner", "map-manager"} {
		t.Run(invalid, func(t *testing.T) {
			pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").Build()
			pcsg := &grovecorev1alpha1.PodCliqueScalingGroup{ObjectMeta: metav1.ObjectMeta{
				Name: "pcs-0-group", Namespace: pcs.Namespace, UID: "group-uid",
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: grovecorev1alpha1.SchemeGroupVersion.String(), Kind: "PodCliqueSet",
					Name: pcs.Name, UID: pcs.UID, Controller: ptr.To(true),
				}},
			}}
			pclq := &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{
				Name: "pcs-0-group-2-worker", Namespace: pcs.Namespace, UID: "clique-uid",
				Labels: map[string]string{apicommon.LabelPodGang: "pcs-0-102-group-2"},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: grovecorev1alpha1.SchemeGroupVersion.String(), Kind: "PodCliqueScalingGroup",
					Name: pcsg.Name, UID: pcsg.UID, Controller: ptr.To(true),
				}},
			}}
			pgm := testutils.NewPodGangMapBuilder(pcs.Name, pcs.Namespace, pcs.UID, 0).Build()
			pgm.Spec.Entries = []grovecorev1alpha1.PodGangEntry{{
				Epoch: "5000", Role: grovecorev1alpha1.PodGangEntryRoleScaleOut, PCSGReplicaIndices: map[string][]int32{"group": {2}},
			}}
			switch invalid {
			case "clique-owner":
				pclq.OwnerReferences[0].UID = "old-group-uid"
			case "group-owner":
				pcsg.OwnerReferences[0].UID = "old-pcs-uid"
			case "map-owner":
				pgm.OwnerReferences[0].UID = "old-pcs-uid"
			case "map-manager":
				delete(pgm.Labels, apicommon.LabelManagedByKey)
			}
			cl := testutils.NewTestClientBuilder().WithObjects(pclq).Build()
			r := _resource{client: cl}
			retiring, err := r.recreateStaleScaleOutPCLQs(context.Background(), &syncSnapshot{
				pcs: pcs, pcsg: pcsg, pgm: pgm, existingPCLQs: []grovecorev1alpha1.PodClique{*pclq},
				expectedPCLQFQNsPerPCSGReplica: map[int][]string{2: {pclq.Name}},
			})
			if invalid == "clique-owner" {
				require.NoError(t, err)
			} else {
				var requeue *groveerr.GroveError
				require.ErrorAs(t, err, &requeue)
				require.Equal(t, groveerr.ErrCodeRequeueAfter, requeue.Code)
			}
			require.Empty(t, retiring)
			require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(pclq), pclq))
			require.True(t, pclq.DeletionTimestamp.IsZero())
		})
	}
}
