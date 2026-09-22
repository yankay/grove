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
	"testing"

	"github.com/ai-dynamo/grove/operator/internal/controller/podclique/expectations"
	"github.com/ai-dynamo/grove/operator/internal/expect"
	componentutils "github.com/ai-dynamo/grove/operator/internal/utils/component"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestGangRecoveryDrainsOldPodsAcrossRestart(t *testing.T) {
	ctx := context.Background()
	pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").Build()
	pclq := testutils.NewPodCliqueBuilder(pcs.Name, pcs.UID, "worker", pcs.Namespace, 0).WithReplicas(3).Build()
	recovery := componentutils.GangRecovery{Epoch: "recovery-1", Phase: componentutils.GangRecoveryDraining}
	require.NoError(t, componentutils.SetGangRecovery(pcs, 0, recovery))
	old := testutils.NewPodBuilder("old", pcs.Namespace).WithOwner(pclq.Name).Build()
	old.UID = "old-pod"
	old.OwnerReferences[0].UID = pclq.UID
	old.Finalizers = []string{"test.grove.io/hold-drain"}
	fresh := old.DeepCopy()
	fresh.Name, fresh.UID, fresh.Finalizers = "fresh", "fresh-pod", nil
	fresh.Annotations = map[string]string{componentutils.AnnotationPodRecoveryEpoch: recovery.Epoch}
	foreign := old.DeepCopy()
	foreign.Name, foreign.UID, foreign.Finalizers = "foreign", "foreign-pod", nil
	foreign.OwnerReferences[0].UID = "another-clique"
	cl := testutils.NewTestClientBuilder().WithObjects(pcs, pclq, old, fresh, foreign).
		WithIndex(&corev1.Pod{}, ".metadata.controller.uid", func(obj client.Object) []string {
			owner := metav1.GetControllerOf(obj)
			if owner == nil {
				return nil
			}
			return []string{string(owner.UID)}
		}).Build()
	r := &Reconciler{client: cl, expectationsStore: expect.NewExpectationsStore()}
	require.NoError(t, r.expectationsStore.AddIndexers(expectations.PodCliqueExpectationsIndexers()))
	result := r.reconcileGangRecovery(ctx, logr.Discard(), pcs, pclq)
	require.False(t, result.HasErrors())
	requeue, err := result.Result()
	require.NoError(t, err)
	assert.Positive(t, requeue.RequeueAfter)
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(old), old))
	require.False(t, old.DeletionTimestamp.IsZero())
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(fresh), fresh))
	assert.True(t, fresh.DeletionTimestamp.IsZero())
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(foreign), foreign))
	assert.True(t, foreign.DeletionTimestamp.IsZero())

	restarted := &Reconciler{client: cl, expectationsStore: expect.NewExpectationsStore()}
	require.NoError(t, restarted.expectationsStore.AddIndexers(expectations.PodCliqueExpectationsIndexers()))
	result = restarted.reconcileGangRecovery(ctx, logr.Discard(), pcs, pclq)
	require.False(t, result.HasErrors())
	requeue, err = result.Result()
	require.NoError(t, err)
	assert.Positive(t, requeue.RequeueAfter, "terminating pods still hold the drain barrier")
	old.Finalizers = nil
	require.NoError(t, cl.Update(ctx, old))
	require.True(t, apierrors.IsNotFound(cl.Get(ctx, client.ObjectKeyFromObject(old), old)))

	recovery.Phase = componentutils.GangRecoveryRecreating
	require.NoError(t, componentutils.SetGangRecovery(pcs, 0, recovery))
	result = restarted.reconcileGangRecovery(ctx, logr.Discard(), pcs, pclq)
	requeue, err = result.Result()
	require.NoError(t, err)
	assert.Zero(t, requeue.RequeueAfter)
	assert.Equal(t, int32(3), pclq.Spec.Replicas)

	// A delayed pre-recovery create is removed even after recovery completed.
	recovery.Phase = componentutils.GangRecoveryComplete
	require.NoError(t, componentutils.SetGangRecovery(pcs, 0, recovery))
	late := foreign.DeepCopy()
	late.Name, late.UID, late.ResourceVersion = "late", "late-pod", ""
	late.OwnerReferences[0].UID = pclq.UID
	require.NoError(t, cl.Create(ctx, late))
	result = restarted.reconcileGangRecovery(ctx, logr.Discard(), pcs, pclq)
	require.False(t, result.HasErrors())
	assert.True(t, apierrors.IsNotFound(cl.Get(ctx, client.ObjectKeyFromObject(late), late)))
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(fresh), fresh))
	assert.True(t, fresh.DeletionTimestamp.IsZero())
}

func TestGangRecoveryWatchIncludesScaledGroupMembers(t *testing.T) {
	pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").Build()
	old := pcs.DeepCopy()
	require.NoError(t, componentutils.SetGangRecovery(pcs, 0, componentutils.GangRecovery{
		Epoch: "recovery", Phase: componentutils.GangRecoveryDraining,
	}))
	pclq := testutils.NewPCSGPodCliqueBuilder("pcs-0-group-5-worker", pcs.Namespace, pcs.Name, "pcs-0-group", 0, 5).Build()
	r := &Reconciler{client: testutils.CreateDefaultFakeClient([]client.Object{pclq})}
	predicate := gangRecoveryPredicate()
	assert.True(t, predicate.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: pcs}))
	requests := r.mapGangRecoveryToPCLQs()(context.Background(), pcs)
	require.Len(t, requests, 1)
	assert.Equal(t, client.ObjectKeyFromObject(pclq), requests[0].NamespacedName)
	unrelated := old.DeepCopy()
	unrelated.Annotations = map[string]string{"other": "changed"}
	assert.False(t, predicate.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: unrelated}))
}
