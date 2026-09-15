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

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestSchedulingDependencyMapsWaitingSiblings(t *testing.T) {
	gang := testutils.NewPodGangBuilder("anchor", "default").
		WithManaged(true).WithLabels(map[string]string{apicommon.LabelPartOfKey: "pcs", apicommon.LabelPodCliqueSetReplicaIndex: "0"}).Build()
	gang.UID = "gang-uid"
	pclqs := []*grovecorev1alpha1.PodClique{
		testutils.NewPCSGPodCliqueBuilder("minimum", "default", "pcs", "pcs-0-workers", 0, 0).Build(),
		testutils.NewPCSGPodCliqueBuilder("extra", "default", "pcs", "pcs-0-workers", 0, 1).Build(),
		testutils.NewPCSGPodCliqueBuilder("other-group", "default", "pcs", "pcs-0-other", 0, 0).Build(),
		testutils.NewPCSGPodCliqueBuilder("other-replica", "default", "pcs", "pcs-1-workers", 1, 0).Build(),
		testutils.NewPCSGPodCliqueBuilder("other-namespace", "other", "pcs", "pcs-0-workers", 0, 0).Build(),
	}
	objects := []client.Object{gang}
	for _, pclq := range pclqs {
		objects = append(objects, pclq)
	}
	r := &Reconciler{client: testutils.NewTestClientBuilder().WithObjects(objects...).Build()}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "minimum-0", Labels: pclqs[0].Labels}}
	native := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: gang.Name, Namespace: gang.Namespace,
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(gang, groveschedulerv1alpha1.SchemeGroupVersion.WithKind("PodGang"))},
	}}
	pgm := testutils.NewPodGangMapBuilder("pcs", "default", "pcs-uid", 0).Build()
	for _, tt := range []struct {
		name string
		obj  client.Object
		want []string
	}{
		{"minimum pod schedules or regresses", pod, []string{"minimum", "extra"}},
		{"native policy handoff", native, []string{"minimum", "extra", "other-group"}},
		{"epoch dependency scheduled", gang, []string{"minimum", "extra", "other-group"}},
		{"placement changes", pgm, []string{"minimum", "extra", "other-group"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requests := r.mapSchedulingDependencyToPCLQs(nil)(context.Background(), tt.obj)
			names := make([]string, 0, len(requests))
			for _, request := range requests {
				assert.Equal(t, "default", request.Namespace)
				names = append(names, request.Name)
			}
			assert.ElementsMatch(t, tt.want, names)
		})
	}
	native.OwnerReferences[0].UID = "old-gang"
	assert.Empty(t, r.mapSchedulingDependencyToPCLQs(nil)(context.Background(), native))
}

func TestPodSchedulingPredicateObservesBinding(t *testing.T) {
	oldPod := testutils.NewPodBuilder("pod", "default").
		WithOwner("pclq").Build()
	oldPod.Labels = apicommon.GetDefaultLabelsForPodCliqueSetManagedResources("pcs")
	pred := podSchedulingPredicate()
	newPod := oldPod.DeepCopy()
	newPod.Generation++
	newPod.Spec.NodeName = "node"
	newPod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}}
	require.True(t, pred.Update(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}),
		"binding may change both Pod generation and scheduling status")
	assert.True(t, pred.Create(event.CreateEvent{Object: newPod}), "cache catch-up must wake quorum waiters")
	assert.True(t, pred.Delete(event.DeleteEvent{Object: newPod}))
	assert.False(t, pred.Update(event.UpdateEvent{ObjectOld: newPod, ObjectNew: newPod.DeepCopy()}))
	assert.False(t, pred.Generic(event.GenericEvent{Object: newPod}))
	unmanaged := newPod.DeepCopy()
	unmanaged.Labels = nil
	assert.False(t, pred.Update(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: unmanaged}))
}
