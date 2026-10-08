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

package validation

import (
	"encoding/json"
	"fmt"
	"testing"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestCoherentGuardCoversIdleTransitions(t *testing.T) {
	for _, replicas := range []int32{0, 2} {
		for _, subresource := range []string{"", "scale"} {
			t.Run(fmt.Sprintf("replicas=%d/subresource=%s", replicas, subresource), func(t *testing.T) {
				pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").
					WithReplicas(2).
					WithStandaloneCliqueReplicas("worker", 2).
					WithUpdateStrategy(&grovecorev1alpha1.PodCliqueSetUpdateStrategy{Type: grovecorev1alpha1.CoherentStrategy}).
					WithUpdateProgress(&grovecorev1alpha1.PodCliqueSetUpdateProgress{
						UpdateStartedAt:   metav1.Now(),
						CurrentlyUpdating: []grovecorev1alpha1.PodCliqueSetReplicaUpdateProgress{{ReplicaIndex: 0}},
					}).Build()
				pclq := testutils.NewPodCliqueBuilder(pcs.Name, pcs.UID, "worker", pcs.Namespace, 1).
					WithReplicas(replicas).WithMinAvailable(2).Build()
				updated := pclq.DeepCopy()
				updated.Spec.Replicas = ptr.To(int32(2) - replicas)
				oldRaw, err := json.Marshal(pclq)
				require.NoError(t, err)
				newRaw, err := json.Marshal(updated)
				require.NoError(t, err)
				if subresource == "scale" {
					oldRaw = marshalScale(t, pclq.Name, replicas)
					newRaw = marshalScale(t, pclq.Name, *updated.Spec.Replicas)
				}
				cl := testutils.NewTestClientBuilder().WithObjects(pcs, pclq).Build()
				handler := &Handler{reader: cl, logger: logr.Discard()}
				response := handler.Handle(t.Context(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
					Name: pclq.Name, Namespace: pclq.Namespace, Operation: admissionv1.Update,
					SubResource: subresource, OldObject: runtime.RawExtension{Raw: oldRaw},
					Object: runtime.RawExtension{Raw: newRaw},
				}})
				require.False(t, response.Allowed)
				require.Contains(t, response.Result.Message, "coherent update is in progress")
			})
		}
	}
}

func TestMemberPositiveScalingAndLegacyUpdates(t *testing.T) {
	for _, tc := range []struct {
		name        string
		oldReplicas int32
		newReplicas int32
	}{
		{name: "scale out", oldReplicas: 3, newReplicas: 4},
		{name: "scale in to minimum", oldReplicas: 4, newReplicas: 3},
		{name: "repair legacy below quorum", oldReplicas: 1, newReplicas: 3},
		{name: "repair legacy zero", oldReplicas: 0, newReplicas: 3},
		{name: "preserve legacy zero", oldReplicas: 0, newReplicas: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, subresource := range []string{"", "scale"} {
				t.Run("subresource="+subresource, func(t *testing.T) {
					owned := testutils.NewPCSGPodCliqueBuilder("owned", "default", "pcs", "group", 0, 0).
						WithReplicas(tc.oldReplicas).Build()
					owned.Spec.MinAvailable = ptr.To(int32(3))
					owned.Finalizers = []string{"grove.io/test"}
					setPCSGControllerOwner(owned, "group")
					updated := owned.DeepCopy()
					updated.Spec.Replicas = ptr.To[int32](tc.newReplicas)
					updated.Finalizers = nil
					oldRaw, err := json.Marshal(owned)
					require.NoError(t, err)
					newRaw, err := json.Marshal(updated)
					require.NoError(t, err)
					if subresource == "scale" {
						newRaw = marshalScale(t, owned.Name, tc.newReplicas)
					}
					cl := testutils.NewTestClientBuilder().WithObjects(owned).Build()
					handler := &Handler{reader: cl, logger: logr.Discard()}
					response := handler.Handle(t.Context(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
						Name: owned.Name, Namespace: owned.Namespace, Operation: admissionv1.Update,
						SubResource: subresource, OldObject: runtime.RawExtension{Raw: oldRaw},
						Object: runtime.RawExtension{Raw: newRaw},
					}})
					require.True(t, response.Allowed, "response: %+v", response.Result)
				})
			}
		})
	}
}
