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

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/utils/component"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestVerifyNoGangRecovery(t *testing.T) {
	for _, scenario := range []string{
		"unchanged", "active recovery", "completed recovery", "replaced clique",
		"missing pod", "replaced pod", "terminating pod", "foreign pod", "empty snapshot",
		"get error", "list error",
	} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").Build()
			pclq := testutils.NewPodCliqueBuilder(pcs.Name, pcs.UID, "worker", pcs.Namespace, 0).Build()
			pclq.UID = "clique-uid"
			original := pclq.DeepCopy()
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "pending", Namespace: pcs.Namespace, UID: "pod-uid",
				Labels: map[string]string{apicommon.LabelPodClique: pclq.Name},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: grovecorev1alpha1.SchemeGroupVersion.String(), Kind: "PodClique",
					Name: pclq.Name, UID: pclq.UID, Controller: ptr.To(true),
				}},
			}}
			uids := sets.New[types.UID](pod.UID)
			switch scenario {
			case "active recovery", "completed recovery":
				phase := componentutils.GangRecoveryDraining
				if scenario == "completed recovery" {
					phase = componentutils.GangRecoveryComplete
				}
				require.NoError(t, componentutils.SetGangRecovery(pcs, 0, componentutils.GangRecovery{Epoch: "unexpected", Phase: phase}))
			case "replaced clique":
				pclq.UID = "new-clique"
			case "replaced pod":
				pod.UID = "new-pod"
			case "terminating pod":
				pod.DeletionTimestamp = ptr.To(metav1.Now())
				pod.Finalizers = []string{"test.grove.io/hold"}
			case "foreign pod":
				pod.OwnerReferences[0].UID = "another-clique"
			case "empty snapshot":
				uids = nil
			}
			builder := testutils.NewTestClientBuilder().WithObjects(pcs, pclq)
			if scenario != "missing pod" {
				builder.WithObjects(pod)
			}
			failure := apierrors.NewServiceUnavailable("API unavailable")
			if scenario == "get error" {
				builder.RecordErrorForObjects(testutils.ClientMethodGet, failure, client.ObjectKeyFromObject(pcs))
			}
			if scenario == "list error" {
				builder.RecordErrorForObjectsMatchingLabels(testutils.ClientMethodList,
					client.ObjectKey{Namespace: pcs.Namespace}, corev1.SchemeGroupVersion.WithKind("PodList"),
					map[string]string{apicommon.LabelPodClique: pclq.Name}, failure)
			}
			wm := &WorkloadManager{cl: builder.Build()}
			err := wm.VerifyNoGangRecovery(ctx, original, uids)
			switch scenario {
			case "unchanged":
				require.NoError(t, err)
			case "get error", "list error":
				require.ErrorIs(t, err, failure)
			default:
				require.Error(t, err)
			}
		})
	}
}
