// /*
// Copyright 2025 The Grove Authors.
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
// */

package utils

import (
	"context"
	"testing"

	"github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestIsOwnerBeingDeleted exercises the cascade-delete predicate helper used by all three
// controllers to suppress noisy reconciles for children whose parent is going away (issue #622).
func TestIsOwnerBeingDeleted(t *testing.T) {
	const ns, ownerName = "default", "owner-pclq"

	scheme := runtime.NewScheme()
	require.NoError(t, grovecorev1alpha1.AddToScheme(scheme))

	makeChild := func(ownerKind string, includeOwner bool) *grovecorev1alpha1.PodClique {
		child := &grovecorev1alpha1.PodClique{
			ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: ns},
		}
		if includeOwner {
			child.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: grovecorev1alpha1.SchemeGroupVersion.String(),
				Kind:       ownerKind,
				Name:       ownerName,
				UID:        types.UID("owner-uid"),
				Controller: ptr.To(true),
			}}
		}
		return child
	}

	now := metav1.Now()
	tests := []struct {
		name      string
		ownerObj  *grovecorev1alpha1.PodClique // optional; nil means no owner present in store
		ownerKind string
		withRef   bool
		want      bool
	}{
		{
			name:      "owner alive: do not filter",
			ownerObj:  &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{Name: ownerName, Namespace: ns, UID: types.UID("owner-uid")}},
			ownerKind: constants.KindPodClique,
			withRef:   true,
			want:      false,
		},
		{
			name: "owner has DeletionTimestamp: filter",
			ownerObj: &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{
				Name: ownerName, Namespace: ns,
				UID:               types.UID("owner-uid"),
				DeletionTimestamp: &now,
				Finalizers:        []string{"keep-around"},
			}},
			ownerKind: constants.KindPodClique,
			withRef:   true,
			want:      true,
		},
		{
			name:      "owner already gone (NotFound): filter",
			ownerObj:  nil,
			ownerKind: constants.KindPodClique,
			withRef:   true,
			want:      true,
		},
		{
			name:      "owner recreated with different UID: filter",
			ownerObj:  &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{Name: ownerName, Namespace: ns, UID: types.UID("recreated-uid")}},
			ownerKind: constants.KindPodClique,
			withRef:   true,
			want:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := fake.NewClientBuilder().WithScheme(scheme)
			if tc.ownerObj != nil {
				b = b.WithObjects(tc.ownerObj)
			}
			cl := b.Build()
			child := makeChild(tc.ownerKind, tc.withRef)

			got := IsOwnerBeingDeleted(context.Background(), cl, child, tc.ownerKind, &grovecorev1alpha1.PodClique{})
			assert.Equal(t, tc.want, got)
		})
	}
}
