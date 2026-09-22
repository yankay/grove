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

package defaulting

import (
	"context"
	"testing"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestDefaultMinimum(t *testing.T) {
	for _, tc := range []struct {
		name        string
		operation   admissionv1.Operation
		replicas    int32
		minimum     *int32
		want        *int32
		subresource string
	}{
		{name: "active", operation: admissionv1.Create, replicas: 3, want: ptr.To(int32(3))},
		{name: "idle", operation: admissionv1.Create, replicas: 0, want: ptr.To(int32(1))},
		{name: "explicit quorum", operation: admissionv1.Create, replicas: 4, minimum: ptr.To(int32(2)), want: ptr.To(int32(2))},
		{name: "invalid zero left for validation", operation: admissionv1.Create, replicas: 3, minimum: ptr.To(int32(0)), want: ptr.To(int32(0))},
		{name: "legacy update", operation: admissionv1.Update, replicas: 3},
		{name: "scale ignored", operation: admissionv1.Update, replicas: 5, subresource: "scale"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pclq := &grovecorev1alpha1.PodClique{Spec: grovecorev1alpha1.PodCliqueSpec{Replicas: ptr.To[int32](tc.replicas), MinAvailable: tc.minimum}}
			ctx := admission.NewContextWithRequest(t.Context(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: tc.operation, SubResource: tc.subresource,
			}})
			require.NoError(t, NewHandler().Default(ctx, pclq))
			require.Equal(t, tc.want, pclq.Spec.MinAvailable)
			require.Equal(t, tc.replicas, ptr.Deref(pclq.Spec.Replicas, 1))
			require.NoError(t, NewHandler().Default(ctx, pclq))
			require.Equal(t, tc.want, pclq.Spec.MinAvailable, "defaulting must be idempotent")
		})
	}
}

func TestDefaultInvalidInput(t *testing.T) {
	require.Error(t, NewHandler().Default(context.Background(), &corev1.Pod{}))
	require.Error(t, NewHandler().Default(context.Background(), &grovecorev1alpha1.PodClique{}))
}

func TestDefaultOmittedReplicas(t *testing.T) {
	pclq := &grovecorev1alpha1.PodClique{}
	ctx := admission.NewContextWithRequest(t.Context(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
	}})
	require.NoError(t, NewHandler().Default(ctx, pclq))
	require.Equal(t, ptr.To(int32(1)), pclq.Spec.Replicas)
	require.Equal(t, ptr.To(int32(1)), pclq.Spec.MinAvailable)
	require.NoError(t, NewHandler().Default(ctx, pclq))
	require.Equal(t, ptr.To(int32(1)), pclq.Spec.Replicas)
}

func TestRegisterWithManager(t *testing.T) {
	cl := testutils.NewTestClientBuilder().Build()
	mgr := &testutils.FakeManager{
		Client: cl, Scheme: cl.Scheme(), WebhookServer: webhook.NewServer(webhook.Options{}),
	}
	require.NoError(t, NewHandler().RegisterWithManager(mgr))
	require.NotNil(t, mgr.WebhookServer.WebhookMux())
}
