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
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	operatorcrds "github.com/ai-dynamo/grove/operator/api/core/v1alpha1/crds"
	pclqdefaulting "github.com/ai-dynamo/grove/operator/internal/webhook/admission/pclq/defaulting"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/yaml"
)

func TestMemberScaleAdmission(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	const admissionTimeout = 30 * time.Second
	const pollInterval = 100 * time.Millisecond
	testEnv := &envtest.Environment{
		UseExistingCluster:       ptr.To(false),
		ControlPlaneStartTimeout: admissionTimeout,
		ControlPlaneStopTimeout:  admissionTimeout,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			MaxTime: admissionTimeout, PollInterval: pollInterval,
			MutatingWebhooks: []*admissionv1.MutatingWebhookConfiguration{{
				ObjectMeta: metav1.ObjectMeta{Name: "pclq-minimum-defaulting"},
				Webhooks: []admissionv1.MutatingWebhook{{
					Name: "pclq.defaulting.webhooks.grove.io", AdmissionReviewVersions: []string{"v1"},
					SideEffects: ptr.To(admissionv1.SideEffectClassNone), FailurePolicy: ptr.To(admissionv1.Fail),
					ClientConfig: admissionv1.WebhookClientConfig{Service: &admissionv1.ServiceReference{
						Path: ptr.To("webhooks/default-podclique"),
					}},
					Rules: []admissionv1.RuleWithOperations{{
						Operations: []admissionv1.OperationType{admissionv1.Create},
						Rule: admissionv1.Rule{
							APIGroups: []string{"grove.io"}, APIVersions: []string{"v1alpha1"}, Resources: []string{"podcliques"},
						},
					}},
				}},
			}},
			ValidatingWebhooks: []*admissionv1.ValidatingWebhookConfiguration{{
				ObjectMeta: metav1.ObjectMeta{Name: "member-replica-validation"},
				Webhooks: []admissionv1.ValidatingWebhook{{
					Name: "pclq.validating.webhooks.grove.io", AdmissionReviewVersions: []string{"v1"},
					SideEffects: ptr.To(admissionv1.SideEffectClassNone), FailurePolicy: ptr.To(admissionv1.Fail),
					ClientConfig: admissionv1.WebhookClientConfig{Service: &admissionv1.ServiceReference{
						Path: ptr.To(strings.TrimPrefix(webhookPath, "/")),
					}},
					Rules: []admissionv1.RuleWithOperations{{
						Operations: []admissionv1.OperationType{admissionv1.Update},
						Rule: admissionv1.Rule{
							APIGroups: []string{"grove.io"}, APIVersions: []string{"v1alpha1"},
							Resources: []string{"podcliques", "podcliques/scale"},
						},
					}},
				}},
			}},
		},
	}
	for _, raw := range []string{operatorcrds.PodCliqueCRD(), operatorcrds.PodCliqueScalingGroupCRD()} {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		require.NoError(t, yaml.Unmarshal([]byte(raw), crd))
		testEnv.CRDs = append(testEnv.CRDs, crd)
	}
	cfg, err := testEnv.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testEnv.Stop()) })
	cfg.Timeout = admissionTimeout
	scheme := runtime.NewScheme()
	require.NoError(t, grovecorev1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, autoscalingv1.AddToScheme(scheme))
	opts := testEnv.WebhookInstallOptions
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"},
		WebhookServer: webhook.NewServer(webhook.Options{
			Host: opts.LocalServingHost, Port: opts.LocalServingPort, CertDir: opts.LocalServingCertDir,
		}),
	})
	require.NoError(t, err)
	require.NoError(t, NewHandler(mgr).RegisterWithManager(mgr))
	require.NoError(t, pclqdefaulting.NewHandler().RegisterWithManager(mgr))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(admissionTimeout):
			t.Error("manager shutdown timed out")
		}
	})
	require.Eventually(t, func() bool {
		return mgr.GetWebhookServer().StartedChecker()(nil) == nil
	}, admissionTimeout, pollInterval)
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	group := testutils.NewPodCliqueScalingGroupBuilder("group", "default", "pcs", 0).
		WithReplicas(1).WithCliqueNames([]string{"worker"}).Build()
	require.NoError(t, cl.Create(ctx, group))
	member := testutils.NewPCSGPodCliqueBuilder("worker", "default", "pcs", group.Name, 0, 0).
		WithReplicas(3).Build()
	member.Spec.MinAvailable = ptr.To(int32(3))
	member.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(group, grovecorev1alpha1.SchemeGroupVersion.WithKind("PodCliqueScalingGroup")),
	}
	require.NoError(t, cl.Create(ctx, member))
	uid := member.UID
	probe := member.DeepCopy()
	probe.Spec.Replicas = ptr.To[int32](0)
	require.Eventually(t, func() bool {
		return apierrors.IsForbidden(cl.Update(ctx, probe.DeepCopy(), client.DryRunAll))
	}, admissionTimeout, pollInterval, "the production member webhook must be active")

	for _, subresource := range []string{"", "scale"} {
		t.Run("subresource="+subresource, func(t *testing.T) {
			for _, target := range []int32{4, 3, 0, 2} {
				candidate := member.DeepCopy()
				candidate.Spec.Replicas = ptr.To[int32](target)
				var updateErr error
				if subresource == "" {
					updateErr = cl.Update(ctx, candidate)
				} else {
					scale := &autoscalingv1.Scale{}
					require.NoError(t, cl.SubResource("scale").Get(ctx, member, scale))
					scale.Spec.Replicas = target
					updateErr = cl.SubResource("scale").Update(ctx, member, client.WithSubResourceBody(scale))
				}
				expected := target
				switch target {
				case 0:
					require.True(t, apierrors.IsForbidden(updateErr), "error: %v", updateErr)
					expected = 3
				case 2:
					var statusErr *apierrors.StatusError
					require.ErrorAs(t, updateErr, &statusErr)
					require.Equal(t, metav1.StatusReasonInvalid, statusErr.ErrStatus.Reason)
					require.Len(t, statusErr.ErrStatus.Details.Causes, 1)
					require.Equal(t, "spec", statusErr.ErrStatus.Details.Causes[0].Field)
					expected = 3
				default:
					require.NoError(t, updateErr)
				}
				require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(member), member))
				require.Equal(t, expected, ptr.Deref(member.Spec.Replicas, 1))
				require.Equal(t, uid, member.UID)
				require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(group), group))
				require.EqualValues(t, 1, group.Spec.Replicas, "member scaling must not scale the group")
			}
		})
	}

	t.Run("direct PodClique minimum", func(t *testing.T) {
		omitted := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "grove.io/v1alpha1", "kind": "PodClique",
			"metadata": map[string]interface{}{"name": "omitted-replicas", "namespace": "default"},
			"spec": map[string]interface{}{"roleName": "worker", "podSpec": map[string]interface{}{
				"containers": []interface{}{map[string]interface{}{"name": "worker", "image": "test:v1"}},
			}},
		}}
		require.NoError(t, cl.Create(ctx, omitted))
		for _, field := range []string{"replicas", "minAvailable"} {
			value, found, err := unstructured.NestedInt64(omitted.Object, "spec", field)
			require.NoError(t, err)
			require.True(t, found)
			require.EqualValues(t, 1, value)
		}
		typed := testutils.NewPodCliqueBuilder("pcs", "pcs-uid", "typed-omitted", "default", 0).Build()
		typed.Spec.Replicas = nil
		typed.Spec.MinAvailable = nil
		require.NoError(t, cl.Create(ctx, typed))
		require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(typed), typed))
		require.Equal(t, ptr.To(int32(1)), typed.Spec.Replicas)
		require.Equal(t, ptr.To(int32(1)), typed.Spec.MinAvailable)
		for _, replicas := range []int32{0, 1, 3} {
			t.Run(fmt.Sprintf("replicas=%d", replicas), func(t *testing.T) {
				pclq := testutils.NewPodCliqueBuilder("pcs", "pcs-uid", fmt.Sprintf("minimum-%d", replicas), "default", 0).
					WithReplicas(replicas).Build()
				pclq.Spec.MinAvailable = nil
				require.NoError(t, cl.Create(ctx, pclq))
				require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pclq), pclq))
				require.NotNil(t, pclq.Spec.MinAvailable)
				require.Equal(t, max(int32(1), replicas), *pclq.Spec.MinAvailable)
				require.Equal(t, replicas, ptr.Deref(pclq.Spec.Replicas, 1))
				increasedFields := []string{"spec.minAvailable"}
				if replicas > 0 {
					increasedFields = append(increasedFields, "spec")
				}
				for _, rejection := range []struct {
					patch  string
					fields []string
				}{
					{`{"spec":{"minAvailable":0}}`, []string{"spec.minAvailable", "spec.minAvailable"}},
					{`{"spec":{"minAvailable":-1}}`, []string{"spec.minAvailable", "spec.minAvailable"}},
					{`{"spec":{"minAvailable":null}}`, []string{"spec.minAvailable", "spec.minAvailable"}},
					{fmt.Sprintf(`{"spec":{"minAvailable":%d}}`, *pclq.Spec.MinAvailable+1), increasedFields},
				} {
					err := cl.Patch(ctx, pclq.DeepCopy(), client.RawPatch(types.MergePatchType, []byte(rejection.patch)))
					var statusErr *apierrors.StatusError
					require.ErrorAs(t, err, &statusErr)
					require.Equal(t, metav1.StatusReasonInvalid, statusErr.ErrStatus.Reason)
					var fields []string
					for _, cause := range statusErr.ErrStatus.Details.Causes {
						require.Equal(t, metav1.CauseTypeFieldValueInvalid, cause.Type)
						fields = append(fields, cause.Field)
					}
					require.ElementsMatch(t, rejection.fields, fields)
				}
			})
		}
	})
}
