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

package crdinstaller_test

import (
	"context"
	"os"
	"testing"
	"time"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	operatorcrds "github.com/ai-dynamo/grove/operator/api/core/v1alpha1/crds"
	"github.com/ai-dynamo/grove/operator/internal/crdinstaller"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"
)

func TestReplicaValidationUpgrade(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	const apiTimeout = 2 * time.Minute
	const convergenceTimeout = 30 * time.Second
	const pollInterval = 100 * time.Millisecond
	testEnv := &envtest.Environment{
		UseExistingCluster:       ptr.To(false),
		ControlPlaneStartTimeout: convergenceTimeout,
		ControlPlaneStopTimeout:  convergenceTimeout,
	}
	cfg, err := testEnv.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testEnv.Stop()) })
	cfg.Timeout = convergenceTimeout
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, apiextensionsv1.AddToScheme(scheme))
	require.NoError(t, grovecorev1alpha1.AddToScheme(scheme))
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), apiTimeout)
	defer cancel()
	require.NoError(t, cl.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "replica-upgrade"}}))

	for _, tc := range []struct {
		kind     string
		crd      string
		spec     map[string]interface{}
		path     []string
		newValue interface{}
	}{
		{
			kind: "PodClique", crd: operatorcrds.PodCliqueCRD(),
			spec: map[string]interface{}{
				"replicas": int64(1), "minAvailable": int64(3), "roleName": "worker",
				"podSpec": map[string]interface{}{
					"containers": []interface{}{map[string]interface{}{"name": "worker", "image": "test:v1"}},
				},
			},
			path:     []string{"spec", "podSpec", "containers"},
			newValue: []interface{}{map[string]interface{}{"name": "worker", "image": "test:v2"}},
		},
		{
			kind: "PodCliqueScalingGroup", crd: operatorcrds.PodCliqueScalingGroupCRD(),
			spec: map[string]interface{}{
				"replicas": int64(1), "minAvailable": int64(3), "cliqueNames": []interface{}{"worker"},
			},
			path: []string{"spec", "cliqueNames"}, newValue: []interface{}{"leader", "worker"},
		},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			currentCRD := &apiextensionsv1.CustomResourceDefinition{}
			require.NoError(t, yaml.Unmarshal([]byte(tc.crd), currentCRD))
			legacyCRD := currentCRD.DeepCopy()
			// Model the pre-validation API without snapshotting a historical CRD.
			specSchema := legacyCRD.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]
			specSchema.XValidations = nil
			legacyCRD.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"] = specSchema
			require.NoError(t, cl.Create(ctx, legacyCRD))
			require.NoError(t, envtest.WaitForCRDs(cfg, []*apiextensionsv1.CustomResourceDefinition{legacyCRD},
				envtest.CRDInstallOptions{MaxTime: convergenceTimeout, PollInterval: pollInterval}))
			legacy := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": grovecorev1alpha1.SchemeGroupVersion.String(), "kind": tc.kind,
				"metadata": map[string]interface{}{"name": "legacy", "namespace": "replica-upgrade"},
				"spec":     tc.spec,
			}}
			require.NoError(t, cl.Create(ctx, legacy))
			uid := legacy.GetUID()
			require.NoError(t, crdinstaller.InstallCRDs(ctx, cl, logr.Discard(), []string{tc.crd}))
			probe := legacy.DeepCopy()
			probe.SetName("invalid-create")
			probe.SetResourceVersion("")
			probe.SetUID("")
			require.Eventually(t, func() bool {
				return apierrors.IsInvalid(cl.Create(ctx, probe.DeepCopy(), client.DryRunAll))
			}, convergenceTimeout, pollInterval, "new below-quorum objects must be rejected after upgrade")

			legacy.SetLabels(map[string]string{"upgrade": "retained"})
			legacy.SetFinalizers([]string{"test.grove.io/hold"})
			require.NoError(t, cl.Update(ctx, legacy))
			require.NoError(t, unstructured.SetNestedField(legacy.Object, tc.newValue, tc.path...))
			require.NoError(t, cl.Update(ctx, legacy), "unrelated spec updates must not require a replica repair")
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(legacy), legacy))
			value, found, err := unstructured.NestedFieldNoCopy(legacy.Object, tc.path...)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, tc.newValue, value)
			legacy.SetFinalizers(nil)
			require.NoError(t, cl.Update(ctx, legacy))
			require.NoError(t, cl.SubResource("scale").Patch(ctx, legacy,
				client.RawPatch(types.MergePatchType, []byte(`{"spec":{"replicas":1}}`))))
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(legacy), legacy))

			for _, field := range []string{"replicas", "minAvailable"} {
				t.Run("reject changed "+field, func(t *testing.T) {
					candidate := legacy.DeepCopy()
					require.NoError(t, unstructured.SetNestedField(candidate.Object, int64(2), "spec", field))
					var statusErr *apierrors.StatusError
					require.ErrorAs(t, cl.Update(ctx, candidate), &statusErr)
					require.Equal(t, metav1.StatusReasonInvalid, statusErr.ErrStatus.Reason)
					require.Len(t, statusErr.ErrStatus.Details.Causes, 1)
					require.Equal(t, "spec", statusErr.ErrStatus.Details.Causes[0].Field)
				})
			}
			require.True(t, apierrors.IsInvalid(cl.SubResource("scale").Patch(ctx, legacy,
				client.RawPatch(types.MergePatchType, []byte(`{"spec":{"replicas":2}}`)))))
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(legacy), legacy))
			replicas, _, err := unstructured.NestedInt64(legacy.Object, "spec", "replicas")
			require.NoError(t, err)
			require.EqualValues(t, 1, replicas, "rejected requests must preserve the persisted target")
			minimum, _, err := unstructured.NestedInt64(legacy.Object, "spec", "minAvailable")
			require.NoError(t, err)
			require.EqualValues(t, 3, minimum)

			// Repair, positive scaling, idle, and wake all retain the same object.
			for _, subresource := range []string{"", "scale"} {
				for _, target := range []int32{3, 4, 3, 0, 3} {
					require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(legacy), legacy))
					if subresource == "" {
						require.NoError(t, unstructured.SetNestedField(legacy.Object, int64(target), "spec", "replicas"))
						require.NoError(t, cl.Update(ctx, legacy))
					} else {
						before := legacy.DeepCopy()
						require.NoError(t, unstructured.SetNestedField(legacy.Object, int64(target), "spec", "replicas"))
						require.NoError(t, cl.SubResource("scale").Patch(ctx, legacy, client.MergeFrom(before)))
					}
					require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(legacy), legacy))
					got, _, err := unstructured.NestedInt64(legacy.Object, "spec", "replicas")
					require.NoError(t, err)
					require.EqualValues(t, target, got)
					require.Equal(t, uid, legacy.GetUID())
					require.True(t, apierrors.IsInvalid(cl.SubResource("scale").Patch(ctx, legacy,
						client.RawPatch(types.MergePatchType, []byte(`{"spec":{"replicas":2}}`)))))
				}
			}
		})
	}

	t.Run("PodCliqueSet template", func(t *testing.T) {
		currentCRD := &apiextensionsv1.CustomResourceDefinition{}
		require.NoError(t, yaml.Unmarshal([]byte(operatorcrds.PodCliqueSetCRD()), currentCRD))
		legacyCRD := currentCRD.DeepCopy()
		cliqueSchema := legacyCRD.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].
			Properties["template"].Properties["cliques"].Items.Schema
		specSchema := cliqueSchema.Properties["spec"]
		specSchema.XValidations = nil
		cliqueSchema.Properties["spec"] = specSchema
		require.NoError(t, cl.Create(ctx, legacyCRD))
		require.NoError(t, envtest.WaitForCRDs(cfg, []*apiextensionsv1.CustomResourceDefinition{legacyCRD},
			envtest.CRDInstallOptions{MaxTime: convergenceTimeout, PollInterval: pollInterval}))
		pcs := testutils.NewPodCliqueSetBuilder("legacy", "replica-upgrade", "").
			WithPodCliqueParameters("worker", 1, nil).Build()
		pcs.Spec.Template.Cliques[0].Spec.MinAvailable = ptr.To(int32(3))
		require.NoError(t, cl.Create(ctx, pcs))
		require.NoError(t, crdinstaller.InstallCRDs(ctx, cl, logr.Discard(), []string{operatorcrds.PodCliqueSetCRD()}))
		probe := pcs.DeepCopy()
		probe.Name, probe.ResourceVersion, probe.UID = "invalid-create", "", ""
		require.Eventually(t, func() bool {
			return apierrors.IsInvalid(cl.Create(ctx, probe.DeepCopy(), client.DryRunAll))
		}, convergenceTimeout, pollInterval)
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Image = "test:v2"
		require.NoError(t, cl.Update(ctx, pcs))
		require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pcs), pcs))
		require.Equal(t, "test:v2", pcs.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Image)
		require.EqualValues(t, 1, pcs.Spec.Template.Cliques[0].Spec.Replicas)
		pcs.Spec.Template.Cliques[0].Spec.Replicas = 2
		require.True(t, apierrors.IsInvalid(cl.Update(ctx, pcs)))
	})
}
