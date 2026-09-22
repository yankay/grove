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
	"testing"

	groveconfigv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	pcsdefaulting "github.com/ai-dynamo/grove/operator/internal/webhook/admission/pcs/defaulting"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestDefaultedIdleTemplatesValidation(t *testing.T) {
	for _, groupIdle := range []bool{false, true} {
		name := "standalone"
		if groupIdle {
			name = "scaling-group"
		}
		t.Run(name, func(t *testing.T) {
			pcs := createTestPodCliqueSet("inference")
			pcs.Spec.Template.Cliques[0].Spec.MinAvailable = ptr.To(int32(2))
			if groupIdle {
				pcs.Spec.Template.Cliques[0].Spec.Replicas = 2
				pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
					Name: "workers", CliqueNames: []string{pcs.Spec.Template.Cliques[0].Name},
					Replicas: ptr.To(int32(0)), MinAvailable: ptr.To(int32(2)),
				}}
			} else {
				pcs.Spec.Template.Cliques[0].Spec.Replicas = 0
			}

			ctx := admission.NewContextWithRequest(t.Context(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Create,
			}})
			defaulter := pcsdefaulting.NewHandler(&testutils.FakeManager{Logger: logr.Discard()})
			require.NoError(t, defaulter.Default(ctx, pcs))
			var rollingUpdate *grovecorev1alpha1.RollingUpdateConfiguration
			if groupIdle {
				rollingUpdate = pcs.Spec.Template.PodCliqueScalingGroupConfigs[0].RollingUpdate
			} else {
				rollingUpdate = pcs.Spec.Template.Cliques[0].RollingUpdate
			}
			require.NotNil(t, rollingUpdate)
			require.Equal(t, ptr.To(int32(1)), rollingUpdate.MaxUnavailable)

			validator := newPCSValidator(pcs, admissionv1.Create, defaultTASConfig(), groveconfigv1alpha1.SchedulerConfiguration{
				Profiles:           []groveconfigv1alpha1.SchedulerProfile{{Name: groveconfigv1alpha1.SchedulerNameKube}},
				DefaultProfileName: string(groveconfigv1alpha1.SchedulerNameKube),
			}, nil, testutils.NewDefaultFakeRegistry())
			_, errs := validator.validate()
			require.NoError(t, errs.ToAggregate())
		})
	}
}
