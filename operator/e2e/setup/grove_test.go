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

package setup

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

func TestGroveConfigToHelmValuesEnablesHealthProbes(t *testing.T) {
	values, err := (&GroveConfig{}).toHelmValues()
	require.NoError(t, err)

	enabled, found, err := unstructured.NestedBool(values, "config", "server", "healthProbes", "enable")
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, enabled)

	port, found, err := unstructured.NestedFieldNoCopy(values, "config", "server", "healthProbes", "port")
	require.NoError(t, err)
	require.True(t, found)
	assert.EqualValues(t, DefaultHealthProbePort, port)
}

func TestGroveConfigToHelmValuesIncludesSchedulerConfig(t *testing.T) {
	values, err := (&GroveConfig{
		Scheduler: &configv1alpha1.SchedulerConfiguration{
			DefaultProfileName: string(configv1alpha1.SchedulerNameKube),
			Profiles: []configv1alpha1.SchedulerProfile{
				{
					Name: configv1alpha1.SchedulerNameKube,
					Config: &runtime.RawExtension{
						Raw: []byte(`{"gangScheduling":true}`),
					},
				},
			},
		},
	}).toHelmValues()
	require.NoError(t, err)

	defaultProfile, found, err := unstructured.NestedString(values, "config", "scheduler", "defaultProfileName")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, string(configv1alpha1.SchedulerNameKube), defaultProfile)

	profiles, found, err := unstructured.NestedSlice(values, "config", "scheduler", "profiles")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, profiles, 1)
	profile, ok := profiles[0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, string(configv1alpha1.SchedulerNameKube), profile["name"])
	config, ok := profile["config"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, true, config["gangScheduling"])
}

func TestWaitForWebhookReadyRequiresConsecutiveSuccesses(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/namespaces/grove-system/services/https:grove-operator:webhooks/proxy/webhooks/default-podcliqueset" {
			http.NotFound(w, r)
			return
		}
		switch calls.Add(1) {
		case 1, 2, 4:
			http.Error(w, "webhook unavailable", http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	err := waitForWebhookReady(
		t.Context(),
		&rest.Config{Host: server.URL},
		time.Second,
		time.Millisecond,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, int32(7), calls.Load())
}
