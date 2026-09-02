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

package charts_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/engine"
	"sigs.k8s.io/yaml"
)

// TestSchedulerProfileConfigRendersNested is a regression test for a chart bug
// where a scheduler profile's `config` block was rendered with too little
// indentation, so keys like `gangScheduling` leaked up to the `scheduler`
// level instead of nesting under `profiles[].config`. That silently disabled
// the default-scheduler gang scheduling backend (GREP-531), because the
// operator parsed an empty profile config.
func TestSchedulerProfileConfigRendersNested(t *testing.T) {
	values := map[string]interface{}{
		"config": map[string]interface{}{
			"scheduler": map[string]interface{}{
				"defaultProfileName": "default-scheduler",
				"profiles": []interface{}{
					map[string]interface{}{
						"name": "default-scheduler",
						"config": map[string]interface{}{
							"gangScheduling": true,
						},
					},
				},
			},
		},
	}

	operatorConfig := renderOperatorConfig(t, values)

	scheduler, ok := operatorConfig["scheduler"].(map[string]interface{})
	require.True(t, ok, "operator config must have a scheduler section")

	// The bug manifested as gangScheduling appearing directly under scheduler.
	_, leaked := scheduler["gangScheduling"]
	require.False(t, leaked, "gangScheduling must not leak to the scheduler level")

	profiles, ok := scheduler["profiles"].([]interface{})
	require.True(t, ok, "scheduler must have profiles")
	require.Len(t, profiles, 1)

	profile, ok := profiles[0].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "default-scheduler", profile["name"])

	profileConfig, ok := profile["config"].(map[string]interface{})
	require.True(t, ok, "profile config must be a nested object")
	require.Equal(t, true, profileConfig["gangScheduling"],
		"gangScheduling must nest under profiles[].config so the backend enables gang scheduling")
}

// renderOperatorConfig renders the operator ConfigMap and returns the parsed
// config.yaml document.
func renderOperatorConfig(t *testing.T, values map[string]interface{}) map[string]interface{} {
	t.Helper()

	chart, err := loader.Load(".")
	require.NoError(t, err)

	renderValues, err := chartutil.ToRenderValues(
		chart,
		values,
		chartutil.ReleaseOptions{Name: "grove", Namespace: "default", IsInstall: true},
		chartutil.DefaultCapabilities,
	)
	require.NoError(t, err)

	manifests, err := engine.Render(chart, renderValues)
	require.NoError(t, err)

	configMapYAML, ok := manifests["grove-charts/templates/configmap-operator.yaml"]
	require.True(t, ok, "configmap-operator.yaml must render")

	var configMap struct {
		Data struct {
			ConfigYAML string `json:"config.yaml"`
		} `json:"data"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(configMapYAML), &configMap))

	operatorConfig := map[string]interface{}{}
	require.NoError(t, yaml.Unmarshal([]byte(configMap.Data.ConfigYAML), &operatorConfig))
	return operatorConfig
}
