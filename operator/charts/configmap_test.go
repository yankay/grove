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
	"slices"
	"testing"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/engine"
	rbacv1 "k8s.io/api/rbac/v1"
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

func TestDefaultSchedulerGangSchedulingDisabledByDefault(t *testing.T) {
	configMap := renderOperatorConfig(t, nil)
	data, err := yaml.Marshal(configMap)
	require.NoError(t, err)
	var config configv1alpha1.OperatorConfiguration
	require.NoError(t, yaml.Unmarshal(data, &config))
	var found bool
	for _, profile := range config.Scheduler.Profiles {
		if profile.Name != configv1alpha1.SchedulerNameKube {
			continue
		}
		found = true
		var kubeConfig configv1alpha1.KubeSchedulerConfig
		if profile.Config != nil {
			require.NoError(t, yaml.Unmarshal(profile.Config.Raw, &kubeConfig))
		}
		assert.False(t, kubeConfig.GangScheduling)
	}
	require.True(t, found, "default-scheduler profile must remain available")
}

func TestDefaultSchedulerGangSchedulingRBAC(t *testing.T) {
	tests := []struct {
		name     string
		profiles []interface{}
		enabled  bool
	}{
		{name: "chart defaults"},
		{
			name:     "disabled kube profile",
			profiles: []interface{}{map[string]interface{}{"name": "default-scheduler", "config": map[string]interface{}{"gangScheduling": false}}},
		},
		{
			name:     "enabled kube profile",
			profiles: []interface{}{map[string]interface{}{"name": "default-scheduler", "config": map[string]interface{}{"gangScheduling": true}}},
			enabled:  true,
		},
		{
			name:     "other backend config does not enable WAS",
			profiles: []interface{}{map[string]interface{}{"name": "kai-scheduler", "config": map[string]interface{}{"gangScheduling": true}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := map[string]interface{}{}
			if tt.profiles != nil {
				values["config"] = map[string]interface{}{"scheduler": map[string]interface{}{"profiles": tt.profiles}}
			}
			manifests := renderChart(t, values)
			var role rbacv1.ClusterRole
			require.NoError(t, yaml.Unmarshal([]byte(manifests["grove-charts/templates/clusterrole.yaml"]), &role))
			var rules []rbacv1.PolicyRule
			for _, rule := range role.Rules {
				if slices.Contains(rule.APIGroups, "scheduling.k8s.io") {
					rules = append(rules, rule)
				}
			}
			if !tt.enabled {
				assert.Empty(t, rules)
				return
			}
			require.Len(t, rules, 1)
			assert.ElementsMatch(t, []string{"workloads", "podgroups", "compositepodgroups"}, rules[0].Resources)
			assert.ElementsMatch(t, []string{"create", "get", "list", "watch", "patch", "update", "delete", "deletecollection"}, rules[0].Verbs)
		})
	}
}

// renderOperatorConfig renders the operator ConfigMap and returns the parsed
// config.yaml document.
func renderOperatorConfig(t *testing.T, values map[string]interface{}) map[string]interface{} {
	t.Helper()
	manifests := renderChart(t, values)

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

func renderChart(t *testing.T, values map[string]interface{}) map[string]string {
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
	return manifests
}
