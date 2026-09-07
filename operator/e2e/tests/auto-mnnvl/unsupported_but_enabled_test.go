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

package automnnvl

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ai-dynamo/grove/operator/e2e/setup"
	"github.com/ai-dynamo/grove/operator/e2e/testctx"
	"github.com/ai-dynamo/grove/operator/e2e/waiter"
	kubeutils "github.com/ai-dynamo/grove/operator/internal/utils/kubernetes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Test_AutoMNNVL_UnsupportedButEnabled is the test suite for when Auto-MNNVL feature is enabled
// but the ComputeDomain CRD is NOT available in the cluster.
// This tests that the operator detects the invalid configuration and exits.
func Test_AutoMNNVL_UnsupportedButEnabled(t *testing.T) {
	ctx := context.Background()

	// Prepare cluster and get clients (0 = no specific worker node requirement)
	tc, cleanup := testctx.PrepareTest(ctx, t, 0)
	defer cleanup()

	// Detect and validate cluster configuration
	clusterConfig := requireClusterConfig(t, ctx, tc.Client)
	clusterConfig.skipUnless(t, crdUnsupported, featureEnabled)

	// Define all subtests
	subtests := []struct {
		description string
		fn          func(*testing.T, *testctx.TestContext)
	}{
		{"operator exits when CD CRD is missing", testOperatorExitsWithoutCDCRD},
	}

	// Run all subtests
	for _, tt := range subtests {
		t.Run(tt.description, func(t *testing.T) {
			tt.fn(t, tc)
		})
	}
}

// testOperatorExitsWithoutCDCRD verifies that the operator fails preflight
// when MNNVL is enabled but the ComputeDomain CRD is missing.
func testOperatorExitsWithoutCDCRD(t *testing.T, tc *testctx.TestContext) {
	pod, err := waitForFailedOperatorPod(tc)
	require.NoError(t, err, "Failed to find grove-operator pod")

	hasTerminated := false
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Terminated != nil || status.LastTerminationState.Terminated != nil {
			hasTerminated = true
			break
		}
	}
	assert.True(t, hasTerminated, "Operator pod should terminate on preflight failure")
}

// waitForFailedOperatorPod polls all operator pods until it finds the
// non-terminating pod whose current or previous logs contain the expected
// preflight failure. A rolling deployment can leave an old restarted pod
// NotReady while it terminates, so pod state alone is not sufficient.
func waitForFailedOperatorPod(tc *testctx.TestContext) (*corev1.Pod, error) {
	w := waiter.New[*corev1.Pod]().
		WithTimeout(defaultPollTimeout).
		WithInterval(defaultPollInterval)
	fetchFailedPod := waiter.FetchFunc[*corev1.Pod](func(ctx context.Context) (*corev1.Pod, error) {
		var podList corev1.PodList
		listErr := tc.Client.List(ctx, &podList, client.InNamespace(groveOperatorNamespace), setup.OperatorPodLabels)
		if listErr != nil {
			return nil, fmt.Errorf("list grove operator pods: %w", listErr)
		}

		for i := range podList.Items {
			pod := &podList.Items[i]
			if !isFailedOperatorPodCandidate(pod) {
				continue
			}
			if containsMNNVLPreflightFailure(fetchOperatorLogs(ctx, tc, pod.Name)) {
				return pod, nil
			}
		}
		return nil, nil
	})
	operatorPod, err := w.WaitFor(tc.Ctx, fetchFailedPod, waiter.IsNotZero[*corev1.Pod])
	return operatorPod, err
}

func isFailedOperatorPodCandidate(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || kubeutils.IsPodReady(pod) {
		return false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Terminated != nil ||
			status.LastTerminationState.Terminated != nil ||
			status.RestartCount > 0 {
			return true
		}
	}
	return false
}

func fetchOperatorLogs(ctx context.Context, tc *testctx.TestContext, podName string) []byte {
	var allLogs []byte
	for _, previous := range []bool{false, true} {
		logs, err := tc.Client.GetLogs(groveOperatorNamespace, podName, &corev1.PodLogOptions{
			Previous: previous,
		}).DoRaw(ctx)
		if err == nil {
			allLogs = append(allLogs, logs...)
		}
	}
	return allLogs
}

func containsMNNVLPreflightFailure(logs []byte) bool {
	logText := string(logs)
	return strings.Contains(logText, "MNNVL preflight check failed") &&
		strings.Contains(logText, "ComputeDomain CRD")
}
