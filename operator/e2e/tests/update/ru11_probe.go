//go:build e2e

// /*
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
// */

package update

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	common "github.com/ai-dynamo/grove/operator/api/common"
	grovev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/e2e/testctx"
	"github.com/ai-dynamo/grove/operator/e2e/tests"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ru11ResourceState struct {
	ResourceVersion string            `json:"resourceVersion"`
	Generation      int64             `json:"generation"`
	Labels          map[string]string `json:"labels,omitempty"`
	SpecReplicas    int32             `json:"specReplicas"`
	Status          any               `json:"status"`
}

type ru11PodState struct {
	Total            int            `json:"total"`
	Ready            int            `json:"ready"`
	HashDistribution map[string]int `json:"hashDistribution"`
	Deleting         []string       `json:"deleting,omitempty"`
}

type ru11ProbeSnapshot struct {
	PCS   ru11ResourceState            `json:"pcs"`
	PCSGs map[string]ru11ResourceState `json:"pcsgs"`
	PCLQs map[string]ru11ResourceState `json:"pclqs"`
	Pods  ru11PodState                 `json:"pods"`
}

func startRU11StatusProbe(tc *testctx.TestContext, interval time.Duration) func() {
	ctx, cancel := context.WithCancel(tc.Ctx)
	done := make(chan struct{})
	startedAt := time.Now()

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		var previousSnapshot string
		tick := 0
		capture := func(reason string, force bool) {
			tick++
			encoded, err := marshalRU11ProbeSnapshot(ctx, tc)
			if err != nil {
				tests.Logger.Infof("[RU11-PROBE] tick=%d elapsed=%s reason=%s error=%q",
					tick, time.Since(startedAt).Round(time.Millisecond), reason, err)
				return
			}

			if !force && encoded == previousSnapshot {
				tests.Logger.Infof("[RU11-PROBE] tick=%d elapsed=%s reason=%s state=unchanged",
					tick, time.Since(startedAt).Round(time.Millisecond), reason)
				return
			}

			previousSnapshot = encoded
			tests.Logger.Infof("[RU11-PROBE] tick=%d elapsed=%s reason=%s state=%s",
				tick, time.Since(startedAt).Round(time.Millisecond), reason, encoded)
		}

		capture("started", true)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				capture("periodic", false)
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

func logRU11StatusSnapshot(tc *testctx.TestContext, reason string) {
	encoded, err := marshalRU11ProbeSnapshot(tc.Ctx, tc)
	if err != nil {
		tests.Logger.Infof("[RU11-PROBE] reason=%s finalError=%q", reason, err)
		return
	}
	tests.Logger.Infof("[RU11-PROBE] reason=%s finalState=%s", reason, encoded)
}

func marshalRU11ProbeSnapshot(ctx context.Context, tc *testctx.TestContext) (string, error) {
	snapshot, err := captureRU11ProbeSnapshot(ctx, tc)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return "", fmt.Errorf("marshal snapshot: %w", err)
	}
	return string(encoded), nil
}

func captureRU11ProbeSnapshot(ctx context.Context, tc *testctx.TestContext) (*ru11ProbeSnapshot, error) {
	workloadLabels := client.MatchingLabels(common.GetDefaultLabelsForPodCliqueSetManagedResources(tc.Workload.Name))

	var pcs grovev1alpha1.PodCliqueSet
	if err := tc.Client.Get(ctx, client.ObjectKey{Name: tc.Workload.Name, Namespace: tc.Namespace}, &pcs); err != nil {
		return nil, fmt.Errorf("get PCS: %w", err)
	}

	var pcsgList grovev1alpha1.PodCliqueScalingGroupList
	if err := tc.Client.List(ctx, &pcsgList, client.InNamespace(tc.Namespace), workloadLabels); err != nil {
		return nil, fmt.Errorf("list PCSGs: %w", err)
	}

	var pclqList grovev1alpha1.PodCliqueList
	if err := tc.Client.List(ctx, &pclqList, client.InNamespace(tc.Namespace), workloadLabels); err != nil {
		return nil, fmt.Errorf("list PCLQs: %w", err)
	}

	var podList corev1.PodList
	if err := tc.Client.List(ctx, &podList, client.InNamespace(tc.Namespace), workloadLabels); err != nil {
		return nil, fmt.Errorf("list Pods: %w", err)
	}

	snapshot := &ru11ProbeSnapshot{
		PCS: ru11ResourceState{
			ResourceVersion: pcs.ResourceVersion,
			Generation:      pcs.Generation,
			SpecReplicas:    pcs.Spec.Replicas,
			Status:          pcs.Status,
		},
		PCSGs: make(map[string]ru11ResourceState, len(pcsgList.Items)),
		PCLQs: make(map[string]ru11ResourceState, len(pclqList.Items)),
		Pods: ru11PodState{
			HashDistribution: map[string]int{},
		},
	}

	for _, pcsg := range pcsgList.Items {
		snapshot.PCSGs[pcsg.Name] = ru11ResourceState{
			ResourceVersion: pcsg.ResourceVersion,
			Generation:      pcsg.Generation,
			Labels:          ru11RelevantLabels(pcsg.Labels),
			SpecReplicas:    pcsg.Spec.Replicas,
			Status:          pcsg.Status,
		}
	}

	for _, pclq := range pclqList.Items {
		snapshot.PCLQs[pclq.Name] = ru11ResourceState{
			ResourceVersion: pclq.ResourceVersion,
			Generation:      pclq.Generation,
			Labels:          ru11RelevantLabels(pclq.Labels),
			SpecReplicas:    pclq.Spec.Replicas,
			Status:          pclq.Status,
		}
	}

	for i := range podList.Items {
		pod := &podList.Items[i]
		snapshot.Pods.Total++
		if isRU11PodReady(pod) {
			snapshot.Pods.Ready++
		}
		if pod.DeletionTimestamp != nil {
			snapshot.Pods.Deleting = append(snapshot.Pods.Deleting, pod.Name)
		}

		hashKey := strings.Join([]string{
			pod.Labels[common.LabelPodClique],
			pod.Labels[common.LabelPodTemplateHash],
			pod.Labels[common.LabelPodCliqueSetReplicaIndex],
			pod.Labels[common.LabelPodCliqueScalingGroupReplicaIndex],
		}, "|")
		snapshot.Pods.HashDistribution[hashKey]++
	}
	sort.Strings(snapshot.Pods.Deleting)

	return snapshot, nil
}

func ru11RelevantLabels(labels map[string]string) map[string]string {
	relevant := map[string]string{}
	for _, key := range []string{
		common.LabelPodCliqueSetReplicaIndex,
		common.LabelPodCliqueScalingGroup,
		common.LabelPodCliqueScalingGroupReplicaIndex,
		common.LabelPodTemplateHash,
	} {
		if value, ok := labels[key]; ok {
			relevant[key] = value
		}
	}
	return relevant
}

func isRU11PodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
