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

package podgroup

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	kaischedulingv2alpha2 "github.com/kai-scheduler/KAI-scheduler/pkg/apis/scheduling/v2alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	volcanov1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

// GetNativePodGroup fetches the backend resource selected by the PodGang's scheduler profile.
func GetNativePodGroup(ctx context.Context, cl client.Client, gang *groveschedulerv1alpha1.PodGang) (client.Object, error) {
	var native client.Object
	switch configv1alpha1.SchedulerName(gang.Labels[apicommon.LabelSchedulerName]) {
	case configv1alpha1.SchedulerNameKai:
		native = &kaischedulingv2alpha2.PodGroup{}
	case configv1alpha1.SchedulerNameVolcano:
		native = &volcanov1beta1.PodGroup{}
	default:
		return nil, fmt.Errorf("unsupported native scheduler %q", gang.Labels[apicommon.LabelSchedulerName])
	}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(gang), native); err != nil {
		return nil, err
	}
	return native, nil
}

// VerifyFlatMembership checks the complete native policy for a gang without topology groups.
// It is intended for active gangs; Volcano may retain old floors on an all-zero coherent release.
func VerifyFlatMembership(gang *groveschedulerv1alpha1.PodGang, native client.Object) error {
	if !metav1.IsControlledBy(native, gang) || !native.GetDeletionTimestamp().IsZero() {
		return fmt.Errorf("native PodGroup is not a live child of PodGang %s", gang.Name)
	}
	want := make(map[string]int32, len(gang.Spec.PodGroups))
	var total int32
	for _, group := range gang.Spec.PodGroups {
		want[group.Name] = group.MinReplicas
		total += group.MinReplicas
	}
	got := make(map[string]int32)
	var actualTotal int32
	switch pg := native.(type) {
	case *kaischedulingv2alpha2.PodGroup:
		if pg.Spec.StalenessGracePeriod == nil || pg.Spec.StalenessGracePeriod.Duration != -time.Second {
			return fmt.Errorf("managed KAI PodGroup %s must disable its own stale-gang eviction", pg.Name)
		}
		actualTotal = ptr.Deref(pg.Spec.MinMember, -1)
		for _, group := range pg.Spec.SubGroups {
			if group.Parent != nil {
				return fmt.Errorf("unexpected parent on flat subgroup %s", group.Name)
			}
			got[group.Name] = ptr.Deref(group.MinMember, -1)
		}
	case *volcanov1beta1.PodGroup:
		actualTotal = pg.Spec.MinMember
		for name, minimum := range want {
			want[name] = max(minimum, 1)
		}
		for _, group := range pg.Spec.SubGroupPolicy {
			if group.LabelSelector == nil ||
				!maps.Equal(group.LabelSelector.MatchLabels, map[string]string{apicommon.LabelPodClique: group.Name}) ||
				!slices.Equal(group.MatchLabelKeys, []string{apicommon.LabelPodClique}) ||
				len(group.LabelSelector.MatchExpressions) != 0 {
				return fmt.Errorf("unexpected subgroup selector for %s", group.Name)
			}
			size, count := ptr.Deref(group.SubGroupSize, -1), ptr.Deref(group.MinSubGroups, -1)
			if count != 1 {
				return fmt.Errorf("unexpected subgroup count for %s", group.Name)
			}
			got[group.Name] = size * count
		}
	default:
		return fmt.Errorf("unexpected native resource %T", native)
	}
	if actualTotal != total || !maps.Equal(want, got) {
		return fmt.Errorf("native minimum %d/%v, expected %d/%v", actualTotal, got, total, want)
	}
	return nil
}
