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

package k8s

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CountWarningEventOccurrences counts observed warnings for the target's UID and
// reason, including aggregated occurrences within each Event.
func CountWarningEventOccurrences(ctx context.Context, cl client.Reader, target client.Object, reason string) (int32, error) {
	if target.GetUID() == "" {
		return 0, fmt.Errorf("cannot count warning events for %s without a UID", client.ObjectKeyFromObject(target))
	}
	events := &corev1.EventList{}
	if err := cl.List(ctx, events, client.InNamespace(target.GetNamespace())); err != nil {
		return 0, err
	}
	var occurrences int32
	for _, event := range events.Items {
		if event.InvolvedObject.UID != target.GetUID() || event.Type != corev1.EventTypeWarning || event.Reason != reason {
			continue
		}
		// Count and Series.Count describe the same occurrences, not separate sets.
		count := max(1, event.Count)
		if event.Series != nil {
			count = max(count, event.Series.Count)
		}
		occurrences += count
	}
	return occurrences, nil
}
