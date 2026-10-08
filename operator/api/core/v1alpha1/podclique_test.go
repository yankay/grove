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

package v1alpha1

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
)

func TestPodCliqueReplicaSerialization(t *testing.T) {
	for _, tc := range []struct {
		name     string
		replicas *int32
	}{
		{name: "omitted"},
		{name: "idle", replicas: ptr.To(int32(0))},
		{name: "active", replicas: ptr.To(int32(3))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := PodCliqueSpec{Replicas: tc.replicas}
			data, err := json.Marshal(spec)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data, &fields))
			value, present := fields["replicas"]
			require.Equal(t, tc.replicas != nil, present)
			if present {
				var replicas int32
				require.NoError(t, json.Unmarshal(value, &replicas))
				require.Equal(t, *tc.replicas, replicas)
			}
			var decoded PodCliqueSpec
			require.NoError(t, json.Unmarshal(data, &decoded))
			require.Equal(t, tc.replicas, decoded.Replicas)

			copy := spec.DeepCopy()
			require.Equal(t, spec.Replicas, copy.Replicas)
			if copy.Replicas != nil {
				*copy.Replicas = 7
				require.Equal(t, *tc.replicas, *spec.Replicas, "deep copy must not alias replica intent")
				require.NotEqual(t, *copy.Replicas, *spec.Replicas)
			}
		})
	}
}
