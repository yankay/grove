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

package k8sclient

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
)

// Exec runs a command without a shell in a Pod container. The caller supplies
// a bounded context so an unresponsive command cannot stall the suite.
func (k *Client) Exec(ctx context.Context, namespace, pod, container string, command []string) (string, error) {
	req := k.clientset.CoreV1().RESTClient().Post().
		Namespace(namespace).Resource("pods").Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container, Command: command, Stdout: true, Stderr: true,
		}, clientgoscheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(k.RestConfig, http.MethodPost, req.URL())
	if err != nil {
		return "", fmt.Errorf("create Pod exec: %w", err)
	}
	var stdout, stderr bytes.Buffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
		return stdout.String(), fmt.Errorf("exec %s/%s: %w: %s", namespace, pod, err, stderr.String())
	}
	return stdout.String(), nil
}
