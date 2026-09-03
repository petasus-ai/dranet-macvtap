/*
Copyright The Kubernetes Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"google.golang.org/grpc"
	drahealthv1alpha1 "k8s.io/kubelet/pkg/apis/dra-health/v1alpha1"
)

// NodeWatchResources serves the kubelet's DRAResourceHealth stream. The driver
// tracks no per-device health, so it sends one empty update and holds the
// stream open until the kubelet closes it; a plugin without the service makes
// the kubelet reconnect every few seconds and log each attempt.
func (np *NetworkDriver) NodeWatchResources(_ *drahealthv1alpha1.NodeWatchResourcesRequest, stream grpc.ServerStreamingServer[drahealthv1alpha1.NodeWatchResourcesResponse]) error {
	if err := stream.Send(&drahealthv1alpha1.NodeWatchResourcesResponse{}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}
