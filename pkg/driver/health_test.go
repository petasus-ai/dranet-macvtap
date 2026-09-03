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
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	drahealthv1alpha1 "k8s.io/kubelet/pkg/apis/dra-health/v1alpha1"
)

// fakeHealthStream records what NodeWatchResources sends and ends when its
// context is canceled.
type fakeHealthStream struct {
	grpc.ServerStream
	ctx     context.Context
	sent    []*drahealthv1alpha1.NodeWatchResourcesResponse
	sendErr error
}

func (s *fakeHealthStream) Context() context.Context { return s.ctx }

func (s *fakeHealthStream) Send(resp *drahealthv1alpha1.NodeWatchResourcesResponse) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sent = append(s.sent, resp)
	return nil
}

func TestNodeWatchResources(t *testing.T) {
	var _ drahealthv1alpha1.DRAResourceHealthServer = &NetworkDriver{}

	ctx, cancel := context.WithCancel(context.Background())
	stream := &fakeHealthStream{ctx: ctx}
	done := make(chan error, 1)
	go func() {
		done <- (&NetworkDriver{}).NodeWatchResources(&drahealthv1alpha1.NodeWatchResourcesRequest{}, stream)
	}()

	select {
	case err := <-done:
		t.Fatalf("stream ended before the kubelet closed it: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not end after the context was canceled")
	}
	if len(stream.sent) != 1 || len(stream.sent[0].Devices) != 0 {
		t.Fatalf("expected one empty update, got %v", stream.sent)
	}
}

func TestNodeWatchResourcesSendError(t *testing.T) {
	want := errors.New("closed")
	stream := &fakeHealthStream{ctx: context.Background(), sendErr: want}
	if err := (&NetworkDriver{}).NodeWatchResources(&drahealthv1alpha1.NodeWatchResourcesRequest{}, stream); !errors.Is(err, want) {
		t.Fatalf("expected %v, got %v", want, err)
	}
}
