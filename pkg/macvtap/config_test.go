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

package macvtap

import (
	"os"
	"path/filepath"
	"testing"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr bool
	}{
		{
			name: "valid",
			content: `
pools:
  - name: mgmt
    parent: eth0
    parentByNode:
      node-a: eno4
    capacity: 16
    mode: bridge
    mtu: 1500
`,
		},
		{
			name: "duplicate pool name",
			content: `
pools:
  - name: mgmt
    parent: eth0
    capacity: 1
  - name: mgmt
    parent: eth1
    capacity: 1
`,
			wantErr: true,
		},
		{
			name: "long pool name accepted (host link name is hashed)",
			content: `
pools:
  - name: averylongpoolname
    parent: eth0
    capacity: 10
`,
		},
		{
			name: "missing parent",
			content: `
pools:
  - name: mgmt
    capacity: 4
`,
			wantErr: true,
		},
		{
			name: "bad mode",
			content: `
pools:
  - name: mgmt
    parent: eth0
    capacity: 4
    mode: taxi
`,
			wantErr: true,
		},
		{
			name: "zero capacity",
			content: `
pools:
  - name: mgmt
    parent: eth0
    capacity: 0
`,
			wantErr: true,
		},
		{
			name: "unknown field rejected",
			content: `
pools:
  - name: mgmt
    parent: eth0
    capacity: 4
    lowerDevice: eth0
`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadConfig(writeConfig(t, tt.content))
			if (err != nil) != tt.wantErr {
				t.Fatalf("LoadConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParentForNode(t *testing.T) {
	pool := PoolConfig{
		Name:         "mgmt",
		Parent:       "eth0",
		ParentByNode: map[string]string{"node-a": "eno4"},
	}
	if got := pool.ParentForNode("node-a"); got != "eno4" {
		t.Errorf("ParentForNode(node-a) = %q, want eno4", got)
	}
	if got := pool.ParentForNode("node-b"); got != "eth0" {
		t.Errorf("ParentForNode(node-b) = %q, want eth0", got)
	}
}

func TestHostIfName(t *testing.T) {
	pool := PoolConfig{Name: "a-very-long-pool-name", Parent: "eth0", Capacity: 16}
	name := HostIfName(pool.DeviceName(15))
	if len(name) != 15 {
		t.Errorf("HostIfName %q must be exactly 15 chars (IFNAMSIZ-1)", name)
	}
	if name[:3] != "mvt" {
		t.Errorf("HostIfName %q must carry the mvt prefix", name)
	}
	if HostIfName(pool.DeviceName(14)) == name {
		t.Errorf("HostIfName must differ per device")
	}
	if HostIfName(pool.DeviceName(15)) != name {
		t.Errorf("HostIfName must be deterministic")
	}
}

func TestBuildDevices(t *testing.T) {
	// "lo" always exists, so the pool that uses it must be advertised while
	// the pool with a bogus parent must be skipped.
	path := writeConfig(t, `
pools:
  - name: mgmt
    parent: lo
    capacity: 2
  - name: ghost
    parent: does-not-exist0
    capacity: 2
`)
	inv := New(path, "test-node")
	devices, specs := inv.buildDevices()
	if len(devices) != 2 {
		t.Fatalf("expected 2 devices, got %d", len(devices))
	}
	for i, want := range []string{"mgmt-0", "mgmt-1"} {
		if devices[i].Name != want {
			t.Errorf("device[%d] = %q, want %q", i, devices[i].Name, want)
		}
	}
	dev := devices[0]
	if got := *dev.Attributes[AttrResourceName].StringValue; got != "petasus.io/mgmt" {
		t.Errorf("resourceName attribute = %q, want petasus.io/mgmt", got)
	}
	if got := *dev.Attributes[AttrDomain+"/parent"].StringValue; got != "lo" {
		t.Errorf("parent attribute = %q, want lo", got)
	}
	spec, ok := specs["mgmt-0"]
	if !ok || spec.Parent != "lo" || spec.Pool != "mgmt" {
		t.Errorf("unexpected spec for mgmt-0: %+v (ok=%v)", spec, ok)
	}
}
