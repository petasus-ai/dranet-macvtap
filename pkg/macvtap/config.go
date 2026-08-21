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
	"errors"
	"fmt"
	"hash/fnv"
	"os"

	"github.com/vishvananda/netlink"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/yaml"
)

const (
	maxPoolCapacity = 999
)

// PoolConfig describes one pool of macvtap slots on a host parent interface.
type PoolConfig struct {
	// Name identifies the pool; devices are published as "<name>-<index>".
	// Must be a DNS-1123 label short enough that "mvt-<name>-<index>" fits
	// in an interface name (15 chars).
	Name string `json:"name"`

	// Parent is the default host interface the macvtap children are created on.
	Parent string `json:"parent,omitempty"`

	// ParentByNode overrides Parent for specific node names.
	ParentByNode map[string]string `json:"parentByNode,omitempty"`

	// Capacity is the number of slots (devices) advertised per node.
	Capacity int `json:"capacity"`

	// Mode is the macvtap mode: bridge (default), private, vepa or passthru.
	Mode string `json:"mode,omitempty"`

	// MTU is published as an informational attribute; the effective MTU is set
	// per claim through the interface config.
	MTU int32 `json:"mtu,omitempty"`

	// ResourceName is published as the "k8s.cni.cncf.io/resourceName"
	// attribute. Defaults to "petasus.io/<name>".
	ResourceName string `json:"resourceName,omitempty"`
}

// Config is the file format of the driver configuration.
type Config struct {
	Pools []PoolConfig `json:"pools"`
}

// ParentForNode resolves the parent interface for the given node.
func (p *PoolConfig) ParentForNode(nodeName string) string {
	if parent, ok := p.ParentByNode[nodeName]; ok {
		return parent
	}
	return p.Parent
}

// MacvtapMode maps the configured mode string to the netlink mode.
func (p *PoolConfig) MacvtapMode() netlink.MacvlanMode {
	switch p.Mode {
	case "", "bridge":
		return netlink.MACVLAN_MODE_BRIDGE
	case "private":
		return netlink.MACVLAN_MODE_PRIVATE
	case "vepa":
		return netlink.MACVLAN_MODE_VEPA
	case "passthru":
		return netlink.MACVLAN_MODE_PASSTHRU
	}
	return netlink.MACVLAN_MODE_BRIDGE
}

// DeviceName returns the published device name of one slot.
func (p *PoolConfig) DeviceName(index int) string {
	return fmt.Sprintf("%s-%d", p.Name, index)
}

// HostIfName returns the host-side link name for a published device. Hashed
// to a fixed 15 chars (IFNAMSIZ-1) so pool names — which come from
// user-chosen network names — carry no length constraint.
func HostIfName(deviceName string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(deviceName))
	return fmt.Sprintf("mvt%012x", h.Sum64()&0xffffffffffff)
}

// LoadConfig reads and validates the configuration file.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	config := &Config{}
	if err := yaml.UnmarshalStrict(raw, config); err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", path, err)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return config, nil
}

// Validate checks the whole configuration.
func (c *Config) Validate() error {
	var errs []error
	seen := map[string]bool{}
	for i := range c.Pools {
		pool := &c.Pools[i]
		if seen[pool.Name] {
			errs = append(errs, fmt.Errorf("pool %q: duplicate name", pool.Name))
		}
		seen[pool.Name] = true
		errs = append(errs, pool.validate()...)
	}
	return errors.Join(errs...)
}

func (p *PoolConfig) validate() []error {
	var errs []error
	if msgs := validation.IsDNS1123Label(p.Name); len(msgs) > 0 {
		errs = append(errs, fmt.Errorf("pool %q: name must be a DNS-1123 label: %v", p.Name, msgs))
	}
	if p.Capacity < 1 || p.Capacity > maxPoolCapacity {
		errs = append(errs, fmt.Errorf("pool %q: capacity must be in [1, %d], got %d", p.Name, maxPoolCapacity, p.Capacity))
	}
	if p.Parent == "" && len(p.ParentByNode) == 0 {
		errs = append(errs, fmt.Errorf("pool %q: parent (or parentByNode) is required", p.Name))
	}
	switch p.Mode {
	case "", "bridge", "private", "vepa", "passthru":
	default:
		errs = append(errs, fmt.Errorf("pool %q: unsupported mode %q", p.Name, p.Mode))
	}
	if p.MTU < 0 {
		errs = append(errs, fmt.Errorf("pool %q: negative mtu", p.Name))
	}
	return errs
}
