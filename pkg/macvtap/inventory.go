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
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/vishvananda/netlink"
	"sigs.k8s.io/dranet/pkg/apis"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

const (
	// AttrDomain is the attribute domain of this driver's device attributes.
	AttrDomain = "macvtap.petasus.io"

	// AttrResourceName carries the pool's logical resource name; the
	// management platform joins DeviceClasses and networks on it.
	AttrResourceName = "k8s.cni.cncf.io/resourceName"

	// DefaultResourceNamePrefix prefixes pool names into resource names.
	DefaultResourceNamePrefix = "petasus.io/"

	defaultReloadInterval = 30 * time.Second
)

// Spec describes how to materialize a macvtap child for one published device.
type Spec struct {
	Pool   string
	Parent string
	Mode   netlink.MacvlanMode
}

// Inventory publishes macvtap pool slots as DRA devices. Pools are defined in
// a config file (mounted from a ConfigMap) and re-read periodically, so pool
// changes propagate without a driver restart.
type Inventory struct {
	configPath     string
	nodeName       string
	reloadInterval time.Duration

	mu        sync.RWMutex
	devices   map[string]resourceapi.Device
	specs     map[string]Spec
	published []resourceapi.Device

	notifications chan []resourceapi.Device
	rescan        chan struct{}
}

// Option customizes the Inventory.
type Option func(*Inventory)

// WithReloadInterval overrides how often the config file is re-read.
func WithReloadInterval(d time.Duration) Option {
	return func(inv *Inventory) {
		inv.reloadInterval = d
	}
}

// New creates an Inventory that reads pool definitions from configPath and
// advertises the slots whose parent interface exists on this node.
func New(configPath, nodeName string, opts ...Option) *Inventory {
	inv := &Inventory{
		configPath:     configPath,
		nodeName:       nodeName,
		reloadInterval: defaultReloadInterval,
		devices:        map[string]resourceapi.Device{},
		specs:          map[string]Spec{},
		notifications:  make(chan []resourceapi.Device, 1),
		rescan:         make(chan struct{}, 1),
	}
	for _, o := range opts {
		o(inv)
	}
	return inv
}

// Run keeps the published device list in sync with the config file and the
// node's link state until the context is cancelled.
func (inv *Inventory) Run(ctx context.Context) error {
	inv.sync(true)
	ticker := time.NewTicker(inv.reloadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			inv.sync(false)
		case <-inv.rescan:
			inv.sync(false)
		}
	}
}

// sync reloads the config, rebuilds the device list and notifies the
// publisher when the list changed (or unconditionally on the first pass, so
// an empty pool set still results in an initial publish).
func (inv *Inventory) sync(force bool) {
	devices, specs := inv.buildDevices()

	inv.mu.Lock()
	changed := !reflect.DeepEqual(devices, inv.published)
	if changed || force {
		inv.published = devices
		inv.devices = map[string]resourceapi.Device{}
		for _, dev := range devices {
			inv.devices[dev.Name] = dev
		}
		inv.specs = specs
	}
	inv.mu.Unlock()

	if changed || force {
		// Coalesce: drop a stale pending notification before sending the new one.
		select {
		case <-inv.notifications:
		default:
		}
		inv.notifications <- devices
	}
}

func (inv *Inventory) buildDevices() ([]resourceapi.Device, map[string]Spec) {
	devices := []resourceapi.Device{}
	specs := map[string]Spec{}

	config, err := LoadConfig(inv.configPath)
	if err != nil {
		klog.Errorf("macvtap inventory: failed to load config %s, advertising nothing: %v", inv.configPath, err)
		return devices, specs
	}

	for i := range config.Pools {
		pool := &config.Pools[i]
		parent := pool.ParentForNode(inv.nodeName)
		if parent == "" {
			klog.V(4).Infof("macvtap inventory: pool %q has no parent for node %s, skipping", pool.Name, inv.nodeName)
			continue
		}
		if _, err := netlink.LinkByName(parent); err != nil {
			klog.Warningf("macvtap inventory: pool %q parent %q not found on node %s, skipping: %v", pool.Name, parent, inv.nodeName, err)
			continue
		}
		resourceName := pool.ResourceName
		if resourceName == "" {
			resourceName = DefaultResourceNamePrefix + pool.Name
		}
		for idx := 0; idx < pool.Capacity; idx++ {
			name := pool.DeviceName(idx)
			device := resourceapi.Device{
				Name: name,
				Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					AttrResourceName:           {StringValue: ptr.To(resourceName)},
					AttrDomain + "/pool":       {StringValue: ptr.To(pool.Name)},
					AttrDomain + "/parent":     {StringValue: ptr.To(parent)},
					AttrDomain + "/mode":       {StringValue: ptr.To(cmpOr(pool.Mode, "bridge"))},
					AttrDomain + "/index":      {IntValue: ptr.To(int64(idx))},
					AttrDomain + "/deviceType": {StringValue: ptr.To("macvtap")},
				},
			}
			if pool.MTU > 0 {
				device.Attributes[AttrDomain+"/mtu"] = resourceapi.DeviceAttribute{IntValue: ptr.To(int64(pool.MTU))}
			}
			devices = append(devices, device)
			specs[name] = Spec{Pool: pool.Name, Parent: parent, Mode: pool.MacvtapMode()}
		}
	}
	sort.Slice(devices, func(i, j int) bool { return devices[i].Name < devices[j].Name })
	return devices, specs
}

func cmpOr(v, fallback string) string {
	if v != "" {
		return v
	}
	return fallback
}

// GetResources returns the channel the publisher consumes device lists from.
func (inv *Inventory) GetResources(_ context.Context) <-chan []resourceapi.Device {
	return inv.notifications
}

// GetDevice returns the published device by name.
func (inv *Inventory) GetDevice(deviceName string) (resourceapi.Device, bool) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	dev, ok := inv.devices[deviceName]
	return dev, ok
}

// GetMacvtapSpec returns the creation spec for a published device.
func (inv *Inventory) GetMacvtapSpec(deviceName string) (Spec, bool) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	spec, ok := inv.specs[deviceName]
	return spec, ok
}

// GetNetInterfaceName returns the host-side link name of the macvtap child
// that PrepareResourceClaims creates for the device.
func (inv *Inventory) GetNetInterfaceName(deviceName string) (string, error) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	if _, ok := inv.devices[deviceName]; !ok {
		return "", fmt.Errorf("device %s not found", deviceName)
	}
	return HostIfName(deviceName), nil
}

// IsIBOnlyDevice is always false: every device is a macvtap slot.
func (inv *Inventory) IsIBOnlyDevice(_ string) bool {
	return false
}

// GetRDMADeviceName never resolves: macvtap slots carry no RDMA device.
func (inv *Inventory) GetRDMADeviceName(deviceName string) (string, error) {
	return "", fmt.Errorf("device %s has no RDMA device", deviceName)
}

// GetDeviceConfig returns no provider-side configuration.
func (inv *Inventory) GetDeviceConfig(_ string) (*apis.NetworkConfig, bool) {
	return nil, false
}

// RequestRescan triggers an immediate config reload and republish check.
func (inv *Inventory) RequestRescan() {
	select {
	case inv.rescan <- struct{}{}:
	default:
	}
}

// GetProfileConfig is not supported; profiles must not be requested.
func (inv *Inventory) GetProfileConfig(deviceName string, _ types.UID, _ *apis.NetworkConfig) (*apis.NetworkConfig, error) {
	return nil, fmt.Errorf("profiles are not supported (device %s)", deviceName)
}

// ReleaseProfileConfig is a no-op as profiles are not supported.
func (inv *Inventory) ReleaseProfileConfig(_ string, _ types.UID, _ *apis.NetworkConfig) error {
	return nil
}
