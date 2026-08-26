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
	"hash/fnv"
	"net"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/vishvananda/netlink"
	"sigs.k8s.io/dranet/pkg/apis"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

const (
	// AttrDomain is the attribute domain of this driver's device attributes.
	AttrDomain = "macvtap.petasus.io"

	// SlotsCapacity is the consumable capacity each parent device publishes;
	// every allocation consumes exactly one slot (request policy default and
	// only valid value).
	SlotsCapacity = AttrDomain + "/slots"

	// SlotsPerParent bounds how many macvtap children one parent carries.
	SlotsPerParent = 999

	defaultRescanInterval = 30 * time.Second
)

// Spec describes how to materialize a macvtap child for one published device.
type Spec struct {
	Parent string
	Mode   netlink.MacvlanMode
}

// NetlinkOps abstracts the netlink queries discovery needs (for testing).
type NetlinkOps interface {
	LinkList() ([]netlink.Link, error)
}

type defaultNetlink struct{}

func (defaultNetlink) LinkList() ([]netlink.Link, error) { return netlink.LinkList() }

// Inventory publishes every eligible host parent interface as ONE shared DRA
// device (allowMultipleAllocations with a slots capacity); selection policy
// lives in DeviceClass CEL on the management side, and the driver creates one
// macvtap child per allocated claim at prepare time. The node's links are
// rescanned periodically, so parent add/remove propagates without a restart.
type Inventory struct {
	nodeName       string
	rescanInterval time.Duration
	netlink        NetlinkOps

	mu        sync.RWMutex
	devices   map[string]resourceapi.Device
	specs     map[string]Spec
	published []resourceapi.Device

	notifications chan []resourceapi.Device
	rescan        chan struct{}
}

// Option customizes the Inventory.
type Option func(*Inventory)

// WithRescanInterval overrides how often the node's links are rescanned.
func WithRescanInterval(d time.Duration) Option {
	return func(inv *Inventory) {
		inv.rescanInterval = d
	}
}

// WithNetlink overrides the netlink implementation (for testing).
func WithNetlink(n NetlinkOps) Option {
	return func(inv *Inventory) {
		inv.netlink = n
	}
}

// New creates an Inventory that discovers and advertises the node's macvtap
// parent interfaces.
func New(nodeName string, opts ...Option) *Inventory {
	inv := &Inventory{
		nodeName:       nodeName,
		rescanInterval: defaultRescanInterval,
		netlink:        defaultNetlink{},
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

// Run keeps the published device list in sync with the node's link state
// until the context is cancelled.
func (inv *Inventory) Run(ctx context.Context) error {
	inv.sync(true)
	ticker := time.NewTicker(inv.rescanInterval)
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

// sync rebuilds the device list and notifies the publisher when the list
// changed (or unconditionally on the first pass, so an empty device set
// still results in an initial publish).
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

// eligibleParentTypes are the netlink link kinds a macvtap child can sit on:
// physical NICs ("device"), bonds and VLAN sub-interfaces. Everything virtual
// (veth, bridge, macvlan/macvtap children, tunnels) is excluded.
var eligibleParentTypes = map[string]bool{
	"device": true,
	"bond":   true,
	"vlan":   true,
}

func (inv *Inventory) buildDevices() ([]resourceapi.Device, map[string]Spec) {
	devices := []resourceapi.Device{}
	specs := map[string]Spec{}

	links, err := inv.netlink.LinkList()
	if err != nil {
		klog.Errorf("macvtap inventory: failed to list links, advertising nothing: %v", err)
		return devices, specs
	}

	for _, link := range links {
		attrs := link.Attrs()
		if attrs == nil || attrs.Name == "" {
			continue
		}
		if attrs.Flags&net.FlagLoopback != 0 {
			continue
		}
		// Macvtap children need an Ethernet parent (no InfiniBand, no tun).
		if attrs.EncapType != "ether" {
			continue
		}
		if !eligibleParentTypes[link.Type()] {
			continue
		}
		name := deviceNameForIfName(attrs.Name)
		if _, dup := specs[name]; dup {
			klog.Warningf("macvtap inventory: device name %q for link %q collides with another link, skipping", name, attrs.Name)
			continue
		}
		device := resourceapi.Device{
			Name:                     name,
			AllowMultipleAllocations: ptr.To(true),
			Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				AttrDomain + "/ifName":     {StringValue: ptr.To(attrs.Name)},
				AttrDomain + "/mtu":        {IntValue: ptr.To(int64(attrs.MTU))},
				AttrDomain + "/state":      {StringValue: ptr.To(strings.ToLower(attrs.OperState.String()))},
				AttrDomain + "/linkKind":   {StringValue: ptr.To(link.Type())},
				AttrDomain + "/deviceType": {StringValue: ptr.To("macvtap-parent")},
			},
			// One slot per claim: the default applies when a request carries
			// no capacity entry, and the single valid value rejects anything
			// else.
			Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
				SlotsCapacity: {
					Value: *resource.NewQuantity(SlotsPerParent, resource.DecimalSI),
					RequestPolicy: &resourceapi.CapacityRequestPolicy{
						Default:     resource.NewQuantity(1, resource.DecimalSI),
						ValidValues: []resource.Quantity{*resource.NewQuantity(1, resource.DecimalSI)},
					},
				},
			},
		}
		devices = append(devices, device)
		specs[name] = Spec{Parent: attrs.Name, Mode: netlink.MACVLAN_MODE_BRIDGE}
	}
	sort.Slice(devices, func(i, j int) bool { return devices[i].Name < devices[j].Name })
	return devices, specs
}

// nonDNSLabelChar matches everything a DNS-1123 label cannot carry.
var nonDNSLabelChar = regexp.MustCompile(`[^a-z0-9-]`)

// deviceNameForIfName turns a host interface name into a DRA device name
// (DNS-1123 label), e.g. "eth0.100" -> "eth0-100".
func deviceNameForIfName(ifName string) string {
	return nonDNSLabelChar.ReplaceAllString(strings.ToLower(ifName), "-")
}

// HostIfName returns the host-side link name of the macvtap child created for
// one allocation. A parent device is shared (allowMultipleAllocations), so
// the name is derived from the claim and request, not the device alone.
// Hashed to a fixed 15 chars (IFNAMSIZ-1).
func HostIfName(deviceName string, claimUID types.UID, requestName string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(deviceName))
	_, _ = h.Write([]byte("/"))
	_, _ = h.Write([]byte(claimUID))
	_, _ = h.Write([]byte("/"))
	_, _ = h.Write([]byte(requestName))
	return fmt.Sprintf("mvt%012x", h.Sum64()&0xffffffffffff)
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

// GetNetInterfaceName never resolves: the macvtap child of a shared parent
// device is per-claim and only exists once the prepare path created it.
func (inv *Inventory) GetNetInterfaceName(deviceName string) (string, error) {
	return "", fmt.Errorf("device %s is a shared macvtap parent; the child link is per-claim", deviceName)
}

// IsIBOnlyDevice is always false: every device is an Ethernet parent.
func (inv *Inventory) IsIBOnlyDevice(_ string) bool {
	return false
}

// GetRDMADeviceName never resolves: macvtap parents carry no RDMA device.
func (inv *Inventory) GetRDMADeviceName(deviceName string) (string, error) {
	return "", fmt.Errorf("device %s has no RDMA device", deviceName)
}

// GetDeviceConfig returns no provider-side configuration.
func (inv *Inventory) GetDeviceConfig(_ string) (*apis.NetworkConfig, bool) {
	return nil, false
}

// RequestRescan triggers an immediate link rescan and republish check.
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
