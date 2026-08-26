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
	"net"
	"testing"

	"github.com/vishvananda/netlink"
)

type fakeNetlink struct {
	links []netlink.Link
}

func (f *fakeNetlink) LinkList() ([]netlink.Link, error) { return f.links, nil }

func linkAttrs(name, encap string, flags net.Flags, mtu int) netlink.LinkAttrs {
	attrs := netlink.NewLinkAttrs()
	attrs.Name = name
	attrs.EncapType = encap
	attrs.Flags = flags
	attrs.MTU = mtu
	attrs.OperState = netlink.OperUp
	return attrs
}

func TestBuildDevices(t *testing.T) {
	nl := &fakeNetlink{links: []netlink.Link{
		&netlink.Device{LinkAttrs: linkAttrs("eno1", "ether", net.FlagUp, 9000)},
		&netlink.Bond{LinkAttrs: linkAttrs("bond0", "ether", net.FlagUp, 1500)},
		&netlink.Vlan{LinkAttrs: linkAttrs("bond0.100", "ether", net.FlagUp, 1500), VlanId: 100},
		// Excluded: virtual leaves, loopback, non-Ethernet encapsulation.
		&netlink.Veth{LinkAttrs: linkAttrs("veth0", "ether", net.FlagUp, 1500)},
		&netlink.Bridge{LinkAttrs: linkAttrs("br0", "ether", net.FlagUp, 1500)},
		&netlink.Device{LinkAttrs: linkAttrs("lo", "loopback", net.FlagUp|net.FlagLoopback, 65536)},
		&netlink.Device{LinkAttrs: linkAttrs("ibp12s0", "infiniband", net.FlagUp, 2044)},
	}}

	inv := New("node-1", WithNetlink(nl))
	devices, specs := inv.buildDevices()

	if len(devices) != 3 {
		names := []string{}
		for _, d := range devices {
			names = append(names, d.Name)
		}
		t.Fatalf("expected 3 devices, got %d: %v", len(devices), names)
	}

	byName := map[string]int{}
	for i, dev := range devices {
		byName[dev.Name] = i
	}

	idx, ok := byName["eno1"]
	if !ok {
		t.Fatal("eno1 not published")
	}
	dev := devices[idx]
	if dev.AllowMultipleAllocations == nil || !*dev.AllowMultipleAllocations {
		t.Fatal("parent device must allow multiple allocations")
	}
	cap, ok := dev.Capacity[SlotsCapacity]
	if !ok {
		t.Fatal("slots capacity missing")
	}
	if cap.Value.Value() != SlotsPerParent {
		t.Fatalf("unexpected slots capacity: %v", cap.Value)
	}
	if cap.RequestPolicy == nil || cap.RequestPolicy.Default.Value() != 1 {
		t.Fatal("slots request policy must default to 1")
	}
	if got := *dev.Attributes[AttrDomain+"/ifName"].StringValue; got != "eno1" {
		t.Fatalf("unexpected ifName: %q", got)
	}
	if got := *dev.Attributes[AttrDomain+"/mtu"].IntValue; got != 9000 {
		t.Fatalf("unexpected mtu: %d", got)
	}
	if got := *dev.Attributes[AttrDomain+"/state"].StringValue; got != "up" {
		t.Fatalf("unexpected state: %q", got)
	}

	// A VLAN sub-interface's dot is normalized in the device name; the spec
	// keeps the real ifName as the macvtap parent.
	idx, ok = byName["bond0-100"]
	if !ok {
		t.Fatal("bond0.100 not published as bond0-100")
	}
	if got := *devices[idx].Attributes[AttrDomain+"/ifName"].StringValue; got != "bond0.100" {
		t.Fatalf("unexpected vlan ifName: %q", got)
	}
	spec, ok := specs["bond0-100"]
	if !ok || spec.Parent != "bond0.100" || spec.Mode != netlink.MACVLAN_MODE_BRIDGE {
		t.Fatalf("unexpected spec: %#v", spec)
	}
}

func TestHostIfNamePerAllocation(t *testing.T) {
	a := HostIfName("eno1", "claim-a", "nic")
	b := HostIfName("eno1", "claim-b", "nic")
	if a == b {
		t.Fatal("host ifnames of two claims on one parent must differ")
	}
	if len(a) != 15 {
		t.Fatalf("host ifname %q must be 15 chars (IFNAMSIZ-1)", a)
	}
	if a != HostIfName("eno1", "claim-a", "nic") {
		t.Fatal("host ifname must be deterministic")
	}
}
