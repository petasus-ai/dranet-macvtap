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
	"crypto/rand"
	"fmt"
	"net"
	"path"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"k8s.io/utils/set"
	"sigs.k8s.io/dranet/internal/nlwrap"
	userns "sigs.k8s.io/dranet/internal/testutils"
	"sigs.k8s.io/dranet/pkg/apis"
	"sigs.k8s.io/dranet/pkg/macvtap"
)

// filterShape is the part of a tc filter the VLAN policy decides.
type filterShape struct {
	parent  uint32
	prio    uint16
	proto   uint16
	kind    string
	vlanID  uint16
	actions string
}

func shapeOf(f netlink.Filter) filterShape {
	a := f.Attrs()
	s := filterShape{parent: a.Parent, prio: a.Priority, proto: a.Protocol, kind: f.Type()}
	var actions []netlink.Action
	switch f := f.(type) {
	case *netlink.Flower:
		s.vlanID = f.VlanId
		actions = f.Actions
	case *netlink.MatchAll:
		actions = f.Actions
	}
	var names []string
	for _, action := range actions {
		switch action := action.(type) {
		case *netlink.GenericAction:
			names = append(names, action.Attrs().Action.String())
		case *netlink.VlanAction:
			names = append(names, fmt.Sprintf("vlan-%d-%d", action.Action, action.VlanID))
		default:
			names = append(names, action.Type())
		}
	}
	s.actions = strings.Join(names, ",")
	return s
}

func TestMacvtapVlanFilters(t *testing.T) {
	in, eg := uint32(netlink.HANDLE_MIN_INGRESS), uint32(netlink.HANDLE_MIN_EGRESS)
	drop := netlink.TcAct(netlink.TC_ACT_SHOT).String()
	for _, tc := range []struct {
		vid  int
		want []filterShape
	}{
		{0, []filterShape{
			{in, 1, unix.ETH_P_8021Q, "matchall", 0, drop},
			{in, 2, unix.ETH_P_8021AD, "matchall", 0, drop},
			{eg, 1, unix.ETH_P_8021Q, "matchall", 0, drop},
			{eg, 2, unix.ETH_P_8021AD, "matchall", 0, drop},
		}},
		{1001, []filterShape{
			{in, 1, unix.ETH_P_8021Q, "flower", 1001, "vlan-1-0"},
			{in, 9, unix.ETH_P_ALL, "matchall", 0, drop},
			{eg, 1, unix.ETH_P_8021Q, "matchall", 0, drop},
			{eg, 2, unix.ETH_P_8021AD, "matchall", 0, drop},
			{eg, 9, unix.ETH_P_ALL, "matchall", 0, "vlan-2-1001"},
		}},
	} {
		filters := macvtapVlanFilters(7, tc.vid)
		if len(filters) != len(tc.want) {
			t.Fatalf("vlan %d: %d filters, want %d", tc.vid, len(filters), len(tc.want))
		}
		for i, f := range filters {
			if got := shapeOf(f); got != tc.want[i] {
				t.Errorf("vlan %d filter %d = %+v, want %+v", tc.vid, i, got, tc.want[i])
			}
			if f.Attrs().LinkIndex != 7 {
				t.Errorf("vlan %d filter %d on link %d, want 7", tc.vid, i, f.Attrs().LinkIndex)
			}
			if fl, ok := f.(*netlink.Flower); ok && fl.EthType != unix.ETH_P_8021Q {
				t.Errorf("vlan %d: flower EthType %#x; without a VLAN EthType the kernel ignores the VLAN key", tc.vid, fl.EthType)
			}
		}
	}
}

func TestPrepareRefusesAVlanTheParentCannotCarry(t *testing.T) {
	userns.Run(t, testPrepareRefusesAVlanTheParentCannotCarry, syscall.CLONE_NEWNET, syscall.CLONE_NEWNS)
}

func testPrepareRefusesAVlanTheParentCannotCarry(t *testing.T) {
	env := newIPVlanTestEnv(t, 1500)
	parent, err := nlwrap.LinkByName(env.parent)
	if err != nil {
		t.Fatal(err)
	}
	// The parent's own link for VLAN 100, and a VLAN link as a parent.
	upper := &netlink.Vlan{LinkAttrs: netlink.LinkAttrs{Name: "tupper100", ParentIndex: parent.Attrs().Index}, VlanId: 100}
	if err := netlink.LinkAdd(upper); err != nil {
		t.Fatalf("add VLAN link: %v", err)
	}
	// A bond parent whose port has its own link for VLAN 300.
	bond := netlink.NewLinkBond(netlink.LinkAttrs{Name: "tbond"})
	bond.Mode = netlink.BOND_MODE_ACTIVE_BACKUP
	if err := netlink.LinkAdd(bond); err != nil {
		t.Fatalf("add bond: %v", err)
	}
	if err := netlink.LinkAdd(&netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: "tport"}}); err != nil {
		t.Fatalf("add bond port: %v", err)
	}
	bondLink, err := nlwrap.LinkByName("tbond")
	if err != nil {
		t.Fatal(err)
	}
	port, err := nlwrap.LinkByName("tport")
	if err != nil {
		t.Fatal(err)
	}
	if err := netlink.LinkSetMasterByIndex(port, bondLink.Attrs().Index); err != nil {
		t.Fatalf("enslave bond port: %v", err)
	}
	portVlan := &netlink.Vlan{LinkAttrs: netlink.LinkAttrs{Name: "tport.300", ParentIndex: port.Attrs().Index}, VlanId: 300}
	if err := netlink.LinkAdd(portVlan); err != nil {
		t.Fatalf("add VLAN link on the bond port: %v", err)
	}
	np := &NetworkDriver{netdb: &fakeMacvtapInventory{fakeInventoryDB: newFakeInventoryDB(), specs: map[string]macvtap.Spec{
		"eth1":  {Parent: env.parent, Mode: netlink.MACVLAN_MODE_BRIDGE},
		"vlan1": {Parent: "tupper100", Mode: netlink.MACVLAN_MODE_BRIDGE},
		"bond1": {Parent: "tbond", Mode: netlink.MACVLAN_MODE_BRIDGE},
	}}}
	claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{UID: "claim-uid"}}
	for _, tc := range []struct {
		device string
		vlan   *int
		want   string
	}{
		{"eth1", ptr.To(0), "outside 1-4094"},
		{"eth1", ptr.To(4095), "outside 1-4094"},
		{"eth1", ptr.To(100), "tupper100"},
		{"vlan1", ptr.To(200), "twice"},
		{"bond1", ptr.To(300), "tport.300"},
		{"eth1", ptr.To(200), ""},
		{"eth1", nil, ""},
		{"vlan1", nil, ""},
		{"bond1", ptr.To(301), ""},
	} {
		cfg := DeviceConfig{NetworkInterfaceConfigInPod: apis.NetworkConfig{
			Interface:          apis.InterfaceConfig{Name: "net1"},
			CarryNetworkConfig: apis.CarryNetworkConfig{Macvtap: &apis.MacvtapConfig{Vlan: tc.vlan}},
		}}
		result := resourceapi.DeviceRequestAllocationResult{Device: tc.device, Request: "nic"}
		hostIfName, err := np.prepareCarryNetdev(claim, result, &cfg)
		label := fmt.Sprintf("%s vlan %v", tc.device, ptr.Deref(tc.vlan, -1))
		if tc.want == "" {
			if err != nil {
				t.Errorf("%s: %v", label, err)
			} else if err := deleteHostLink(hostIfName); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want a refusal naming %q", label, err, tc.want)
		}
		if cfg.MacvtapHostIfName != "" {
			t.Errorf("%s: a macvtap child was recorded for a refused claim", label)
		}
	}
}

func TestRunPodSandboxAppliesTheVlanPolicy(t *testing.T) {
	userns.Run(t, testRunPodSandboxAppliesTheVlanPolicy, syscall.CLONE_NEWNET, syscall.CLONE_NEWNS)
}

// Every sandbox gets the policy anew: the move into the pod namespace drops
// the child's qdiscs, and a later sandbox gets a new child.
func testRunPodSandboxAppliesTheVlanPolicy(t *testing.T) {
	for name, vlan := range map[string]*int{"vlan": ptr.To(1001), "untagged": nil} {
		t.Run(name, func(t *testing.T) {
			env := newIPVlanTestEnv(t, 1500)
			claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "claim1", UID: "claim1-uid"}}
			np := &NetworkDriver{
				driverName:     "macvtap.petasus.io",
				nodeName:       "node1",
				kubeClient:     fake.NewClientset(claim),
				podConfigStore: mustNewPodConfigStore(),
				netdb: &fakeMacvtapInventory{
					fakeInventoryDB: newFakeInventoryDB(),
					specs:           map[string]macvtap.Spec{"eth1": {Parent: env.parent, Mode: netlink.MACVLAN_MODE_BRIDGE}},
				},
				eventRecorder: record.NewFakeRecorder(100),
			}
			result := resourceapi.DeviceRequestAllocationResult{Device: "eth1", Request: "nic"}
			cfg := DeviceConfig{
				Claim: types.NamespacedName{Namespace: "ns", Name: "claim1"},
				NetworkInterfaceConfigInPod: apis.NetworkConfig{
					Interface:          apis.InterfaceConfig{Name: "net1"},
					CarryNetworkConfig: apis.CarryNetworkConfig{Macvtap: &apis.MacvtapConfig{Vlan: vlan}},
				},
			}
			hostIfName, err := np.prepareCarryNetdev(claim, result, &cfg)
			if err != nil {
				t.Fatalf("prepareCarryNetdev: %v", err)
			}
			cfg.NetworkInterfaceConfigInHost.Interface.Name = hostIfName
			podUID := types.UID("pod-a")
			if err := np.podConfigStore.SetDeviceConfig(podUID, np.deviceStoreKey(claim, result), cfg); err != nil {
				t.Fatalf("SetDeviceConfig: %v", err)
			}
			pod := &api.PodSandbox{
				Uid:       string(podUID),
				Name:      "virt-launcher",
				Namespace: "ns",
				Linux:     &api.LinuxPodSandbox{Namespaces: []*api.LinuxNamespace{{Type: "network", Path: env.nsPath}}},
			}
			vid := ptr.Deref(vlan, 0)
			for sandbox := 1; sandbox <= 2; sandbox++ {
				if err := np.RunPodSandbox(context.Background(), pod); err != nil {
					t.Fatalf("sandbox %d: RunPodSandbox: %v", sandbox, err)
				}
				assertVlanPolicy(t, env.testNS, "net1", vid)
				if err := np.StopPodSandbox(context.Background(), pod); err != nil {
					t.Fatalf("sandbox %d: StopPodSandbox: %v", sandbox, err)
				}
			}
		})
	}
}

// assertVlanPolicy checks the child's clsact filters and, for a VLAN, its
// down registration link, as the kernel reports them.
func assertVlanPolicy(t *testing.T, ns netns.NsHandle, ifName string, vid int) {
	t.Helper()
	nh, err := nlwrap.NewHandleAt(ns)
	if err != nil {
		t.Fatal(err)
	}
	defer nh.Close()
	link, err := nh.LinkByName(ifName)
	if err != nil {
		t.Fatalf("child %s: %v", ifName, err)
	}
	want := map[uint32][]filterShape{}
	for _, f := range macvtapVlanFilters(link.Attrs().Index, vid) {
		s := shapeOf(f)
		s.actions = ""
		want[s.parent] = append(want[s.parent], s)
	}
	for parent, wantShapes := range want {
		filters, err := nh.FilterList(link, parent)
		if err != nil {
			t.Fatalf("FilterList: %v", err)
		}
		var got []filterShape
		for _, f := range filters {
			s := shapeOf(f)
			s.actions = ""
			got = append(got, s)
		}
		if fmt.Sprint(got) != fmt.Sprint(wantShapes) {
			t.Errorf("filters on %#x = %+v, want %+v", parent, got, wantShapes)
		}
	}
	links, err := nh.LinkList()
	if err != nil {
		t.Fatal(err)
	}
	var vlanLinks []*netlink.Vlan
	for _, l := range links {
		if v, ok := l.(*netlink.Vlan); ok && v.ParentIndex == link.Attrs().Index {
			vlanLinks = append(vlanLinks, v)
		}
	}
	if vid == 0 {
		if len(vlanLinks) != 0 {
			t.Errorf("untagged child carries VLAN links %v", vlanLinks)
		}
		return
	}
	if len(vlanLinks) != 1 || vlanLinks[0].VlanId != vid || vlanLinks[0].Name != macvtapVlanLinkName(link.Attrs().Index) {
		t.Fatalf("VLAN links on the child = %+v, want one %s with VLAN %d", vlanLinks, macvtapVlanLinkName(link.Attrs().Index), vid)
	}
	if vlanLinks[0].Flags&net.FlagUp != 0 {
		t.Errorf("registration link %s is up", vlanLinks[0].Name)
	}
}

func TestMacvtapVlanPolicyDataPath(t *testing.T) {
	userns.Run(t, testMacvtapVlanPolicyDataPath, syscall.CLONE_NEWNET, syscall.CLONE_NEWNS)
}

// vlanTestNS is a named network namespace of the data-path test.
type vlanTestNS struct {
	handle netns.NsHandle
	path   string
}

func newVlanTestNS(t *testing.T) vlanTestNS {
	t.Helper()
	origns, err := netns.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer origns.Close()
	rnd := make([]byte, 4)
	if _, err := rand.Read(rnd); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("vt%x", rnd)
	runtime.LockOSThread()
	handle, err := netns.NewNamed(name)
	setErr := netns.Set(origns)
	runtime.UnlockOSThread()
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if setErr != nil {
		t.Fatalf("switch back to the test namespace: %v", setErr)
	}
	t.Cleanup(func() {
		handle.Close()
		_ = netns.DeleteNamed(name)
	})
	ns := vlanTestNS{handle: handle, path: path.Join("/run/netns", name)}
	ns.run(t, "lo up", func(nh nlwrap.Handle) error {
		lo, err := nh.LinkByName("lo")
		if err != nil {
			return err
		}
		return nh.LinkSetUp(lo)
	})
	return ns
}

func (ns vlanTestNS) run(t *testing.T, what string, fn func(nh nlwrap.Handle) error) {
	t.Helper()
	nh, err := nlwrap.NewHandleAt(ns.handle)
	if err != nil {
		t.Fatal(err)
	}
	defer nh.Close()
	if err := fn(nh); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// addAddr brings a link of the namespace up with an address on it.
func (ns vlanTestNS) addAddr(t *testing.T, ifName, cidr string) {
	t.Helper()
	ns.run(t, ifName+" "+cidr, func(nh nlwrap.Handle) error {
		link, err := nh.LinkByName(ifName)
		if err != nil {
			return err
		}
		addr, err := netlink.ParseAddr(cidr)
		if err != nil {
			return err
		}
		if err := nh.AddrAdd(link, addr); err != nil {
			return err
		}
		return nh.LinkSetUp(link)
	})
}

// addVlan adds a VLAN link on a link of the namespace, the way a guest tags
// its own frames.
func (ns vlanTestNS) addVlan(t *testing.T, ifName, name string, vid int, cidr string) {
	t.Helper()
	ns.run(t, name, func(nh nlwrap.Handle) error {
		link, err := nh.LinkByName(ifName)
		if err != nil {
			return err
		}
		return nh.LinkAdd(&netlink.Vlan{LinkAttrs: netlink.LinkAttrs{Name: name, ParentIndex: link.Attrs().Index}, VlanId: vid})
	})
	ns.addAddr(t, name, cidr)
}

// in runs fn with the thread in the namespace: sockets fn opens stay there.
func (ns vlanTestNS) in(t *testing.T, fn func()) {
	t.Helper()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	origns, err := netns.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer origns.Close()
	if err := netns.Set(ns.handle); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := netns.Set(origns); err != nil {
			t.Fatal(err)
		}
	}()
	fn()
}

// listen accepts and closes TCP connections on port 5201 of the namespace.
func (ns vlanTestNS) listen(t *testing.T) {
	t.Helper()
	var ln net.Listener
	ns.in(t, func() {
		var err error
		if ln, err = net.Listen("tcp4", "0.0.0.0:5201"); err != nil {
			t.Fatal(err)
		}
	})
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
}

// reaches reports whether a TCP connection from the namespace to addr opens.
func (ns vlanTestNS) reaches(t *testing.T, addr string) bool {
	t.Helper()
	ok := false
	ns.in(t, func() {
		conn, err := net.DialTimeout("tcp4", net.JoinHostPort(addr, "5201"), time.Second)
		if err == nil {
			conn.Close()
			ok = true
		}
	})
	return ok
}

// A trunk stands in for the parent: one end in the test namespace carries the
// macvlan children (the macvtap data path without the tap), the other is the
// switch side with VLAN 230 and 240 links and the untagged native VLAN.
func testMacvtapVlanPolicyDataPath(t *testing.T) {
	sw := newVlanTestNS(t)
	trunk := &netlink.Veth{LinkAttrs: netlink.LinkAttrs{Name: "vtrunk"}, PeerName: "vswitch"}
	if err := netlink.LinkAdd(trunk); err != nil {
		t.Fatalf("add veth: %v", err)
	}
	host, err := nlwrap.LinkByName("vtrunk")
	if err != nil {
		t.Fatal(err)
	}
	if err := netlink.LinkSetUp(host); err != nil {
		t.Fatal(err)
	}
	peer, err := nlwrap.LinkByName("vswitch")
	if err != nil {
		t.Fatal(err)
	}
	if err := netlink.LinkSetNsFd(peer, int(sw.handle)); err != nil {
		t.Fatal(err)
	}
	sw.addAddr(t, "vswitch", "10.169.0.1/24")
	sw.addVlan(t, "vswitch", "vswitch.230", 230, "10.167.103.1/24")
	sw.addVlan(t, "vswitch", "vswitch.240", 240, "10.168.0.1/24")
	sw.listen(t)

	child := func(name string, vid int, cidr string) vlanTestNS {
		ns := newVlanTestNS(t)
		mv := &netlink.Macvlan{LinkAttrs: netlink.LinkAttrs{Name: name, ParentIndex: host.Attrs().Index}, Mode: netlink.MACVLAN_MODE_BRIDGE}
		if err := netlink.LinkAdd(mv); err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
		link, err := nlwrap.LinkByName(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := netlink.LinkSetNsFd(link, int(ns.handle)); err != nil {
			t.Fatal(err)
		}
		ns.addAddr(t, name, cidr)
		if err := applyMacvtapVlanPolicy(ns.path, name, vid); err != nil {
			t.Fatalf("policy on %s: %v", name, err)
		}
		return ns
	}
	vmA := child("mvA", 230, "10.167.103.10/24")
	vmB := child("mvB", 240, "10.168.0.10/24")
	vmC := child("mvC", 230, "10.167.103.30/24")
	vmD := child("mvD", 0, "10.169.0.20/24")
	vmC.listen(t)

	check := func(from vlanTestNS, to string, want bool, label string) {
		t.Helper()
		if got := from.reaches(t, to); got != want {
			t.Errorf("%s: reached = %v, want %v", label, got, want)
		}
	}
	check(vmA, "10.167.103.1", true, "VLAN 230 child to the VLAN 230 gateway")
	check(vmB, "10.168.0.1", true, "VLAN 240 child to the VLAN 240 gateway")
	check(vmA, "10.167.103.30", true, "VLAN 230 child to a VLAN 230 child on the same parent")
	check(vmD, "10.169.0.1", true, "untagged child to the untagged gateway")

	vmB.addAddr(t, "mvB", "10.167.103.20/24")
	check(vmB, "10.167.103.1", false, "VLAN 240 child to the VLAN 230 gateway")
	check(vmB, "10.167.103.30", false, "VLAN 240 child to a VLAN 230 child on the same parent")
	vmA.addAddr(t, "mvA", "10.169.0.10/24")
	check(vmA, "10.169.0.1", false, "VLAN 230 child to the untagged native VLAN")
	vmA.addVlan(t, "mvA", "mvA.240", 240, "10.168.0.50/24")
	check(vmA, "10.168.0.1", false, "VLAN 230 child tagging 240 itself")
	vmD.addVlan(t, "mvD", "mvD.230", 230, "10.167.103.40/24")
	check(vmD, "10.167.103.1", false, "untagged child tagging 230 itself")
}

func TestMacvtapVlanRegistrationKeepsForeignLinks(t *testing.T) {
	userns.Run(t, testMacvtapVlanRegistrationKeepsForeignLinks, syscall.CLONE_NEWNET, syscall.CLONE_NEWNS)
}

// The registration link is found by its VLAN on the child, never by name: a
// link that only shares the name is refused, not replaced.
func testMacvtapVlanRegistrationKeepsForeignLinks(t *testing.T) {
	env := newIPVlanTestEnv(t, 1500)
	spec := macvtap.Spec{Parent: env.parent, Mode: netlink.MACVLAN_MODE_BRIDGE}
	if err := ensureMacvtapDevice("mvtclaim", spec); err != nil {
		t.Fatal(err)
	}
	child, err := nlwrap.LinkByName("mvtclaim")
	if err != nil {
		t.Fatal(err)
	}
	if err := netlink.LinkSetNsFd(child, int(env.testNS)); err != nil {
		t.Fatal(err)
	}
	var index int
	ns := vlanTestNS{handle: env.testNS, path: env.nsPath}
	ns.run(t, "foreign link", func(nh nlwrap.Handle) error {
		link, err := nh.LinkByName("mvtclaim")
		if err != nil {
			return err
		}
		index = link.Attrs().Index
		return nh.LinkAdd(&netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: macvtapVlanLinkName(index)}})
	})
	if err := applyMacvtapVlanPolicy(env.nsPath, "mvtclaim", 1001); err == nil || !strings.Contains(err.Error(), "taken") {
		t.Fatalf("policy with the registration name taken: err = %v, want a refusal", err)
	}
	ns.run(t, "rename foreign link", func(nh nlwrap.Handle) error {
		foreign, err := nh.LinkByName(macvtapVlanLinkName(index))
		if err != nil {
			return fmt.Errorf("the foreign link is gone: %w", err)
		}
		return nh.LinkSetName(foreign, "foreign0")
	})
	for i := 0; i < 2; i++ {
		if err := applyMacvtapVlanPolicy(env.nsPath, "mvtclaim", 1001); err != nil {
			t.Fatalf("policy %d: %v", i+1, err)
		}
	}
	assertVlanPolicy(t, env.testNS, "mvtclaim", 1001)
}

func TestAttachFailsWhenTheVlanPolicyFails(t *testing.T) {
	userns.Run(t, testAttachFailsWhenTheVlanPolicyFails, syscall.CLONE_NEWNET, syscall.CLONE_NEWNS)
}

// The policy goes in before the child comes up in the pod: a policy the kernel
// refuses fails the attach, and the child goes back to the host.
func testAttachFailsWhenTheVlanPolicyFails(t *testing.T) {
	env := newIPVlanTestEnv(t, 1500)
	spec := macvtap.Spec{Parent: env.parent, Mode: netlink.MACVLAN_MODE_BRIDGE}
	const hostIfName = "mvt0000000000aa"
	if err := ensureMacvtapDevice(hostIfName, spec); err != nil {
		t.Fatal(err)
	}
	carryNetdevPolicies.put(carryPolicyKey(env.nsPath, "net1"), carryPolicy{hostIfName: hostIfName, vid: 5000})
	if _, err := nsAttachNetdev(hostIfName, env.nsPath, apis.InterfaceConfig{Name: "net1"}); err == nil || !strings.Contains(err.Error(), "filter") {
		t.Fatalf("attach with a refused policy: err = %v, want a filter error", err)
	}
	back, err := nlwrap.LinkByName(hostIfName)
	if err != nil {
		t.Fatalf("the child did not return to the host: %v", err)
	}
	if back.Attrs().Flags&net.FlagUp != 0 {
		t.Error("the child returned to the host up, where it has no VLAN policy")
	}
	ns := vlanTestNS{handle: env.testNS, path: env.nsPath}
	ns.run(t, "pod links", func(nh nlwrap.Handle) error {
		if _, err := nh.LinkByName("net1"); err == nil {
			return fmt.Errorf("the child stayed in the pod")
		}
		return nil
	})
	if err := configureCarryNetdevInNS(env.nsPath, "lo", DeviceConfig{CarryDeviceConfig: CarryDeviceConfig{MacvtapHostIfName: "mvtclaim"}}); err == nil {
		t.Fatal("a macvtap child without its policy passed configuration")
	}
}

func TestRunPodSandboxRetriesAVlanChildAfterAFailedAttach(t *testing.T) {
	userns.Run(t, testRunPodSandboxRetriesAVlanChildAfterAFailedAttach, syscall.CLONE_NEWNET, syscall.CLONE_NEWNS)
}

// A configuration step that fails after the policy returns the child to the
// host, its registration link left in the pod namespace; the retry in the same
// namespace still gets the child and exactly one registration.
func testRunPodSandboxRetriesAVlanChildAfterAFailedAttach(t *testing.T) {
	env := newIPVlanTestEnv(t, 1500)
	claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "claim1", UID: "claim1-uid"}}
	np := &NetworkDriver{
		driverName:     "macvtap.petasus.io",
		nodeName:       "node1",
		kubeClient:     fake.NewClientset(claim),
		podConfigStore: mustNewPodConfigStore(),
		netdb: &fakeMacvtapInventory{
			fakeInventoryDB: newFakeInventoryDB(),
			specs:           map[string]macvtap.Spec{"eth1": {Parent: env.parent, Mode: netlink.MACVLAN_MODE_BRIDGE}},
		},
		eventRecorder: record.NewFakeRecorder(100),
	}
	result := resourceapi.DeviceRequestAllocationResult{Device: "eth1", Request: "nic"}
	cfg := DeviceConfig{
		Claim: types.NamespacedName{Namespace: "ns", Name: "claim1"},
		NetworkInterfaceConfigInPod: apis.NetworkConfig{
			Interface:          apis.InterfaceConfig{Name: "net1"},
			CarryNetworkConfig: apis.CarryNetworkConfig{Macvtap: &apis.MacvtapConfig{Vlan: ptr.To(1001)}},
		},
	}
	hostIfName, err := np.prepareCarryNetdev(claim, result, &cfg)
	if err != nil {
		t.Fatalf("prepareCarryNetdev: %v", err)
	}
	cfg.NetworkInterfaceConfigInHost.Interface.Name = hostIfName
	podUID := types.UID("pod-a")
	storeKey := np.deviceStoreKey(claim, result)
	pod := &api.PodSandbox{
		Uid:       string(podUID),
		Name:      "virt-launcher",
		Namespace: "ns",
		Linux:     &api.LinuxPodSandbox{Namespaces: []*api.LinuxNamespace{{Type: "network", Path: env.nsPath}}},
	}

	// No address carries the gateway, so the route fails after the policy.
	failing := cfg
	failing.NetworkInterfaceConfigInPod.Routes = []apis.RouteConfig{{Destination: "10.1.0.0/16", Gateway: "192.0.2.1"}}
	if err := np.podConfigStore.SetDeviceConfig(podUID, storeKey, failing); err != nil {
		t.Fatal(err)
	}
	if err := np.RunPodSandbox(context.Background(), pod); err == nil {
		t.Fatal("RunPodSandbox with an unreachable route succeeded")
	}
	if err := np.podConfigStore.SetDeviceConfig(podUID, storeKey, cfg); err != nil {
		t.Fatal(err)
	}
	if err := np.RunPodSandbox(context.Background(), pod); err != nil {
		t.Fatalf("retry: RunPodSandbox: %v", err)
	}
	assertVlanPolicy(t, env.testNS, "net1", 1001)
	ns := vlanTestNS{handle: env.testNS, path: env.nsPath}
	ns.run(t, "registration links", func(nh nlwrap.Handle) error {
		links, err := nh.LinkList()
		if err != nil {
			return err
		}
		var vlans []string
		for _, l := range links {
			if l.Type() == "vlan" {
				vlans = append(vlans, l.Attrs().Name)
			}
		}
		if len(vlans) != 1 {
			return fmt.Errorf("VLAN links in the pod after the retry: %v, want only the new child's", vlans)
		}
		return nil
	})
}

func TestCarryPoliciesHoldOneEntryPerChild(t *testing.T) {
	p := &carryPolicies{byKey: map[string]carryPolicy{}, keyOf: map[string]string{}}
	// Sandboxes whose attach never took their entry.
	p.put(carryPolicyKey("/run/netns/a", "net1"), carryPolicy{hostIfName: "mvt000000000001", vid: 1001})
	p.put(carryPolicyKey("/run/netns/b", "net1"), carryPolicy{hostIfName: "mvt000000000001", vid: 1001})
	p.put(carryPolicyKey("/run/netns/b", "net2"), carryPolicy{hostIfName: "mvt000000000002"})
	if got := p.len(); got != 2 {
		t.Fatalf("%d entries for two children, want 2", got)
	}
	if _, ok := p.take(carryPolicyKey("/run/netns/a", "net1")); ok {
		t.Error("an older sandbox's entry survived the next one")
	}
	if policy, ok := p.take(carryPolicyKey("/run/netns/b", "net1")); !ok || policy.vid != 1001 {
		t.Errorf("take = %+v %v, want VLAN 1001", policy, ok)
	}
	p.prune(set.New[string]())
	if got := p.len(); got != 0 {
		t.Errorf("%d entries after the children went unused, want 0", got)
	}
}
