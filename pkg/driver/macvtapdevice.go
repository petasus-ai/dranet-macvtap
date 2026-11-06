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
	"errors"
	"fmt"
	"os"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/set"
	"sigs.k8s.io/dranet/internal/nlwrap"
	"sigs.k8s.io/dranet/pkg/apis"
	"sigs.k8s.io/dranet/pkg/macvtap"
)

const (
	// qemuUID/qemuGID own the tap character device inside the container so the
	// qemu process in KubeVirt's virt-launcher (uid/gid 107) can open it.
	qemuUID = 107
	qemuGID = 107

	// tapDevFileMode is the mode of the tap character device node created in
	// the container.
	tapDevFileMode = 0o660
)

// macvtapInventory is the extra contract a macvtap-pool inventory provides on
// top of the generic inventoryDB interface.
type macvtapInventory interface {
	GetMacvtapSpec(deviceName string) (macvtap.Spec, bool)
}

// macvtapSpecOf resolves the macvtap creation spec of a device, when the
// inventory serves macvtap parents and the device is one.
func macvtapSpecOf(db inventoryDB, deviceName string) (macvtap.Spec, bool) {
	mvdb, ok := db.(macvtapInventory)
	if !ok {
		return macvtap.Spec{}, false
	}
	return mvdb.GetMacvtapSpec(deviceName)
}

// macvtapStoreKey keys a shared macvtap parent's per-claim entry in the pod
// config store; the published device name alone would collide when one pod
// carries two claims on the same parent.
func macvtapStoreKey(deviceName string, claimUID types.UID, requestName string) string {
	return fmt.Sprintf("%s@%s@%s", deviceName, claimUID, requestName)
}

// ensureMacvtapDevice (re)creates the macvtap child link for one allocated
// slot in the host namespace. Recreate semantics keep the operation
// idempotent across kubelet retries and driver restarts.
func ensureMacvtapDevice(hostIfName string, spec macvtap.Spec) error {
	parent, err := nlwrap.LinkByName(spec.Parent)
	if err != nil {
		return fmt.Errorf("macvtap parent %q not found: %w", spec.Parent, err)
	}

	if err := deleteHostLink(hostIfName); err != nil {
		return fmt.Errorf("failed to delete stale macvtap link %q: %w", hostIfName, err)
	}

	link := &netlink.Macvtap{
		Macvlan: netlink.Macvlan{
			LinkAttrs: netlink.LinkAttrs{
				Name:        hostIfName,
				ParentIndex: parent.Attrs().Index,
				TxQLen:      parent.Attrs().TxQLen,
			},
			Mode: spec.Mode,
		},
	}
	if err := netlink.LinkAdd(link); err != nil {
		return fmt.Errorf("failed to create macvtap %q on parent %q: %w", hostIfName, spec.Parent, err)
	}
	return nil
}

// macvtapVlanOf is the claim's VLAN ID, 0 for an untagged-only child.
func macvtapVlanOf(conf apis.NetworkConfig) int {
	if conf.Macvtap == nil || conf.Macvtap.Vlan == nil {
		return 0
	}
	return *conf.Macvtap.Vlan
}

// checkMacvtapVlan refuses a claim VLAN the child cannot carry on its parent.
func checkMacvtapVlan(parentName string, conf *apis.MacvtapConfig) error {
	if conf == nil || conf.Vlan == nil {
		return nil
	}
	vid := *conf.Vlan
	if vid < 1 || vid > 4094 {
		return fmt.Errorf("vlan %d is outside 1-4094", vid)
	}
	parent, err := nlwrap.LinkByName(parentName)
	if err != nil {
		return fmt.Errorf("macvtap parent %q not found: %w", parentName, err)
	}
	if parent.Type() == "vlan" {
		return fmt.Errorf("vlan %d on parent %s, itself a VLAN link, would tag the frames twice", vid, parentName)
	}
	links, err := nlwrap.LinkList()
	if err != nil {
		return fmt.Errorf("failed to list links: %w", err)
	}
	// The kernel hands a VLAN's frames to a link of that VLAN on the parent,
	// or on a port of a bond parent, before the parent's children see them.
	lowers := map[int]string{parent.Attrs().Index: parentName}
	for _, link := range links {
		if link.Attrs().MasterIndex == parent.Attrs().Index {
			lowers[link.Attrs().Index] = link.Attrs().Name
		}
	}
	for _, link := range links {
		upper, ok := link.(*netlink.Vlan)
		if !ok || upper.VlanId != vid || upper.VlanProtocol == netlink.VLAN_PROTOCOL_8021AD {
			continue
		}
		if lower, ok := lowers[upper.ParentIndex]; ok {
			return fmt.Errorf("%s has VLAN link %s for vlan %d, which takes that VLAN's frames before the macvtap child", lower, upper.Name, vid)
		}
	}
	return nil
}

// macvtapVlanFilters is the tc policy of a macvtap child on its clsact qdisc.
// With a VLAN the child is an access port of it: ingress passes only that
// VLAN, untagged; egress drops the guest's own tags and tags the rest. Without
// one the child is untagged-only. The policy closes a trunk parent to the
// guest either way.
func macvtapVlanFilters(linkIndex int, vid int) []netlink.Filter {
	attrs := func(parent uint32, prio uint16, proto uint16) netlink.FilterAttrs {
		return netlink.FilterAttrs{LinkIndex: linkIndex, Parent: parent, Priority: prio, Protocol: proto}
	}
	drop := func(parent uint32, prio uint16, proto uint16) netlink.Filter {
		return &netlink.MatchAll{FilterAttrs: attrs(parent, prio, proto), Actions: []netlink.Action{
			&netlink.GenericAction{ActionAttrs: netlink.ActionAttrs{Action: netlink.TC_ACT_SHOT}},
		}}
	}
	if vid == 0 {
		return []netlink.Filter{
			drop(netlink.HANDLE_MIN_INGRESS, 1, unix.ETH_P_8021Q),
			drop(netlink.HANDLE_MIN_INGRESS, 2, unix.ETH_P_8021AD),
			drop(netlink.HANDLE_MIN_EGRESS, 1, unix.ETH_P_8021Q),
			drop(netlink.HANDLE_MIN_EGRESS, 2, unix.ETH_P_8021AD),
		}
	}
	pop := netlink.NewVlanAction()
	pop.Action = netlink.TCA_VLAN_ACT_POP
	push := netlink.NewVlanAction()
	push.Action = netlink.TCA_VLAN_ACT_PUSH
	push.VlanID = uint16(vid)
	return []netlink.Filter{
		// flower reads the VLAN key only under a VLAN EthType.
		&netlink.Flower{
			FilterAttrs: attrs(netlink.HANDLE_MIN_INGRESS, 1, unix.ETH_P_8021Q),
			EthType:     unix.ETH_P_8021Q,
			VlanId:      uint16(vid),
			Actions:     []netlink.Action{pop},
		},
		drop(netlink.HANDLE_MIN_INGRESS, 9, unix.ETH_P_ALL),
		drop(netlink.HANDLE_MIN_EGRESS, 1, unix.ETH_P_8021Q),
		drop(netlink.HANDLE_MIN_EGRESS, 2, unix.ETH_P_8021AD),
		&netlink.MatchAll{
			FilterAttrs: attrs(netlink.HANDLE_MIN_EGRESS, 9, unix.ETH_P_ALL),
			Actions:     []netlink.Action{push},
		},
	}
}

// macvtapVlanLinkName names the link that registers a child's VLAN; one per
// child in the pod namespace.
func macvtapVlanLinkName(childIndex int) string {
	return fmt.Sprintf("vid%d", childIndex)
}

// applyMacvtapVlanPolicy replaces the child's clsact qdisc with the VLAN
// policy and, for a VLAN, keeps a down 802.1Q link of that VLAN on the child.
// The link only registers the VLAN ID: macvlan passes it to the parent (and a
// bond to its ports), so a parent NIC with hardware VLAN filtering receives
// the VLAN; the kernel drops the registration with the child. The ingress pop
// runs first, so the link never receives a frame.
func applyMacvtapVlanPolicy(nsPath string, ifName string, vid int) error {
	ns, err := netns.GetFromPath(nsPath)
	if err != nil {
		return fmt.Errorf("could not get network namespace from path %s: %w", nsPath, err)
	}
	defer ns.Close()
	nh, err := nlwrap.NewHandleAt(ns)
	if err != nil {
		return fmt.Errorf("could not get netlink handle for namespace %s: %w", nsPath, err)
	}
	defer nh.Close()
	link, err := nh.LinkByName(ifName)
	if err != nil {
		return fmt.Errorf("link %s not found: %w", ifName, err)
	}
	index := link.Attrs().Index

	qdiscs, err := nh.QdiscList(link)
	if err != nil {
		return fmt.Errorf("failed to list qdiscs: %w", err)
	}
	for _, qdisc := range qdiscs {
		if qdisc.Type() == "clsact" {
			if err := nh.QdiscDel(qdisc); err != nil {
				return fmt.Errorf("failed to delete clsact qdisc: %w", err)
			}
		}
	}
	clsact := &netlink.Clsact{QdiscAttrs: netlink.QdiscAttrs{
		LinkIndex: index,
		Handle:    netlink.MakeHandle(0xffff, 0),
		Parent:    netlink.HANDLE_CLSACT,
	}}
	if err := nh.QdiscAdd(clsact); err != nil {
		return fmt.Errorf("failed to add clsact qdisc: %w", err)
	}
	for _, filter := range macvtapVlanFilters(index, vid) {
		if err := nh.FilterAdd(filter); err != nil {
			attrs := filter.Attrs()
			return fmt.Errorf("failed to add %s filter pref %d protocol %#04x: %w", filter.Type(), attrs.Priority, attrs.Protocol, err)
		}
	}
	if vid == 0 {
		return nil
	}

	links, err := nh.LinkList()
	if err != nil {
		return fmt.Errorf("failed to list links: %w", err)
	}
	name := macvtapVlanLinkName(index)
	for _, l := range links {
		if v, ok := l.(*netlink.Vlan); ok && v.ParentIndex == index && v.VlanId == vid {
			return nil
		}
	}
	for _, l := range links {
		if l.Attrs().Name == name {
			return fmt.Errorf("link %s, which registers vlan %d, is taken by another link", name, vid)
		}
	}
	// Loose binding keeps it down when the child comes up.
	looseBinding := true
	vlanLink := &netlink.Vlan{
		LinkAttrs:    netlink.LinkAttrs{Name: name, ParentIndex: index},
		VlanId:       vid,
		VlanProtocol: netlink.VLAN_PROTOCOL_8021Q,
		LooseBinding: &looseBinding,
	}
	if err := nh.LinkAdd(vlanLink); err != nil {
		return fmt.Errorf("failed to add vlan %d link %s: %w", vid, name, err)
	}
	return nil
}

// requireMacvtapVlanPolicy fails a child without its clsact qdisc.
func requireMacvtapVlanPolicy(nsPath string, ifName string) error {
	ns, err := netns.GetFromPath(nsPath)
	if err != nil {
		return fmt.Errorf("could not get network namespace from path %s: %w", nsPath, err)
	}
	defer ns.Close()
	nh, err := nlwrap.NewHandleAt(ns)
	if err != nil {
		return fmt.Errorf("could not get netlink handle for namespace %s: %w", nsPath, err)
	}
	defer nh.Close()
	link, err := nh.LinkByName(ifName)
	if err != nil {
		return fmt.Errorf("link %s not found: %w", ifName, err)
	}
	qdiscs, err := nh.QdiscList(link)
	if err != nil {
		return fmt.Errorf("failed to list qdiscs: %w", err)
	}
	for _, qdisc := range qdiscs {
		if qdisc.Type() == "clsact" {
			return nil
		}
	}
	return errors.New("no VLAN policy: the child has no clsact qdisc")
}

// ensureHostMacvtap creates the macvtap child in the host namespace unless it
// is there already.
func ensureHostMacvtap(hostIfName string, spec macvtap.Spec) error {
	_, err := nlwrap.LinkByName(hostIfName)
	if err == nil {
		return nil
	}
	var linkNotFound netlink.LinkNotFoundError
	if !errors.As(err, &linkNotFound) {
		return fmt.Errorf("failed to look up macvtap link %q: %w", hostIfName, err)
	}
	klog.Infof("creating macvtap link %s on %s again for a new sandbox", hostIfName, spec.Parent)
	return ensureMacvtapDevice(hostIfName, spec)
}

// podTapDevice resolves the tap character device of a macvtap link in a pod
// network namespace, shaped for container injection. Moving a macvtap link
// into another namespace re-registers it, and the kernel gives it the lowest
// free tap minor, so the device is only known once the link is in the pod.
func podTapDevice(nsPath string, ifName string) (LinuxDevice, error) {
	if nsPath == "" {
		return LinuxDevice{}, fmt.Errorf("network namespace of macvtap link %s is unknown", ifName)
	}
	ns, err := netns.GetFromPath(nsPath)
	if err != nil {
		return LinuxDevice{}, fmt.Errorf("could not get network namespace from path %s: %w", nsPath, err)
	}
	defer ns.Close()
	nh, err := nlwrap.NewHandleAt(ns)
	if err != nil {
		return LinuxDevice{}, fmt.Errorf("could not get netlink handle for namespace %s: %w", nsPath, err)
	}
	defer nh.Close()
	link, err := nh.LinkByName(ifName)
	if err != nil {
		return LinuxDevice{}, fmt.Errorf("macvtap link %s not found in namespace %s: %w", ifName, nsPath, err)
	}
	// The node is named after the ifindex the link has in the pod namespace.
	dev, err := GetDeviceInfo(fmt.Sprintf("/dev/tap%d", link.Attrs().Index))
	if err != nil {
		return LinuxDevice{}, fmt.Errorf("tap device of macvtap link %s: %w", ifName, err)
	}
	dev.FileMode = tapDevFileMode
	dev.UID = qemuUID
	dev.GID = qemuGID
	return dev, nil
}

// removeUnusedMacvtaps deletes every macvtap child in the host namespace
// that no stored device references: the children of unprepared claims and
// any that a failed or interrupted prepare or unprepare left behind. It runs
// only where no prepare is in flight (at start, and in unprepare, which the
// kubeletplugin helper serializes with prepare), so every live child is
// stored or already in its pod namespace.
func (np *NetworkDriver) removeUnusedMacvtaps() {
	// Only the driver serving macvtap parents owns these children.
	if _, ok := np.netdb.(macvtapInventory); !ok {
		return
	}
	links, err := nlwrap.LinkList()
	if err != nil {
		klog.Errorf("failed to list links for unused macvtap children: %v", err)
		return
	}
	inUse := set.New[string]()
	for _, podUID := range np.podConfigStore.ListPods() {
		podCfg, _ := np.podConfigStore.GetPodConfig(podUID)
		for _, cfg := range podCfg.DeviceConfigs {
			inUse.Insert(cfg.MacvtapHostIfName)
		}
	}
	carryNetdevPolicies.prune(inUse)
	for _, link := range links {
		name := link.Attrs().Name
		if link.Type() != "macvtap" || !macvtap.IsHostIfName(name) || inUse.Has(name) {
			continue
		}
		if err := netlink.LinkDel(link); err != nil {
			klog.Errorf("failed to delete unused macvtap link %s: %v", name, err)
			continue
		}
		klog.Infof("deleted unused macvtap link %s", name)
	}
}

// deleteHostLink removes a link from the host namespace; a missing link is
// not an error.
func deleteHostLink(ifName string) error {
	link, err := nlwrap.LinkByName(ifName)
	if err != nil {
		var linkNotFound netlink.LinkNotFoundError
		if errors.As(err, &linkNotFound) {
			return nil
		}
		return err
	}
	return netlink.LinkDel(link)
}

// deleteLinkInNS removes a link inside the pod network namespace; a missing
// link or an already-gone namespace is not an error, since the kernel destroys
// virtual links together with the namespace.
func deleteLinkInNS(containerNsPath string, ifName string) error {
	containerNs, err := netns.GetFromPath(containerNsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("could not get network namespace from path %s: %w", containerNsPath, err)
	}
	defer containerNs.Close()

	nhNs, err := nlwrap.NewHandleAt(containerNs)
	if err != nil {
		return fmt.Errorf("could not get netlink handle for namespace %s: %w", containerNsPath, err)
	}
	defer nhNs.Close()

	nsLink, err := nhNs.LinkByName(ifName)
	if err != nil {
		var linkNotFound netlink.LinkNotFoundError
		if errors.As(err, &linkNotFound) {
			return nil
		}
		return fmt.Errorf("link %s not found on namespace %s: %w", ifName, containerNsPath, err)
	}
	if err := nhNs.LinkDel(nsLink); err != nil {
		return fmt.Errorf("failed to delete link %s on namespace %s: %w", ifName, containerNsPath, err)
	}
	klog.V(2).Infof("deleted macvtap link %s in namespace %s", ifName, containerNsPath)
	return nil
}
