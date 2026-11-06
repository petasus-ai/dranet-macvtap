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

// This file holds what the macvtap.petasus.io driver adds to the upstream
// driver. The upstream files reach it only through one-line hooks (the
// carry* names), so rebasing onto a newer upstream carries those calls and
// nothing else.

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/vishvananda/netlink"
	resourceapi "k8s.io/api/resource/v1"
	resourceapply "k8s.io/client-go/applyconfigurations/resource/v1"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/klog/v2"
	"k8s.io/utils/set"
	"sigs.k8s.io/dranet/pkg/apis"
	"sigs.k8s.io/dranet/pkg/macvtap"
)

// CarryDeviceConfig is embedded in DeviceConfig, so its fields sit at the top
// level of the checkpointed JSON.
type CarryDeviceConfig struct {
	// DeviceName is the published ResourceSlice device name when it differs
	// from the store key: a shared macvtap parent backs several claims
	// (allowMultipleAllocations), so those entries are keyed per claim.
	DeviceName string `json:"deviceName,omitempty"`

	// ShareID is the allocation share of a device that allows multiple
	// allocations; status.devices entries must carry it to match the result.
	ShareID string `json:"shareID,omitempty"`

	// MacvtapHostIfName is the host-side link name of the macvtap child the
	// driver created for this device; empty for non-macvtap devices.
	MacvtapHostIfName string `json:"macvtapHostIfName,omitempty"`

	// MacvtapParent and MacvtapMode are the creation spec of the child, which
	// a later sandbox of the pod gets anew (runCarryDevice). An entry without
	// them, checkpointed by an earlier driver, takes the inventory's spec.
	MacvtapParent string              `json:"macvtapParent,omitempty"`
	MacvtapMode   netlink.MacvlanMode `json:"macvtapMode,omitempty"`
}

// initCarry runs in Start once the options are applied, before the driver
// serves any request.
func (np *NetworkDriver) initCarry() {
	np.removeUnusedMacvtaps()
}

// carryKubeletOptions is appended to the kubelet plugin options, so it
// overrides the upstream ones.
func (np *NetworkDriver) carryKubeletOptions() []kubeletplugin.Option {
	// Kubelet 1.36 re-dials a health stream that is absent or declined every
	// few seconds and logs each attempt; WatchHealthStatus holds it open.
	return []kubeletplugin.Option{kubeletplugin.HealthService(true)}
}

// carryHealthReportInterval re-sends the empty health report well inside the
// 30s after which the kubelet plugin helper flags a silent stream as stale.
var carryHealthReportInterval = 10 * time.Second

// watchCarryHealth serves WatchHealthStatus. The driver tracks no per-device
// health, so the stream carries an empty report, re-sent periodically, and
// stays open until the kubelet closes it: kubelet 1.36 re-dials an absent or
// declined stream every few seconds and logs each attempt.
func (np *NetworkDriver) watchCarryHealth(ctx context.Context, reports chan<- kubeletplugin.DeviceHealthReport) error {
	ticker := time.NewTicker(carryHealthReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case reports <- kubeletplugin.DeviceHealthReport{}:
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// deviceStoreKey is the PodConfigStore key of a device prepared by the
// upstream netdev path: per claim when the inventory serves shared macvtap
// parents, whose device name alone would collide when one pod carries two
// claims on one parent.
func (np *NetworkDriver) deviceStoreKey(claim *resourceapi.ResourceClaim, result resourceapi.DeviceRequestAllocationResult) string {
	if _, ok := np.netdb.(macvtapInventory); ok {
		return macvtapStoreKey(result.Device, claim.UID, result.Request)
	}
	return result.Device
}

// prepareCarryNetdev resolves the host interface of a device prepared by the
// upstream netdev path. A macvtap parent has no netdev of its own to hand
// over: the driver creates a per-claim child link on it and records it in
// deviceCfg.
func (np *NetworkDriver) prepareCarryNetdev(claim *resourceapi.ResourceClaim, result resourceapi.DeviceRequestAllocationResult, deviceCfg *DeviceConfig) (string, error) {
	spec, isMacvtap := macvtapSpecOf(np.netdb, result.Device)
	if !isMacvtap {
		return np.netdb.GetNetInterfaceName(result.Device)
	}
	iface := deviceCfg.NetworkInterfaceConfigInPod.Interface
	// Claim defaulting folds the deprecated dhcp field into Addressing.
	if iface.Addressing == apis.AddressingModeDHCP {
		return "", fmt.Errorf("DHCP is not supported for macvtap device %s", result.Device)
	}
	// The child itself is the pod's interface; a subinterface would be built
	// on top of it instead.
	if iface.IsSubinterface() {
		return "", fmt.Errorf("interface type %s is not supported for macvtap device %s", iface.Type, result.Device)
	}
	if err := checkMacvtapVlan(spec.Parent, deviceCfg.NetworkInterfaceConfigInPod.Macvtap); err != nil {
		return "", fmt.Errorf("macvtap device %s: %w", result.Device, err)
	}
	deviceCfg.DeviceName = result.Device
	if result.ShareID != nil {
		deviceCfg.ShareID = string(*result.ShareID)
	}
	hostIfName := macvtap.HostIfName(result.Device, claim.UID, result.Request)
	if err := ensureMacvtapDevice(hostIfName, spec); err != nil {
		return "", fmt.Errorf("failed to create macvtap for device %s: %w", result.Device, err)
	}
	deviceCfg.MacvtapHostIfName = hostIfName
	deviceCfg.MacvtapParent = spec.Parent
	deviceCfg.MacvtapMode = spec.Mode
	return hostIfName, nil
}

// rollbackCarryNetdev undoes prepareCarryNetdev when the device was not
// committed.
func (np *NetworkDriver) rollbackCarryNetdev(committed bool, deviceCfg DeviceConfig) {
	if committed || deviceCfg.MacvtapHostIfName == "" {
		return
	}
	if err := deleteHostLink(deviceCfg.MacvtapHostIfName); err != nil {
		klog.Errorf("failed to delete macvtap link %s after a failed prepare: %v", deviceCfg.MacvtapHostIfName, err)
	}
}

// unprepareCarryClaim runs once per unprepared claim, after its devices left
// the store: their host-side children go with any earlier leftovers.
func (np *NetworkDriver) unprepareCarryClaim(_ kubeletplugin.NamespacedObject) {
	np.removeUnusedMacvtaps()
}

// addCarryContainerDevices adds the tap character device of every macvtap
// child in the pod to the container, owned by qemu so virt-launcher can hand
// it to the VM. The device is read from the child as it is in the pod
// namespace (see podTapDevice).
func (np *NetworkDriver) addCarryContainerDevices(adjust *api.ContainerAdjustment, podConfig PodConfig, devPaths set.Set[string]) error {
	for storeKey, config := range podConfig.DeviceConfigs {
		if config.MacvtapHostIfName == "" {
			continue
		}
		dev, err := podTapDevice(podConfig.NetNS, config.NetworkInterfaceConfigInPod.Interface.Name)
		if err != nil {
			return fmt.Errorf("macvtap device %s: %w", carryDeviceName(storeKey, config), err)
		}
		if devPaths.Has(dev.Path) {
			continue
		}
		devPaths.Insert(dev.Path)
		adjust.AddDevice(&api.LinuxDevice{
			Path:     dev.Path,
			Type:     dev.Type,
			Major:    dev.Major,
			Minor:    dev.Minor,
			FileMode: api.FileMode(dev.FileMode),
			Uid:      api.UInt32(dev.UID),
			Gid:      api.UInt32(dev.GID),
		})
	}
	return nil
}

// carryDeviceName is the published device name of a stored entry.
func carryDeviceName(storeKey string, config DeviceConfig) string {
	if config.DeviceName != "" {
		return config.DeviceName
	}
	return storeKey
}

// carryDeviceStatus completes the claim status of a device in RunPodSandbox.
func (np *NetworkDriver) carryDeviceStatus(config DeviceConfig, status *resourceapply.AllocatedDeviceStatusApplyConfiguration) {
	if config.ShareID != "" {
		status.WithShareID(config.ShareID)
	}
}

// carryNetdevPolicies hands a macvtap child's VLAN policy from runCarryDevice
// to its attach in the same RunPodSandbox (setupCarryNetdevInNS), keyed by
// pod namespace and pod interface name: the attach carries only the
// interface config. Each child holds at most one entry, so an attach that
// failed before taking its entry leaves nothing behind once the next sandbox
// stores its own or the child goes unused (removeUnusedMacvtaps).
var carryNetdevPolicies = &carryPolicies{byKey: map[string]carryPolicy{}, keyOf: map[string]string{}}

type carryPolicy struct {
	hostIfName string
	vid        int
}

type carryPolicies struct {
	mu    sync.Mutex
	byKey map[string]carryPolicy
	keyOf map[string]string // host interface name -> key
}

func carryPolicyKey(nsPath string, ifName string) string {
	return nsPath + "\x00" + ifName
}

func (p *carryPolicies) put(key string, policy carryPolicy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.keyOf[policy.hostIfName]; ok {
		delete(p.byKey, old)
	}
	p.byKey[key] = policy
	p.keyOf[policy.hostIfName] = key
}

func (p *carryPolicies) take(key string) (carryPolicy, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	policy, ok := p.byKey[key]
	if ok {
		delete(p.byKey, key)
		delete(p.keyOf, policy.hostIfName)
	}
	return policy, ok
}

// prune drops the entries of children no stored device references.
func (p *carryPolicies) prune(inUse set.Set[string]) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for host, key := range p.keyOf {
		if !inUse.Has(host) {
			delete(p.byKey, key)
			delete(p.keyOf, host)
		}
	}
}

func (p *carryPolicies) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.byKey)
}

// runCarryDevice readies a carried device in RunPodSandbox before it is
// attached. A macvtap child goes with the sandbox that held it
// (stopCarryDevice), so a later sandbox of the pod gets a new one on the
// stored parent: kubelet creates one for a restartPolicy Never pod (a KubeVirt
// launcher) while none of its containers has run. A child with a VLAN is
// always created anew: one a failed attach returned to the host keeps its
// registration link in the namespace it left, and that link would refuse a
// second one.
func (np *NetworkDriver) runCarryDevice(ns string, deviceName string, config DeviceConfig) error {
	if config.MacvtapHostIfName == "" {
		return nil
	}
	vid := macvtapVlanOf(config.NetworkInterfaceConfigInPod)
	spec := macvtap.Spec{Parent: config.MacvtapParent, Mode: config.MacvtapMode}
	known := spec.Parent != ""
	if !known {
		spec, known = macvtapSpecOf(np.netdb, deviceName)
	}
	// Without a spec the child is attached as it is.
	if known {
		ensure := ensureHostMacvtap
		if vid != 0 {
			ensure = ensureMacvtapDevice
		}
		if err := ensure(config.MacvtapHostIfName, spec); err != nil {
			return fmt.Errorf("macvtap device %s: %w", deviceName, err)
		}
	}
	podIfName := config.NetworkInterfaceConfigInPod.Interface.Name
	if podIfName == "" {
		podIfName = config.MacvtapHostIfName
	}
	carryNetdevPolicies.put(carryPolicyKey(ns, podIfName), carryPolicy{hostIfName: config.MacvtapHostIfName, vid: vid})
	return nil
}

// setupCarryNetdevInNS applies the VLAN policy runCarryDevice recorded for a
// macvtap child once the child is in the pod namespace and before it comes
// up, so the pod's kernel never sends on it unfiltered. The move destroyed the
// child's qdiscs, so every sandbox gets the policy anew.
func setupCarryNetdevInNS(nsPath string, ifName string) error {
	policy, ok := carryNetdevPolicies.take(carryPolicyKey(nsPath, ifName))
	if !ok {
		return nil
	}
	if err := applyMacvtapVlanPolicy(nsPath, ifName, policy.vid); err != nil {
		return fmt.Errorf("macvtap child %s in ns %s: %w", ifName, nsPath, err)
	}
	return nil
}

// carryKeepsDetachedDown keeps a macvtap child that a failed attach returned
// to the host down: no host workload uses it, and there it has no VLAN policy.
func carryKeepsDetachedDown(hostIfName string) bool {
	return macvtap.IsHostIfName(hostIfName)
}

// configureCarryNetdevInNS fails a macvtap child that reached the pod
// namespace without its VLAN policy.
func configureCarryNetdevInNS(nsPath string, ifNameInNs string, config DeviceConfig) error {
	if config.MacvtapHostIfName == "" {
		return nil
	}
	if err := requireMacvtapVlanPolicy(nsPath, ifNameInNs); err != nil {
		return fmt.Errorf("macvtap child %s in ns %s: %w", ifNameInNs, nsPath, err)
	}
	return nil
}

// stopCarryDevice tears a carried device down in StopPodSandbox; handled is
// false for a device the upstream path detaches. A macvtap child is deleted in
// the pod namespace (the kernel would destroy it with the namespace anyway),
// not returned to the host.
func (np *NetworkDriver) stopCarryDevice(ctx context.Context, ns string, deviceName string, config DeviceConfig) bool {
	if config.MacvtapHostIfName == "" {
		return false
	}
	if ifName := config.NetworkInterfaceConfigInPod.Interface.Name; ifName != "" {
		if err := deleteLinkInNS(ns, ifName); err != nil {
			klog.FromContext(ctx).Error(err, "Failed to delete macvtap device", "device", deviceName)
		}
	}
	return true
}
