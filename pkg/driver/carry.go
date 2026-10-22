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
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	resourceapply "k8s.io/client-go/applyconfigurations/resource/v1"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/klog/v2"
	"k8s.io/utils/set"
	"sigs.k8s.io/dranet/pkg/apis"
	"sigs.k8s.io/dranet/pkg/macvtap"
)

// carryState is embedded in NetworkDriver.
type carryState struct {
	// claimOpMu serializes prepare/unprepare claim processing. Kubelet can
	// overlap an in-flight unprepare with the next pod's prepare of the
	// same claim (a VM restart): without the lock, unprepare's store
	// cleanup races the fresh prepare and deletes the new pod's state.
	claimOpMu sync.Mutex
}

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

	// TapDevice is the tap character device of the macvtap child, injected
	// into the Pod's containers so qemu can open it.
	TapDevice *LinuxDevice `json:"tapDevice,omitempty"`
}

// initCarry runs in Start once the options are applied.
func (np *NetworkDriver) initCarry(_ string) {}

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

// lockCarryClaim serializes claim prepare and unprepare (claimOpMu); call
// it as defer np.lockCarryClaim()().
func (np *NetworkDriver) lockCarryClaim() func() {
	np.claimOpMu.Lock()
	return np.claimOpMu.Unlock
}

// carryClaim collects what the carried devices of one claim prepared.
type carryClaim struct {
	devices []kubeletplugin.Device
}

// prepareCarryDevice prepares a device the upstream netdev path cannot;
// every macvtap device goes through that path, so handled is always false.
func (np *NetworkDriver) prepareCarryDevice(_ context.Context, _ *resourceapi.ResourceClaim, _ types.UID, _ resourceapi.DeviceRequestAllocationResult, _ *carryClaim) (bool, error) {
	return false, nil
}

// finishCarryClaim runs after every device of a claim prepared without
// error.
func (np *NetworkDriver) finishCarryClaim(_ *resourceapi.ResourceClaim, _ *carryClaim) error {
	return nil
}

// deviceStoreKey is the PodConfigStore key of a device prepared by the
// upstream netdev path: per claim for a shared macvtap parent, whose device
// name alone would collide when one pod carries two claims on it.
func (np *NetworkDriver) deviceStoreKey(claim *resourceapi.ResourceClaim, result resourceapi.DeviceRequestAllocationResult) string {
	if _, isMacvtap := macvtapSpecOf(np.netdb, result.Device); isMacvtap {
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
	if iface.Addressing == apis.AddressingModeDHCP || iface.DHCP != nil && *iface.DHCP {
		return "", fmt.Errorf("DHCP is not supported for macvtap device %s", result.Device)
	}
	deviceCfg.DeviceName = result.Device
	if result.ShareID != nil {
		deviceCfg.ShareID = string(*result.ShareID)
	}
	hostIfName := macvtap.HostIfName(result.Device, claim.UID, result.Request)
	tapDev, err := ensureMacvtapDevice(hostIfName, spec)
	if err != nil {
		return "", fmt.Errorf("failed to create macvtap for device %s: %w", result.Device, err)
	}
	deviceCfg.MacvtapHostIfName = hostIfName
	deviceCfg.TapDevice = &tapDev
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

// unprepareCarryDevice runs for every stored device of an unprepared claim.
func (np *NetworkDriver) unprepareCarryDevice(claim kubeletplugin.NamespacedObject, _ string, devCfg DeviceConfig) {
	// Remove a macvtap child that never left the host namespace (the pod was
	// never started); links moved into a pod netns die with the namespace.
	if devCfg.MacvtapHostIfName != "" {
		if err := deleteHostLink(devCfg.MacvtapHostIfName); err != nil {
			klog.Errorf("failed to delete macvtap link %s for claim %v: %v", devCfg.MacvtapHostIfName, claim.NamespacedName, err)
		}
	}
}

// unprepareCarryClaim runs once per unprepared claim.
func (np *NetworkDriver) unprepareCarryClaim(_ kubeletplugin.NamespacedObject) {}

// addCarryContainerDevices adds the devices NRI hands to every container of
// the pod: the macvtap tap character device, owned by qemu so virt-launcher
// can hand it to the VM.
func (np *NetworkDriver) addCarryContainerDevices(adjust *api.ContainerAdjustment, config DeviceConfig, devPaths set.Set[string]) {
	dev := config.TapDevice
	if dev == nil || devPaths.Has(dev.Path) {
		return
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

// stopCarryDevice tears a carried device down in StopPodSandbox; handled is
// false for a device the upstream path detaches. A macvtap child is never
// returned to the host: it is deleted in the pod namespace (the kernel would
// destroy it with the namespace anyway), and UnprepareResourceClaims covers
// host-side leftovers.
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
