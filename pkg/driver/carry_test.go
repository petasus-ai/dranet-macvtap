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
	"strings"
	"testing"
	"time"

	"github.com/containerd/nri/pkg/api"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/dranet/pkg/apis"
	"sigs.k8s.io/dranet/pkg/inventory"
	"sigs.k8s.io/dranet/pkg/macvtap"
)

// fullCarryDeviceConfig populates every carried field for the checkpoint
// wire-format fixture (fullDeviceConfig).
func fullCarryDeviceConfig() CarryDeviceConfig {
	return CarryDeviceConfig{
		DeviceName:        "enp1s0f0v1",
		ShareID:           "4a1b2c3d-0000-4000-8000-000000000001",
		MacvtapHostIfName: "mvt0123456789ab",
		TapDevice: &LinuxDevice{
			Path:     "/dev/tap42",
			Type:     "c",
			Major:    236,
			Minor:    1,
			FileMode: 0o660,
			UID:      107,
			GID:      107,
		},
	}
}

func TestRunPodSandboxReportsShareID(t *testing.T) {
	claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "claim1"}}
	kubeClient := fake.NewClientset(claim)
	store := mustNewPodConfigStore()
	podUID := types.UID("shared-pod")
	// No host interface and no RDMA link: runPodSandbox only reports status.
	if err := store.SetDeviceConfig(podUID, "eth1@claim1-uid@nic", DeviceConfig{
		Claim:             types.NamespacedName{Namespace: "ns", Name: "claim1"},
		CarryDeviceConfig: CarryDeviceConfig{DeviceName: "eth1", ShareID: "share-1"},
	}); err != nil {
		t.Fatalf("SetDeviceConfig() error: %v", err)
	}
	np := &NetworkDriver{
		driverName:     "macvtap.petasus.io",
		nodeName:       "node1",
		kubeClient:     kubeClient,
		podConfigStore: store,
		netdb:          inventory.New(),
		eventRecorder:  record.NewFakeRecorder(100),
	}
	pod := &api.PodSandbox{
		Uid:       string(podUID),
		Name:      "shared-pod",
		Namespace: "ns",
		Linux: &api.LinuxPodSandbox{
			Namespaces: []*api.LinuxNamespace{{Type: "network", Path: "/var/run/netns/test"}},
		},
	}
	if err := np.RunPodSandbox(context.Background(), pod); err != nil {
		t.Fatalf("RunPodSandbox() error: %v", err)
	}

	var devices []resourceapi.AllocatedDeviceStatus
	err := wait.PollUntilContextTimeout(context.Background(), 10*time.Millisecond, 3*time.Second, true,
		func(ctx context.Context) (bool, error) {
			got, err := kubeClient.ResourceV1().ResourceClaims("ns").Get(ctx, "claim1", metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			devices = got.Status.Devices
			return len(devices) > 0, nil
		})
	if err != nil {
		t.Fatalf("claim status was not updated: %v", err)
	}
	if len(devices) != 1 || devices[0].Device != "eth1" || devices[0].Pool != "node1" {
		t.Fatalf("unexpected status devices %+v", devices)
	}
	if devices[0].ShareID == nil || *devices[0].ShareID != "share-1" {
		t.Fatalf("status device shareID = %v, want share-1", devices[0].ShareID)
	}
}

func TestCreateContainerInjectsTapDevice(t *testing.T) {
	np := &NetworkDriver{podConfigStore: mustNewPodConfigStore()}
	podUID := types.UID("pod-a")
	tap := &LinuxDevice{Path: "/dev/tap42", Type: "c", Major: 236, Minor: 1, FileMode: 0o660, UID: 107, GID: 107}
	for _, key := range []string{"eth1@claim1-uid@nic", "eth1@claim2-uid@nic"} {
		if err := np.podConfigStore.SetDeviceConfig(podUID, key, DeviceConfig{
			Claim:             types.NamespacedName{Namespace: "ns", Name: key},
			CarryDeviceConfig: CarryDeviceConfig{DeviceName: "eth1", TapDevice: tap},
		}); err != nil {
			t.Fatalf("SetDeviceConfig: %v", err)
		}
	}
	pod := &api.PodSandbox{Uid: string(podUID), Name: "virt-launcher", Namespace: "ns"}
	adjust, _, err := np.CreateContainer(context.Background(), pod, &api.Container{Name: "compute"})
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}
	if adjust == nil || adjust.Linux == nil || len(adjust.Linux.Devices) != 1 {
		t.Fatalf("injected %+v, want the tap device once", adjust)
	}
	got := adjust.Linux.Devices[0]
	if got.Path != "/dev/tap42" || got.Uid.GetValue() != 107 || got.Gid.GetValue() != 107 || got.FileMode.GetValue() != 0o660 {
		t.Errorf("tap device %+v, want qemu-owned 0660 /dev/tap42", got)
	}
}

func TestWatchHealthStatusHoldsTheStream(t *testing.T) {
	defer func(interval time.Duration) { carryHealthReportInterval = interval }(carryHealthReportInterval)
	carryHealthReportInterval = 20 * time.Millisecond

	np := &NetworkDriver{}
	ctx, cancel := context.WithCancel(context.Background())
	reports := make(chan kubeletplugin.DeviceHealthReport)
	done := make(chan error, 1)
	go func() { done <- np.WatchHealthStatus(ctx, reports) }()

	// The report is re-sent, so the helper never sees a stale stream.
	for i := 0; i < 2; i++ {
		select {
		case <-reports:
		case <-time.After(time.Second):
			t.Fatalf("health report %d not sent", i+1)
		}
	}
	select {
	case err := <-done:
		t.Fatalf("WatchHealthStatus returned %v before the stream was closed", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WatchHealthStatus returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WatchHealthStatus did not return after the stream was closed")
	}
}

type fakeMacvtapInventory struct {
	*fakeInventoryDB
	specs map[string]macvtap.Spec
}

func (f *fakeMacvtapInventory) GetMacvtapSpec(deviceName string) (macvtap.Spec, bool) {
	spec, ok := f.specs[deviceName]
	return spec, ok
}

func TestMacvtapStoreKeyAndDHCPRefusal(t *testing.T) {
	db := &fakeMacvtapInventory{fakeInventoryDB: newFakeInventoryDB(), specs: map[string]macvtap.Spec{"eth1": {Parent: "eth1"}}}
	np := &NetworkDriver{netdb: db}
	claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{UID: "claim-uid"}}
	shared := resourceapi.DeviceRequestAllocationResult{Device: "eth1", Request: "nic"}

	if got := np.deviceStoreKey(claim, shared); got != "eth1@claim-uid@nic" {
		t.Errorf("store key of a macvtap parent = %q, want it per claim", got)
	}
	if got := np.deviceStoreKey(claim, resourceapi.DeviceRequestAllocationResult{Device: "eth2", Request: "nic"}); got != "eth2" {
		t.Errorf("store key of another device = %q, want the device name", got)
	}

	for _, iface := range []apis.InterfaceConfig{{Addressing: apis.AddressingModeDHCP}, {DHCP: ptr.To(true)}} {
		cfg := DeviceConfig{NetworkInterfaceConfigInPod: apis.NetworkConfig{Interface: iface}}
		if _, err := np.prepareCarryNetdev(claim, shared, &cfg); err == nil || !strings.Contains(err.Error(), "DHCP") {
			t.Errorf("interface %+v: err = %v, want a DHCP refusal", iface, err)
		}
		if cfg.MacvtapHostIfName != "" {
			t.Errorf("interface %+v: a macvtap child was recorded for a refused claim", iface)
		}
	}
}
