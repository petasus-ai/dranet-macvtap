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
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/vishvananda/netlink"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"sigs.k8s.io/dranet/internal/nlwrap"
	userns "sigs.k8s.io/dranet/internal/testutils"
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
		MacvtapParent:     "enp1s0f0",
		MacvtapMode:       netlink.MACVLAN_MODE_BRIDGE,
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

func TestCreateContainerInjectsTheTapDeviceOfTheMovedChild(t *testing.T) {
	userns.Run(t, testCreateContainerInjectsTheTapDeviceOfTheMovedChild, syscall.CLONE_NEWNET, syscall.CLONE_NEWNS)
}

func testCreateContainerInjectsTheTapDeviceOfTheMovedChild(t *testing.T) {
	env := newIPVlanTestEnv(t, 1500)
	spec := macvtap.Spec{Parent: env.parent, Mode: netlink.MACVLAN_MODE_BRIDGE}
	for _, name := range []string{"mvtfreed", "mvtclaim"} {
		if err := ensureMacvtapDevice(name, spec); err != nil {
			t.Fatalf("ensureMacvtapDevice(%s): %v", name, err)
		}
	}
	hostLink, err := nlwrap.LinkByName("mvtclaim")
	if err != nil {
		t.Fatal(err)
	}
	atPrepare, err := GetDeviceInfo(fmt.Sprintf("/dev/tap%d", hostLink.Attrs().Index))
	if err != nil {
		t.Fatal(err)
	}
	// A lower tap minor freed before the pod starts is the one the moved child gets.
	if err := deleteHostLink("mvtfreed"); err != nil {
		t.Fatal(err)
	}

	claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "claim1"}}
	np := &NetworkDriver{
		driverName:     "macvtap.petasus.io",
		nodeName:       "node1",
		kubeClient:     fake.NewClientset(claim),
		podConfigStore: mustNewPodConfigStore(),
		netdb:          inventory.New(),
		eventRecorder:  record.NewFakeRecorder(100),
	}
	podUID := types.UID("pod-a")
	if err := np.podConfigStore.SetDeviceConfig(podUID, "eth1@claim1-uid@nic", DeviceConfig{
		Claim:                        types.NamespacedName{Namespace: "ns", Name: "claim1"},
		NetworkInterfaceConfigInHost: apis.NetworkConfig{Interface: apis.InterfaceConfig{Name: "mvtclaim"}},
		NetworkInterfaceConfigInPod:  apis.NetworkConfig{Interface: apis.InterfaceConfig{Name: "net1"}},
		CarryDeviceConfig:            CarryDeviceConfig{DeviceName: "eth1", MacvtapHostIfName: "mvtclaim"},
	}); err != nil {
		t.Fatalf("SetDeviceConfig: %v", err)
	}
	pod := &api.PodSandbox{
		Uid:       string(podUID),
		Name:      "virt-launcher",
		Namespace: "ns",
		Linux: &api.LinuxPodSandbox{
			Namespaces: []*api.LinuxNamespace{{Type: "network", Path: env.nsPath}},
		},
	}
	if err := np.RunPodSandbox(context.Background(), pod); err != nil {
		t.Fatalf("RunPodSandbox: %v", err)
	}
	adjust, _, err := np.CreateContainer(context.Background(), pod, &api.Container{Name: "compute"})
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}

	want, err := podTapDevice(env.nsPath, "net1")
	if err != nil {
		t.Fatal(err)
	}
	if want.Minor == atPrepare.Minor {
		t.Fatalf("moving the child kept tap minor %d; the kernel did not re-register it", want.Minor)
	}
	if adjust == nil || adjust.Linux == nil || len(adjust.Linux.Devices) != 1 {
		t.Fatalf("injected %+v, want the tap device once", adjust)
	}
	got := adjust.Linux.Devices[0]
	if got.Path != want.Path || got.Major != want.Major || got.Minor != want.Minor {
		t.Errorf("tap device %s %d:%d, want %s %d:%d (the child in the pod namespace)", got.Path, got.Major, got.Minor, want.Path, want.Major, want.Minor)
	}
	if got.Uid.GetValue() != 107 || got.Gid.GetValue() != 107 || got.FileMode.GetValue() != 0o660 {
		t.Errorf("tap device %+v, want qemu-owned 0660", got)
	}
}

func TestRunPodSandboxRecreatesTheChildOfAStoppedSandbox(t *testing.T) {
	userns.Run(t, testRunPodSandboxRecreatesTheChildOfAStoppedSandbox, syscall.CLONE_NEWNET, syscall.CLONE_NEWNS)
}

// Stopping a sandbox deletes the macvtap child; kubelet's next sandbox for the
// pod gets a new child on the parent prepare recorded, or for an entry
// checkpointed without it, on the inventory's parent.
func testRunPodSandboxRecreatesTheChildOfAStoppedSandbox(t *testing.T) {
	for name, recorded := range map[string]bool{"recorded spec": true, "earlier checkpoint": false} {
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
				Claim:                       types.NamespacedName{Namespace: "ns", Name: "claim1"},
				NetworkInterfaceConfigInPod: apis.NetworkConfig{Interface: apis.InterfaceConfig{Name: "net1"}},
			}
			hostIfName, err := np.prepareCarryNetdev(claim, result, &cfg)
			if err != nil {
				t.Fatalf("prepareCarryNetdev: %v", err)
			}
			cfg.NetworkInterfaceConfigInHost.Interface.Name = hostIfName
			if !recorded {
				cfg.MacvtapParent, cfg.MacvtapMode = "", 0
			}
			podUID := types.UID("pod-a")
			if err := np.podConfigStore.SetDeviceConfig(podUID, np.deviceStoreKey(claim, result), cfg); err != nil {
				t.Fatalf("SetDeviceConfig: %v", err)
			}
			pod := &api.PodSandbox{
				Uid:       string(podUID),
				Name:      "virt-launcher",
				Namespace: "ns",
				Linux: &api.LinuxPodSandbox{
					Namespaces: []*api.LinuxNamespace{{Type: "network", Path: env.nsPath}},
				},
			}

			for sandbox := 1; sandbox <= 2; sandbox++ {
				if err := np.RunPodSandbox(context.Background(), pod); err != nil {
					t.Fatalf("sandbox %d: RunPodSandbox: %v", sandbox, err)
				}
				if _, err := podTapDevice(env.nsPath, "net1"); err != nil {
					t.Fatalf("sandbox %d: no macvtap child in the pod: %v", sandbox, err)
				}
				if err := np.StopPodSandbox(context.Background(), pod); err != nil {
					t.Fatalf("sandbox %d: StopPodSandbox: %v", sandbox, err)
				}
				if _, err := podTapDevice(env.nsPath, "net1"); err == nil {
					t.Fatalf("sandbox %d: the macvtap child outlived its sandbox", sandbox)
				}
			}
		})
	}
}

func TestCreateContainerFailsWithoutThePodNamespace(t *testing.T) {
	np := &NetworkDriver{podConfigStore: mustNewPodConfigStore()}
	podUID := types.UID("pod-a")
	if err := np.podConfigStore.SetDeviceConfig(podUID, "eth1@claim1-uid@nic", DeviceConfig{
		Claim:             types.NamespacedName{Namespace: "ns", Name: "claim1"},
		CarryDeviceConfig: CarryDeviceConfig{DeviceName: "eth1", MacvtapHostIfName: "mvtclaim"},
	}); err != nil {
		t.Fatalf("SetDeviceConfig: %v", err)
	}
	pod := &api.PodSandbox{Uid: string(podUID), Name: "virt-launcher", Namespace: "ns"}
	if _, _, err := np.CreateContainer(context.Background(), pod, &api.Container{Name: "compute"}); err == nil || !strings.Contains(err.Error(), "eth1") {
		t.Fatalf("CreateContainer error = %v, want a tap device error for eth1", err)
	}
}

func TestRemoveUnusedMacvtapsKeepsStoredChildren(t *testing.T) {
	userns.Run(t, testRemoveUnusedMacvtapsKeepsStoredChildren, syscall.CLONE_NEWNET, syscall.CLONE_NEWNS)
}

func testRemoveUnusedMacvtapsKeepsStoredChildren(t *testing.T) {
	env := newIPVlanTestEnv(t, 1500)
	spec := macvtap.Spec{Parent: env.parent, Mode: netlink.MACVLAN_MODE_BRIDGE}
	stored, unused, foreign := "mvt000000000001", "mvt000000000002", "macvtap0"
	for _, name := range []string{stored, unused, foreign} {
		if err := ensureMacvtapDevice(name, spec); err != nil {
			t.Fatalf("ensureMacvtapDevice(%s): %v", name, err)
		}
	}
	// A driver without the macvtap inventory leaves every child alone.
	(&NetworkDriver{podConfigStore: mustNewPodConfigStore(), netdb: newFakeInventoryDB()}).removeUnusedMacvtaps()
	if _, err := nlwrap.LinkByName(unused); err != nil {
		t.Fatalf("a driver without the macvtap inventory deleted %s: %v", unused, err)
	}

	np := &NetworkDriver{podConfigStore: mustNewPodConfigStore(), netdb: &fakeMacvtapInventory{fakeInventoryDB: newFakeInventoryDB()}}
	if err := np.podConfigStore.SetDeviceConfig("pod-a", "eth1@claim1-uid@nic", DeviceConfig{
		Claim:             types.NamespacedName{Namespace: "ns", Name: "claim1"},
		CarryDeviceConfig: CarryDeviceConfig{DeviceName: "eth1", MacvtapHostIfName: stored},
	}); err != nil {
		t.Fatalf("SetDeviceConfig: %v", err)
	}

	np.removeUnusedMacvtaps()

	for name, wantPresent := range map[string]bool{stored: true, unused: false, foreign: true} {
		_, err := nlwrap.LinkByName(name)
		if present := err == nil; present != wantPresent {
			t.Errorf("link %s present = %v, want %v", name, present, wantPresent)
		}
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

func TestMacvtapStoreKeyAndRefusedInterfaces(t *testing.T) {
	db := &fakeMacvtapInventory{fakeInventoryDB: newFakeInventoryDB(), specs: map[string]macvtap.Spec{"eth1": {Parent: "eth1"}}}
	np := &NetworkDriver{netdb: db}
	claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{UID: "claim-uid"}}
	shared := resourceapi.DeviceRequestAllocationResult{Device: "eth1", Request: "nic"}

	if got := np.deviceStoreKey(claim, shared); got != "eth1@claim-uid@nic" {
		t.Errorf("store key of a macvtap parent = %q, want it per claim", got)
	}
	// A parent the inventory dropped between two lookups keeps its key.
	if got := np.deviceStoreKey(claim, resourceapi.DeviceRequestAllocationResult{Device: "eth2", Request: "nic"}); got != "eth2@claim-uid@nic" {
		t.Errorf("store key of an unknown device = %q, want it per claim", got)
	}
	if got := (&NetworkDriver{netdb: newFakeInventoryDB()}).deviceStoreKey(claim, shared); got != "eth1" {
		t.Errorf("store key without a macvtap inventory = %q, want the device name", got)
	}

	for _, tc := range []struct {
		iface apis.InterfaceConfig
		want  string
	}{
		{apis.InterfaceConfig{Addressing: apis.AddressingModeDHCP}, "DHCP"},
		{apis.InterfaceConfig{Type: apis.InterfaceTypeIPVLAN}, "IPVLAN"},
	} {
		cfg := DeviceConfig{NetworkInterfaceConfigInPod: apis.NetworkConfig{Interface: tc.iface}}
		if _, err := np.prepareCarryNetdev(claim, shared, &cfg); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("interface %+v: err = %v, want a %s refusal", tc.iface, err, tc.want)
		}
		if cfg.MacvtapHostIfName != "" {
			t.Errorf("interface %+v: a macvtap child was recorded for a refused claim", tc.iface)
		}
	}
}
