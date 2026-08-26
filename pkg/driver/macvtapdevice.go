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
	"time"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/dranet/internal/nlwrap"
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

	// tapDevWaitTimeout bounds the wait for devtmpfs to create /dev/tapN
	// after the macvtap link is added.
	tapDevWaitTimeout = 2 * time.Second
	tapDevWaitStep    = 50 * time.Millisecond
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
// slot in the host namespace and resolves its tap character device. Recreate
// semantics keep the operation idempotent across kubelet retries and driver
// restarts.
func ensureMacvtapDevice(hostIfName string, spec macvtap.Spec) (LinuxDevice, error) {
	parent, err := nlwrap.LinkByName(spec.Parent)
	if err != nil {
		return LinuxDevice{}, fmt.Errorf("macvtap parent %q not found: %w", spec.Parent, err)
	}

	if err := deleteHostLink(hostIfName); err != nil {
		return LinuxDevice{}, fmt.Errorf("failed to delete stale macvtap link %q: %w", hostIfName, err)
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
		return LinuxDevice{}, fmt.Errorf("failed to create macvtap %q on parent %q: %w", hostIfName, spec.Parent, err)
	}

	created, err := nlwrap.LinkByName(hostIfName)
	if err != nil {
		return LinuxDevice{}, fmt.Errorf("macvtap %q not found after creation: %w", hostIfName, err)
	}

	tapDev, err := waitTapDevice(created.Attrs().Index)
	if err != nil {
		_ = deleteHostLink(hostIfName)
		return LinuxDevice{}, err
	}
	return tapDev, nil
}

// waitTapDevice waits for devtmpfs to create the /dev/tap<ifindex> character
// device of a macvtap link and returns it shaped for container injection.
func waitTapDevice(ifindex int) (LinuxDevice, error) {
	path := fmt.Sprintf("/dev/tap%d", ifindex)
	deadline := time.Now().Add(tapDevWaitTimeout)
	for {
		dev, err := GetDeviceInfo(path)
		if err == nil {
			dev.FileMode = tapDevFileMode
			dev.UID = qemuUID
			dev.GID = qemuGID
			return dev, nil
		}
		if time.Now().After(deadline) {
			return LinuxDevice{}, fmt.Errorf("tap device %s did not appear: %w", path, err)
		}
		time.Sleep(tapDevWaitStep)
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
