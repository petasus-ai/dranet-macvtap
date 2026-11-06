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

package apis

// CarryNetworkConfig is embedded in NetworkConfig: the claim parameters the
// macvtap.petasus.io driver adds to the upstream schema.
type CarryNetworkConfig struct {
	// Macvtap configures the per-claim macvtap child.
	Macvtap *MacvtapConfig `json:"macvtap,omitempty"`
}

// MacvtapConfig is the claim's macvtap child configuration.
type MacvtapConfig struct {
	// Vlan is the 802.1Q VLAN ID (1-4094) the child carries on its parent:
	// the driver tags the guest's frames with it and passes the guest only
	// that VLAN, untagged. Unset, the guest is untagged-only: the driver drops
	// tagged frames both ways.
	Vlan *int `json:"vlan,omitempty"`
}
