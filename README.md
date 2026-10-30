# dranet-macvtap

DRA driver (fork of [kubernetes-sigs/dranet](https://github.com/kubernetes-sigs/dranet))
that gives KubeVirt VMs macvtap secondary NICs on a host parent interface,
allocated through `resource.k8s.io/v1`.

Driver name: `macvtap.petasus.io`.

## How it works

- Every eligible host parent interface — physical NICs, bonds and VLAN
  sub-interfaces (Ethernet encapsulation, not loopback) — is auto-discovered
  and published as ONE shared ResourceSlice device (`allowMultipleAllocations:
  true`) with the attributes
  `macvtap.petasus.io/{nodeName,ifName,mtu,state,linkKind,deviceType}`
  (`nodeName` is the node the slice is published for, so a DeviceClass CEL
  can select a parent interface per node — CEL sees device attributes only)
  and the
  consumable capacity `macvtap.petasus.io/slots: 128` (request policy: every
  allocation consumes exactly 1 slot). There is no driver configuration;
  selection policy lives entirely in DeviceClass CEL over the published
  attributes. The node's links are rescanned periodically
  (`--rescan-interval`), so parent add/remove needs no driver restart.

- On `NodePrepareResources` the driver creates the per-claim macvtap child on
  the parent (host-side name `mvt` + 12 hex chars, hashed from device + claim
  + request — a shared parent backs several claims) in bridge mode and
  resolves its `/dev/tap<ifindex>` character device.

- Via NRI, the tap character device is injected into the pod's containers
  owned by uid/gid 107 (qemu in virt-launcher), and on `RunPodSandbox` the
  link is moved into the pod network namespace, renamed and configured from
  the claim's opaque config (dranet `NetworkConfig`):

  ```yaml
  config:
    - requests: ["nic"]
      opaque:
        driver: macvtap.petasus.io
        parameters:
          interface:
            name: pod0123456789a   # pod interface name KubeVirt expects
            hardwareAddr: "02:00:00:aa:bb:cc"
            mtu: 1500
  ```

- On pod stop the link is deleted; `NodeUnprepareResources` removes host-side
  leftovers of pods that never started. DHCP addressing is refused.

- The claim status reports each device with its allocation `shareID`.

KubeVirt consumes the device with the zero-code `macvtap` network binding
plugin (`domainAttachmentType: tap`) and `spec.networks[].resourceClaim`
(`NetworkDevicesWithDRA` feature gate).

## Deploy

```sh
kubectl apply -f deployments/dranet-macvtap.yaml
kubectl apply -f deployments/examples/bundang-10f-rtx.yaml   # example DeviceClass + claim
```

## Fork layout

The fork follows upstream `main`; the commits on top of it are the whole
difference, kept small so a sync is a plain rebase:

- `pkg/macvtap/**`: the parent inventory.
- `pkg/driver/carry.go`, `pkg/driver/macvtapdevice.go`: the driver
  additions. Upstream files call them through one-line hooks (the same ones
  as dranet-vfio's) and embed `carryState` / `CarryDeviceConfig`; nothing
  else in `pkg/driver` differs from upstream.
- `cmd/dranet-macvtap`: the entry point (`cmd/dranet` stays upstream's).
- `Dockerfile.petasus`, `.github/workflows/build-push.yaml`: the image
  `quay.io/edgestack/dranet-macvtap` (`main` → `dev`, `v*` tags, `latest` for the newest
  final release, and a `sync/*` branch → its slug). Upstream's own workflows are
  disabled in this repository.

To sync: `git fetch --no-tags upstream main`, then
`git rebase --onto upstream/main <previous upstream base> main`; conflicts
can only come from the hook lines.

The upstream dranet documentation below describes the inherited machinery.

---

# DRANET: DRA Kubernetes Network Driver

DRANET is a Kubernetes Network Driver that uses Dynamic Resource Allocation
(DRA) to deliver high-performance networking for demanding applications in
Kubernetes.

## Key Features

- **DRA Integration:** Leverages the power of Kubernetes' Dynamic Resource
  Allocation.
- **High-Performance Networking:** Designed for demanding workloads like AI/ML
  applications.
- **Simplified Management:** Easy to deploy and manage.
- **Enhanced Efficiency:** Optimizes resource utilization for improved overall
  performance.
- **Cluster-Wide Scalability:**  Effectively manages network resources across a
  large number of nodes for seamless operation in Kubernetes deployments.

Our research paper, **"[The Kubernetes Network Driver Model: A Composable Architecture for High-Performance Networking](/site/static/docs/kubernetes_network_driver_model_dranet_paper.pdf)"**, provides a deep dive into the DRANET model and its impact.

<p align="center">
<img src="site/static/images/nccl_all_gather_results.png" width="400" height="300">   <img src="site/static/images/nccl_all_reduce_results.png" width="400" height="300">
</p>

The key findings include:

- **Up to 60% Bandwidth Increase:** By enabling topology-aware scheduling of GPUs and NICs, DRANET boosts bus bandwidth by up to 59.6% for `all_gather` and 58.1% for `all_reduce` operations in distributed AI/ML workloads.
- **Operational Simplicity:** The paper demonstrates how the KND model used by DRANET drastically simplifies the management of high-performance hardware, replacing fragile, multi-component chains with a clean, composable architecture.

## How It Works

The DRANET driver communicates with the Kubelet through the [DRA
API](https://github.com/kubernetes/kubernetes/blob/3bec2450efd29787df0f27415de4e8049979654f/staging/src/k8s.io/kubelet/pkg/apis/dra/v1beta1/api.proto)
and with the Container Runtime via [NRI](https://github.com/containerd/nri).
This architectural approach ensures robust supportability and minimizes
complexity, making it fully compatible with existing CNI plugins in your
cluster.

Upon the creation of a Pod's network namespaces, the Container Runtime initiates
a GRPC call to DRANET via NRI to execute the necessary network configurations.

A more detailed diagram illustrating this process can be found in our
documentation: [How It
Works](https://dranet.sigs.k8s.io/docs/concepts/howitworks/).

## Quick Start

To get started with DRANET, your Kubernetes cluster needs to have [Dynamic
Resource Allocation (DRA)
enabled](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/).
DRA is stable since Kubernetes v1.35 and enabled by default. 

![](site/static/images/dranet.gif)

### Kubernetes Cluster with DRA

#### KIND

If you are using
[KIND](https://github.com/kubernetes-sigs/kind?tab=readme-ov-file#installation-and-usage),
you can create a cluster with the following configuration:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  image: kindest/node:v1.37.0
- role: worker
  image: kindest/node:v1.37.0
- role: worker
  image: kindest/node:v1.37.0
```

Then to create the cluster:

```sh
kind create cluster --config kind.yaml
```

#### Google Cloud (GKE)

For instructions on setting up DRA on GKE, refer to the official documentation:
[Set up Dynamic Resource
Allocation](https://cloud.google.com/kubernetes-engine/docs/how-to/set-up-dra)

### Installation

Install the latest stable version of DRANET using the provided manifest:

```sh
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/dranet/refs/heads/main/install.yaml
```

or using the Helm chart:

```sh
helm install -n kube-system dranet oci://registry.k8s.io/networking/charts/dranet --version v1.4.0
```

For available configuration options, see the [chart README](deployments/helm/dranet/README.md).

DRANET persists prepared pod device configuration in a local bbolt database so
NRI hooks can continue initialization after a driver restart. The default DB
path is `/var/run/dranet/dranet.db`, so deployment manifests must mount
`/var/run/dranet` as writable storage (hostPath with `DirectoryOrCreate` in the
provided manifests).

You can override the DB path with `--db-path`; set it to an empty
string to disable persistence and use in-memory state.

### How to Use It

Once DRANET is running, you can inspect the network interfaces and their
attributes published by the drivers. Users can then create `DeviceClasses`,
`ResourceClaims`, and/or `ResourceClaimTemplates` to schedule pods and allocate
network devices.

For examples of how to use DRANET with `DeviceClass` and `ResourceClaim` to
attach network interfaces to pods, please refer to the [Quick Start
guide](https://dranet.sigs.k8s.io/docs/quick-start).


## Contributing

We welcome your contributions! Please check out our [CONTRIBUTING.md](CONTRIBUTING.md) for more information.

## Further Reading

Explore more concepts and advanced topics:

* **Design:** Understand the architectural choices behind DRANET:
  [Design](https://dranet.sigs.k8s.io/docs/concepts/howitworks)
* **RDMA:** Learn about RDMA components in Linux and their interplay:
  [RDMA](https://dranet.sigs.k8s.io/docs/concepts/rdma)
* **References:** A list of relevant Kubernetes Enhancement Proposals (KEPs) and
  presentations:
  [References](https://dranet.sigs.k8s.io/docs/concepts/references)
