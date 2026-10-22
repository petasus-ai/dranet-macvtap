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

// dranet-macvtap runs the upstream driver as macvtap.petasus.io with the
// macvtap parent inventory in place of the netdev one; cmd/dranet stays
// upstream's.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/ext"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"sigs.k8s.io/dranet/pkg/driver"
	"sigs.k8s.io/dranet/pkg/features"
	"sigs.k8s.io/dranet/pkg/macvtap"

	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/klog/v2"
)

const (
	driverName = "macvtap.petasus.io"
)

var (
	hostnameOverride string
	kubeconfig       string
	bindAddress      string
	celExpression    string
	dbPath           string
	rescanInterval   time.Duration
	featureGates     string

	kubeletRootDir string

	ready atomic.Bool
)

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	flag.StringVar(&bindAddress, "bind-address", ":9177", "The IP address and port for the metrics and healthz server to serve on")
	flag.StringVar(&hostnameOverride, "hostname-override", "", "If non-empty, will be used as the name of the Node the driver runs on. If unset, the node name is assumed to be the same as the node's hostname.")
	flag.StringVar(&celExpression, "filter", "", "CEL expression to filter network interface attributes (v1.DeviceAttribute).")
	flag.StringVar(&dbPath, "db-path", filepath.Join("/var/run/dranet-macvtap", "state.db"), "Path to the persistent bbolt database file. Set to an empty string to disable persistence and use in-memory state.")
	flag.DurationVar(&rescanInterval, "rescan-interval", 30*time.Second, "How often the node's links are rescanned and republished.")
	flag.StringVar(&kubeletRootDir, "kubelet-root-dir", "/var/lib/kubelet", "The kubelet data directory (its --root-dir). The driver's registration socket lives under <dir>/plugins_registry and its dra.sock under <dir>/plugins/<driver-name>. Set this to match the kubelet --root-dir on clusters that relocate it.")
	flag.StringVar(&featureGates, "feature-gates", "", "A set of key=value pairs that describe feature gates for alpha/experimental features.")

	flag.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage: dranet-macvtap [options]\n\n")
		flag.PrintDefaults()
	}
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	if featureGates != "" {
		if err := features.DefaultMutableFeatureGate.Set(featureGates); err != nil {
			klog.Fatalf("Failed to set feature gates: %v", err)
		}
	}

	printVersion()
	flag.VisitAll(func(f *flag.Flag) {
		klog.Infof("FLAG: --%s=%q", f.Name, f.Value)
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	})
	mux.Handle("/metrics", promhttp.Handler())
	// Bind before the driver starts, so a port still held by the previous pod
	// stops this process before it accepts any claim; kubelet restarts it.
	if _, err := startMetricsServer(bindAddress, mux, func(err error) {
		klog.Fatalf("metrics/healthz server on %s stopped: %v", bindAddress, err)
	}); err != nil {
		klog.Fatalf("Failed to start the metrics/healthz server: %v", err)
	}

	var config *rest.Config
	var err error
	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		klog.Fatalf("can not create client-go configuration: %v", err)
	}

	// use protobuf for better performance at scale
	// https://kubernetes.io/docs/reference/using-api/api-concepts/#alternate-representations-of-resources
	config.AcceptContentTypes = "application/vnd.kubernetes.protobuf,application/json"
	config.ContentType = "application/vnd.kubernetes.protobuf"

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("can not create client-go client: %v", err)
	}

	nodeName, err := nodeutil.GetHostname(hostnameOverride)
	if err != nil {
		klog.Fatalf("can not obtain the node name, use the hostname-override flag if you want to set it to a specific value: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Trap signals for graceful shutdown.
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// If dbPath is empty the store is in-memory only; otherwise it is backed
	// by a bbolt checkpoint at that path.
	store, err := driver.NewPodConfigStore(dbPath)
	if err != nil {
		klog.Fatalf("failed to initialize pod config store: %v", err)
	}
	defer store.Close()

	opts := []driver.Option{
		driver.WithKubeletRootDir(kubeletRootDir),
	}
	if celExpression != "" {
		env, err := cel.NewEnv(
			ext.NativeTypes(
				reflect.ValueOf(resourcev1.DeviceAttribute{}),
			),
			cel.Variable("attributes", cel.MapType(cel.StringType, cel.ObjectType("v1.DeviceAttribute"))),
		)
		if err != nil {
			klog.Fatalf("error creating CEL environment: %v", err)
		}
		ast, issues := env.Compile(celExpression)
		if issues != nil && issues.Err() != nil {
			klog.Fatalf("type-check error: %s", issues.Err())
		}
		prg, err := env.Program(ast)
		if err != nil {
			klog.Fatalf("program construction error: %s", err)
		}
		opts = append(opts, driver.WithFilter(prg))
	}

	db := macvtap.New(nodeName, macvtap.WithRescanInterval(rescanInterval))
	opts = append(opts, driver.WithInventory(db))
	macvtapDriver, err := driver.Start(ctx, driverName, clientset, nodeName, store, opts...)
	if err != nil {
		klog.Fatalf("driver failed to start: %v", err)
	}
	defer macvtapDriver.Stop(cancel)

	ready.Store(true)
	klog.Info("driver started")

	select {
	case sig := <-signalCh:
		klog.Infof("Received shutdown signal: %q. Initiating graceful shutdown...", sig)
	case <-ctx.Done():
		klog.Info("Context cancelled. Initiating graceful shutdown...")
	}
}

func printVersion() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	var vcsRevision, vcsTime string
	for _, f := range info.Settings {
		switch f.Key {
		case "vcs.revision":
			vcsRevision = f.Value
		case "vcs.time":
			vcsTime = f.Value
		}
	}
	klog.Infof("dranet-macvtap go %s build: %s time: %s", info.GoVersion, vcsRevision, vcsTime)
}

// startMetricsServer binds addr and serves handler on it in the background.
// A bind failure is returned to the caller. Any error that stops the server,
// other than http.ErrServerClosed from a Shutdown, is passed to onServeError.
func startMetricsServer(addr string, handler http.Handler, onServeError func(error)) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			onServeError(err)
		}
	}()
	return ln, nil
}
