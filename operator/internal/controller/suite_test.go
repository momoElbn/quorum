package controller

// The tests in this package run against envtest: a real kube-apiserver and
// etcd started on this machine, with our CRD installed. There are no
// controllers (no kube-controller-manager, no kubelet), so a StatefulSet
// never creates pods and nothing garbage-collects. Tests fake what those
// would do, e.g. by writing a StatefulSet's status by hand.
//
// Run with `make test`, which downloads the binaries first. Plain `go test`
// also works after `make setup-envtest`.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	quorumv1alpha1 "github.com/momoElbn/quorum/operator/api/v1alpha1"
)

var (
	k8sClient client.Client
	scheme    = runtime.NewScheme()
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run is separate from TestMain so the deferred Stop runs before os.Exit.
func run(m *testing.M) int {
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	// `make test` sets KUBEBUILDER_ASSETS. Otherwise use the binaries
	// `make setup-envtest` downloaded into bin/k8s.
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		dirs, _ := filepath.Glob(filepath.Join("..", "..", "bin", "k8s", "*"))
		if len(dirs) == 0 {
			fmt.Fprintln(os.Stderr, "envtest binaries not found: run `make setup-envtest` or `make test`")
			return 1
		}
		env.BinaryAssetsDirectory = dirs[0]
	}

	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start envtest:", err)
		return 1
	}
	defer env.Stop()

	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := quorumv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintln(os.Stderr, "create client:", err)
		return 1
	}
	return m.Run()
}

// ctx is the context for API calls in tests.
var ctx = context.Background()
