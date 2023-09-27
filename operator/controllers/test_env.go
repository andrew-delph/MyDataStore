package controllers

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var testBinaries = flag.String("binaries", "hack/bin", "")

func ExpandPath(path ...string) string {
	if v, ok := os.LookupEnv("RUNFILES_DIR"); ok {
		prefix := []string{v, os.Getenv("TEST_WORKSPACE"), os.Getenv("BUILD_WORKSPACE_DIRECTORY")}
		return filepath.Join(append(prefix, path...)...)
	}

	// when not running with bazel
	return filepath.Join(append([]string{"."}, path...)...)
}

// PrependToPath prepends the supplied path to PATH
func PrependToPath(path ...string) {
	os.Setenv("PATH", fmt.Sprintf("%s:%s", filepath.Join(path...), os.Getenv("PATH")))
}

func TestEnv() *envtest.Environment {
	dir, err := os.Getwd()
	if err != nil {
		logrus.Fatal(err)
	}
	os.Setenv("KUBEBUILDER_ASSETS", dir)
	PrependToPath(dir)

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	return testEnv
}
