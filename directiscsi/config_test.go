package directiscsi

import (
	"os"
	"path/filepath"
	"testing"

	"k8s.io/cloud-provider-openstack/pkg/csi/cinder/openstack"
)

const (
	cloudConf = `[Global]
auth-url = https://keystone.example:5000/v3
username = csi
domain-name = Default
tenant-name = admin
region = RegionOne
`
	baseConf = `[DirectISCSI]
initiator-prefix = iqn.2005-01.com.example:
target-iqn = iqn.1986-03.com.hp:storage.msa2040.0123456789
`
	envConf = `[DirectISCSI]
storage-network = k8s-storage
`
	blockStorageConf = `[BlockStorage]
rescan-on-resize = true
`
)

func confFiles(t *testing.T, contents ...string) []string {
	dir := t.TempDir()
	var paths []string
	for i, c := range contents {
		p := filepath.Join(dir, string(rune('a'+i))+".conf")
		if err := os.WriteFile(p, []byte(c), 0o600); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}
	return paths
}

// The deployed --cloud-config list: Secret, [BlockStorage], base and overlay [DirectISCSI].
func TestReadConfigMergesFilesInOrder(t *testing.T) {
	files := confFiles(t, cloudConf, blockStorageConf, baseConf, envConf)
	cfg, ok, err := readConfig(files)
	if err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	want := Config{
		InitiatorPrefix: "iqn.2005-01.com.example:",
		TargetIQN:       "iqn.1986-03.com.hp:storage.msa2040.0123456789",
		StorageNetwork:  "k8s-storage",
	}
	if cfg != want {
		t.Fatalf("%+v, want %+v", cfg, want)
	}

	// Upstream reads the same list: it must ignore [DirectISCSI] and still get its own sections.
	up, err := openstack.GetConfigFromFiles(files)
	if err != nil {
		t.Fatalf("upstream config: %v", err)
	}
	if up.Global[""] == nil || up.Global[""].Username != "csi" || !up.BlockStorage.RescanOnResize {
		t.Fatalf("upstream config %+v", up)
	}
}

func TestReadConfigRejects(t *testing.T) {
	for _, c := range []string{
		"[DirectISCSI]\ntarget-iqn = iqn.x\n",
		"[DirectISCSI]\ninitiator-prefix = p:\ntarget-iqn = iqn.x\nno-such-key = 1\n",
	} {
		if _, _, err := readConfig(confFiles(t, c)); err == nil {
			t.Fatalf("accepted %q", c)
		}
	}
}
