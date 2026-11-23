// Package directiscsi attaches multiattach Cinder volumes of the HPE MSA by
// logging the Kubernetes nodes in to the array themselves, instead of going
// through Nova. See README.md.
package directiscsi

import (
	"fmt"
	"os"
	"strings"

	gcfg "gopkg.in/gcfg.v1"
)

const driverName = "cinder.csi.openstack.org"

// Keys of the ControllerPublish context of a direct volume.
const (
	ctxDirect    = "directISCSI"
	ctxLUN       = "lun"
	ctxTargetIQN = "targetIQN"
)

// Config is the [DirectISCSI] section of the --cloud-config files.
type Config struct {
	InitiatorPrefix string `gcfg:"initiator-prefix"`
	TargetIQN       string `gcfg:"target-iqn"`
	StorageNetwork  string `gcfg:"storage-network"`
}

// readConfig reads [DirectISCSI] from the files in order, later files winning,
// like upstream reads the rest of them. ok is false when no file sets target-iqn.
func readConfig(paths []string) (cfg Config, ok bool, err error) {
	var file struct{ DirectISCSI Config }
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return cfg, false, err
		}
		// Every other section is upstream's, which gcfg reports as a warning;
		// an unknown key of ours is a typo and fails.
		err = gcfg.ReadInto(&file, f)
		f.Close()
		if fatal := gcfg.FatalOnly(err); fatal != nil {
			return cfg, false, fmt.Errorf("%s: %w", p, fatal)
		}
		if err != nil && strings.Contains(strings.ToLower(err.Error()), `section "directiscsi", variable`) {
			return cfg, false, fmt.Errorf("%s: unknown key in [DirectISCSI]: %v", p, err)
		}
	}
	cfg = file.DirectISCSI
	if cfg.TargetIQN == "" {
		return cfg, false, nil
	}
	if cfg.InitiatorPrefix == "" {
		return cfg, false, fmt.Errorf("[DirectISCSI] initiator-prefix is required with target-iqn")
	}
	return cfg, true, nil
}

// IsDirect reports whether ControllerPublish attached the volume directly.
func IsDirect(publishContext map[string]string) bool {
	return publishContext[ctxDirect] == "true"
}
