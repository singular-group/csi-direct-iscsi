// Copied unchanged from k8s.io/cloud-provider-openstack v1.36.0
// tests/sanity/cinder/fakemetadata.go (Apache License 2.0): upstream's fakes are
// unexported, and the sanity run below drives upstream's driver with them.

package sanity

import "k8s.io/cloud-provider-openstack/pkg/csi/cinder"

type fakemetadata struct {
}

// fake metadata

func (m *fakemetadata) GetInstanceID() (string, error) {
	return cinder.FakeInstanceID, nil
}

func (m *fakemetadata) GetAvailabilityZone() (string, error) {
	return cinder.FakeAvailability, nil
}
