package directiscsi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/attachments"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/volumes"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/servers"
	"google.golang.org/grpc/codes"

	"k8s.io/cloud-provider-openstack/pkg/csi/cinder/openstack"
	cpoerrors "k8s.io/cloud-provider-openstack/pkg/util/errors"
)

const testIQN = "iqn.1986-03.com.hp:storage.msa2040.0123456789"

// fakeCloud is Nova + the IOpenStack part of Cinder. Any method it does not
// define panics through the nil embedded interface: a Nova attach or detach
// on a direct volume fails the test.
type fakeCloud struct {
	openstack.IOpenStack
	c         *fakeCinder
	servers   map[string]*servers.Server
	serverErr error
}

func (f *fakeCloud) GetVolume(_ context.Context, id string) (*volumes.Volume, error) {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	v, ok := f.c.vols[id]
	if !ok {
		return nil, cpoerrors.ErrNotFound
	}
	f.c.tick(v)
	cp := *v
	cp.Attachments = append([]volumes.Attachment(nil), v.Attachments...)
	return &cp, nil
}

func (f *fakeCloud) GetInstanceByID(_ context.Context, id string) (*servers.Server, error) {
	if f.serverErr != nil {
		return nil, f.serverErr
	}
	s, ok := f.servers[id]
	if !ok {
		return nil, cpoerrors.ErrNotFound
	}
	return s, nil
}

func (f *fakeCloud) WaitVolumeTargetStatus(_ context.Context, id string, want []string) error {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	if v := f.c.vols[id]; v.Status != want[0] {
		return fmt.Errorf("volume %s is %s", id, v.Status)
	}
	return nil
}

type fakeAttachment struct {
	id, volume, host, status string
	lun                      int
}

// fakeCinder simulates the volume and attachment state the hooks drive.
type fakeCinder struct {
	mu          sync.Mutex
	vols        map[string]*volumes.Volume
	atts        []*fakeAttachment
	types       map[string]bool
	nextLUN     int
	retypeFails bool
	failHost    string
	calls       []string
	active      int
	maxActive   int
	retyping    map[string]string
}

func newFakeCinder() *fakeCinder {
	return &fakeCinder{vols: map[string]*volumes.Volume{}, types: map[string]bool{"msa-fast-multiattach": true, "msa-slow-multiattach": true}, nextLUN: 1, retyping: map[string]string{}}
}

func (c *fakeCinder) record(s string) {
	c.calls = append(c.calls, s)
}

// tick finishes a retype the way cinder-volume does, one poll later.
func (c *fakeCinder) tick(v *volumes.Volume) {
	target, ok := c.retyping[v.ID]
	if !ok {
		return
	}
	delete(c.retyping, v.ID)
	v.Status = "available"
	// Cinder sets the flag before the retype runs and leaves it on a failure.
	v.Multiattach = true
	if !c.retypeFails {
		v.VolumeType = target
	}
}

func (c *fakeCinder) changeType(_ context.Context, id, newType string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("retype " + id + " " + newType)
	v := c.vols[id]
	if v.Status != "available" {
		return fmt.Errorf("volume %s is %s", id, v.Status)
	}
	v.Status = "retyping"
	c.retyping[id] = newType
	return nil
}

func (c *fakeCinder) typeExists(_ context.Context, name string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("types")
	return c.types[name], nil
}

func (c *fakeCinder) createAttachment(_ context.Context, volumeID string, connector map[string]any) (*attachments.Attachment, error) {
	c.mu.Lock()
	c.active++
	c.maxActive = max(c.maxActive, c.active)
	c.mu.Unlock()
	time.Sleep(2 * time.Millisecond) // let concurrent publishes overlap if the semaphore allowed it
	c.mu.Lock()
	defer func() { c.active--; c.mu.Unlock() }()
	host := connector["host"].(string)
	b, _ := json.Marshal(connector)
	c.record("create " + volumeID + " " + string(b))
	if host == c.failHost {
		return nil, errors.New("array busy")
	}
	v := c.vols[volumeID]
	if !v.Multiattach && len(v.Attachments) > 0 || v.Status != "available" && v.Status != "in-use" {
		return nil, fmt.Errorf("volume %s is %s", volumeID, v.Status)
	}
	a := &fakeAttachment{id: fmt.Sprintf("att-%d", len(c.atts)+1), volume: volumeID, host: host, status: "attaching", lun: c.nextLUN}
	c.nextLUN++
	c.atts = append(c.atts, a)
	v.Status = "attaching"
	return &attachments.Attachment{ID: a.id, ConnectionInfo: map[string]any{"driver_volume_type": "iscsi", "target_lun": float64(a.lun), "target_iqn": testIQN}}, nil
}

func (c *fakeCinder) completeAttachment(_ context.Context, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("complete " + id)
	for _, a := range c.atts {
		if a.id == id {
			a.status = "attached"
			v := c.vols[a.volume]
			v.Attachments = append(v.Attachments, volumes.Attachment{AttachmentID: a.id, HostName: a.host, VolumeID: a.volume})
			v.Status = "in-use"
			return nil
		}
	}
	return cpoerrors.ErrNotFound
}

func (c *fakeCinder) getAttachment(_ context.Context, id string) (*attachments.Attachment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("get " + id)
	for _, a := range c.atts {
		if a.id == id {
			return &attachments.Attachment{ID: a.id, Status: a.status, ConnectionInfo: map[string]any{"target_lun": float64(a.lun)}}, nil
		}
	}
	return nil, cpoerrors.ErrNotFound
}

func (c *fakeCinder) listAttachments(_ context.Context, volumeID string) ([]attachments.Attachment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("list " + volumeID)
	var out []attachments.Attachment
	for _, a := range c.atts {
		if a.volume == volumeID {
			out = append(out, attachments.Attachment{ID: a.id, VolumeID: a.volume, Status: a.status})
		}
	}
	return out, nil
}

func (c *fakeCinder) deleteAttachment(_ context.Context, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("delete " + id)
	for i, a := range c.atts {
		if a.id == id {
			c.atts = append(c.atts[:i], c.atts[i+1:]...)
			v := c.vols[a.volume]
			var keep []volumes.Attachment
			for _, va := range v.Attachments {
				if va.AttachmentID != id {
					keep = append(keep, va)
				}
			}
			v.Attachments = keep
			if len(keep) == 0 {
				v.Status = "available"
			}
			return nil
		}
	}
	return cpoerrors.ErrNotFound
}

func (c *fakeCinder) callsWith(prefix string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, s := range c.calls {
		if strings.HasPrefix(s, prefix) {
			out = append(out, s)
		}
	}
	return out
}

type fakeNodes map[string]string

func (f fakeNodes) nodes() map[string]string {
	m := map[string]string{}
	for k, v := range f {
		m[k] = v
	}
	return m
}

func withStorage(nets ...string) *servers.Server {
	addrs := map[string]any{"k8s-net": []any{map[string]any{"addr": "192.0.2.51"}}}
	for _, n := range nets {
		addrs[n] = []any{map[string]any{"addr": "198.51.100.51"}}
	}
	return &servers.Server{Addresses: addrs}
}

type testEnv struct {
	c     *Controller
	cloud *fakeCloud
	cin   *fakeCinder
	nodes fakeNodes
}

// newTestEnv: three nodes, all rolled with the storage NIC.
func newTestEnv(t *testing.T) *testEnv {
	cin := newFakeCinder()
	e := &testEnv{
		cin:   cin,
		nodes: fakeNodes{"node-1": "uuid-1", "node-2": "uuid-2", "node-3": "uuid-3"},
	}
	e.cloud = &fakeCloud{c: cin, servers: map[string]*servers.Server{
		"uuid-1": withStorage("k8s-storage"), "uuid-2": withStorage("k8s-storage"), "uuid-3": withStorage("k8s-storage"),
	}}
	cfg := Config{InitiatorPrefix: "iqn.2005-01.com.example:", TargetIQN: testIQN, StorageNetwork: "k8s-storage"}
	e.c = newController(cfg, e.nodes, func(openstack.IOpenStack) (cinderAPI, bool) { return cin, true })
	e.c.pollInterval = time.Millisecond
	e.c.retypeTimeout = time.Second
	controller = e.c
	t.Cleanup(func() { controller = nil })
	return e
}

func (e *testEnv) vol(id, typ, status string, multi bool, atts ...volumes.Attachment) {
	e.cin.vols[id] = &volumes.Volume{ID: id, VolumeType: typ, Status: status, Multiattach: multi, Attachments: atts}
}

func TestCreateVolumeType(t *testing.T) {
	ctx := context.Background()
	e := newTestEnv(t)
	if got := VolumeTypeForCreate(ctx, e.cloud, "msa-slow"); got != "msa-slow-multiattach" {
		t.Fatalf("ready cluster: %s", got)
	}
	e.cin.types["msa-fast-multiattach"] = false
	if got := VolumeTypeForCreate(ctx, e.cloud, "msa-fast"); got != "msa-fast" {
		t.Fatalf("multiattach type missing in Cinder: %s", got)
	}
}

func TestCreateVolumeTypeNotReady(t *testing.T) {
	ctx := context.Background()
	e := newTestEnv(t)
	e.cloud.servers["uuid-3"] = withStorage() // master-03 not rolled yet
	if got := VolumeTypeForCreate(ctx, e.cloud, "msa-slow"); got != "msa-slow" {
		t.Fatalf("mixed cluster: %s", got)
	}
	e2 := newTestEnv(t)
	e2.cloud.serverErr = errors.New("nova down")
	if got := VolumeTypeForCreate(ctx, e2.cloud, "msa-slow"); got != "msa-slow" {
		t.Fatalf("Nova error: %s", got)
	}
	e3 := newTestEnv(t)
	for k := range e3.nodes {
		delete(e3.nodes, k)
	}
	if got := VolumeTypeForCreate(ctx, e3.cloud, "msa-slow"); got != "msa-slow" {
		t.Fatalf("no CSINodes: %s", got)
	}
}

func TestPublishNewDirectVolumeMapsEveryNodeSelfFirst(t *testing.T) {
	e := newTestEnv(t)
	e.vol("v1", "msa-slow-multiattach", "available", true)
	resp, handled, err := ControllerPublish(context.Background(), e.cloud, "v1", "uuid-2")
	if err != nil || !handled {
		t.Fatalf("%v %v", handled, err)
	}
	creates := e.cin.callsWith("create ")
	if len(creates) != 3 || !strings.Contains(creates[0], `"host":"node-2"`) || !strings.Contains(creates[1], `"host":"node-1"`) || !strings.Contains(creates[2], `"host":"node-3"`) {
		t.Fatalf("creates %v, want master-02 first, then the others in order", creates)
	}
	for _, s := range []string{`"initiator":"iqn.2005-01.com.example:node-2"`, `"multipath":true`, `"do_local_attach":false`, `"os_type":"linux2"`, `"platform":"x86_64"`} {
		if !strings.Contains(creates[0], s) {
			t.Fatalf("connector %s lacks %s", creates[0], s)
		}
	}
	if got := e.cin.callsWith("complete "); len(got) != 3 {
		t.Fatalf("completes %v", got)
	}
	want := map[string]string{ctxDirect: "true", ctxLUN: "1", ctxTargetIQN: testIQN}
	if fmt.Sprint(resp.PublishContext) != fmt.Sprint(want) {
		t.Fatalf("publish context %v, want %v", resp.PublishContext, want)
	}
}

func TestPublishMoveOnlyReadsCinder(t *testing.T) {
	e := newTestEnv(t)
	e.vol("v1", "msa-slow-multiattach", "available", true)
	ctx := context.Background()
	if _, _, err := ControllerPublish(ctx, e.cloud, "v1", "uuid-1"); err != nil {
		t.Fatal(err)
	}
	if direct, err := ControllerUnpublish(ctx, e.cloud, "v1"); !direct || err != nil {
		t.Fatalf("unpublish %v %v", direct, err)
	}
	e.cin.calls = nil
	resp, _, err := ControllerPublish(ctx, e.cloud, "v1", "uuid-3")
	if err != nil {
		t.Fatal(err)
	}
	if resp.PublishContext[ctxLUN] != "3" {
		t.Fatalf("LUN %s, want master-03's", resp.PublishContext[ctxLUN])
	}
	for _, c := range e.cin.calls {
		if !strings.HasPrefix(c, "get ") {
			t.Fatalf("Cinder changed on a move: %v", e.cin.calls)
		}
	}
}

func TestPublishConvertsAnAvailableLegacyVolume(t *testing.T) {
	e := newTestEnv(t)
	e.vol("v1", "msa-fast", "available", false)
	resp, handled, err := ControllerPublish(context.Background(), e.cloud, "v1", "uuid-1")
	if err != nil || !handled || !IsDirect(resp.PublishContext) {
		t.Fatalf("%v %v %v", resp, handled, err)
	}
	if got := e.cin.callsWith("retype "); len(got) != 1 || got[0] != "retype v1 msa-fast-multiattach" {
		t.Fatalf("retypes %v", got)
	}
	v := e.cin.vols["v1"]
	if v.VolumeType != "msa-fast-multiattach" || !v.Multiattach || len(v.Attachments) != 3 {
		t.Fatalf("volume after convert %+v", v)
	}
}

func TestPublishRetypeFailureLeavesTheVolumeLegacy(t *testing.T) {
	e := newTestEnv(t)
	e.cin.retypeFails = true
	e.vol("v1", "msa-fast", "available", false)
	_, handled, err := ControllerPublish(context.Background(), e.cloud, "v1", "uuid-1")
	if !handled || code(err) != codes.Unavailable {
		t.Fatalf("%v %v, want Unavailable", handled, err)
	}
	if len(e.cin.callsWith("create ")) != 0 || e.cin.vols["v1"].VolumeType != "msa-fast" {
		t.Fatal("attached or retyped after a failed retype")
	}
	// The attacher's retry converts it, multiattach flag left set or not.
	e.cin.retypeFails = false
	resp, handled, err := ControllerPublish(context.Background(), e.cloud, "v1", "uuid-1")
	if err != nil || !handled || !IsDirect(resp.PublishContext) {
		t.Fatalf("retry: %v %v %v", resp, handled, err)
	}
	if v := e.cin.vols["v1"]; v.VolumeType != "msa-fast-multiattach" || len(v.Attachments) != 3 {
		t.Fatalf("volume after the retry %+v", v)
	}
}

func TestPublishLegacyDecisionTable(t *testing.T) {
	ctx := context.Background()
	nova := volumes.Attachment{ServerID: "uuid-1", AttachmentID: "nova-att"}
	for _, tc := range []struct {
		name    string
		setup   func(e *testEnv)
		node    string
		handled bool
		code    codes.Code
	}{
		{"in-use on this node: upstream republish", func(e *testEnv) { e.vol("v", "msa-slow", "in-use", false, nova) }, "uuid-1", false, codes.OK},
		{"in-use elsewhere: wait for the detach", func(e *testEnv) { e.vol("v", "msa-slow", "in-use", false, nova) }, "uuid-2", true, codes.FailedPrecondition},
		{"detaching: retry", func(e *testEnv) { e.vol("v", "msa-slow", "detaching", false, nova) }, "uuid-2", true, codes.Unavailable},
		{"cluster not ready: upstream", func(e *testEnv) {
			e.vol("v", "msa-slow", "available", false)
			e.cloud.servers["uuid-3"] = withStorage()
		}, "uuid-1", false, codes.OK},
		{"multiattach type missing: upstream", func(e *testEnv) {
			e.vol("v", "msa-slow", "available", false)
			e.cin.types["msa-slow-multiattach"] = false
		}, "uuid-1", false, codes.OK},
		{"Nova down while checking the gate: retry", func(e *testEnv) {
			e.vol("v", "msa-slow", "available", false)
			e.cloud.serverErr = errors.New("nova down")
		}, "uuid-1", true, codes.Unavailable},
		{"direct volume on a node not rolled yet: still direct", func(e *testEnv) {
			e.vol("v", "msa-slow-multiattach", "available", true)
			e.cloud.servers["uuid-1"] = withStorage()
		}, "uuid-1", true, codes.OK},
		{"node without CSINode: retry", func(e *testEnv) { e.vol("v", "msa-slow-multiattach", "available", true) }, "uuid-9", true, codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEnv(t)
			tc.setup(e)
			_, handled, err := ControllerPublish(ctx, e.cloud, "v", tc.node)
			if handled != tc.handled || codeOf(err) != tc.code {
				t.Fatalf("handled %v err %v, want %v %v", handled, err, tc.handled, tc.code)
			}
			if got := len(e.cin.callsWith("retype ")); got != 0 {
				t.Fatalf("retypes %d", got)
			}
		})
	}
}

func codeOf(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	return code(err)
}

func TestPublishOtherNodeFailureDoesNotBlock(t *testing.T) {
	e := newTestEnv(t)
	e.cin.failHost = "node-3"
	e.vol("v1", "msa-slow-multiattach", "available", true)
	if _, _, err := ControllerPublish(context.Background(), e.cloud, "v1", "uuid-1"); err != nil {
		t.Fatal(err)
	}
	e.cin.failHost = "node-1"
	e.vol("v2", "msa-slow-multiattach", "available", true)
	if _, _, err := ControllerPublish(context.Background(), e.cloud, "v2", "uuid-1"); code(err) != codes.Unavailable {
		t.Fatalf("own attachment failed: %v, want Unavailable", err)
	}
	// master-03 is retried by the next publish of v1, anywhere.
	e.cin.failHost = ""
	e.cin.calls = nil
	if _, _, err := ControllerPublish(context.Background(), e.cloud, "v1", "uuid-2"); err != nil {
		t.Fatal(err)
	}
	if got := e.cin.callsWith("create "); len(got) != 1 || !strings.Contains(got[0], "node-3") {
		t.Fatalf("creates %v, want only master-03's", got)
	}
}

func TestPublishResumesAfterACrash(t *testing.T) {
	e := newTestEnv(t)
	e.vol("v1", "msa-slow-multiattach", "attaching", true)
	// Crashed between create and complete for master-01, and while reserving for master-02.
	e.cin.atts = []*fakeAttachment{
		{id: "att-a", volume: "v1", host: "node-1", status: "attaching", lun: 5},
		{id: "att-b", volume: "v1", host: "node-2", status: "reserved"},
	}
	resp, _, err := ControllerPublish(context.Background(), e.cloud, "v1", "uuid-1")
	if err != nil {
		t.Fatal(err)
	}
	if got := e.cin.callsWith("complete att-a"); len(got) != 1 {
		t.Fatalf("crashed attachment not completed: %v", e.cin.calls)
	}
	if got := e.cin.callsWith("delete att-b"); len(got) != 1 {
		t.Fatalf("reserved attachment not removed: %v", e.cin.calls)
	}
	if resp.PublishContext[ctxLUN] != "5" {
		t.Fatalf("LUN %s, want the resumed attachment's", resp.PublishContext[ctxLUN])
	}
	if got := e.cin.callsWith("create "); len(got) != 2 {
		t.Fatalf("creates %v, want master-02 and master-03", got)
	}
}

func TestPublishFinishesALeftoverAttachmentBeforeMappingItsNode(t *testing.T) {
	e := newTestEnv(t)
	// In use on master-01; master-02's complete never ran.
	e.vol("v1", "msa-slow-multiattach", "in-use", true, volumes.Attachment{AttachmentID: "att-a", HostName: "node-1", VolumeID: "v1"})
	e.cin.atts = []*fakeAttachment{
		{id: "att-a", volume: "v1", host: "node-1", status: "attached", lun: 5},
		{id: "att-b", volume: "v1", host: "node-2", status: "attaching", lun: 6},
	}
	if _, _, err := ControllerPublish(context.Background(), e.cloud, "v1", "uuid-1"); err != nil {
		t.Fatal(err)
	}
	if got := e.cin.callsWith("complete att-b"); len(got) != 1 {
		t.Fatalf("leftover attachment not completed: %v", e.cin.calls)
	}
	if got := e.cin.callsWith("create "); len(got) != 1 || !strings.Contains(got[0], "node-3") {
		t.Fatalf("creates %v, want only master-03's", got)
	}
}

func TestPublishRefusesHiddenHosts(t *testing.T) {
	e := newTestEnv(t)
	e.vol("v1", "msa-slow-multiattach", "in-use", true, volumes.Attachment{AttachmentID: "att-x"})
	if _, _, err := ControllerPublish(context.Background(), e.cloud, "v1", "uuid-1"); code(err) != codes.FailedPrecondition {
		t.Fatalf("err %v, want FailedPrecondition", err)
	}
}

func TestOneArrayOperationAtATime(t *testing.T) {
	e := newTestEnv(t)
	for i := 0; i < 8; i++ {
		e.vol(fmt.Sprintf("v%d", i), "msa-slow", "available", false)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("v%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := ControllerPublish(context.Background(), e.cloud, id, "uuid-1"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if e.cin.maxActive != 1 {
		t.Fatalf("%d attachment creations ran at once", e.cin.maxActive)
	}
	if got := len(e.cin.callsWith("create ")); got != 24 {
		t.Fatalf("%d creates, want 8 volumes x 3 nodes", got)
	}
}

func TestUnpublishOfALegacyVolumeGoesUpstream(t *testing.T) {
	e := newTestEnv(t)
	e.vol("legacy", "msa-slow", "in-use", false)
	if direct, err := ControllerUnpublish(context.Background(), e.cloud, "legacy"); direct || err != nil {
		t.Fatalf("legacy: %v %v", direct, err)
	}
}

func TestPublishedNodeIDs(t *testing.T) {
	atts := []volumes.Attachment{{ServerID: "uuid-nova"}, {HostName: "node-2"}, {HostName: "removed-node"}, {}}
	controller = nil
	if got := PublishedNodeIDs(atts); fmt.Sprint(got) != fmt.Sprint([]string{"uuid-nova", "", "", ""}) {
		t.Fatalf("without config %q, want upstream's server IDs", got)
	}
	if got := PublishedNodeIDs(nil); got == nil || len(got) != 0 {
		t.Fatalf("no attachments: %#v", got)
	}
	newTestEnv(t)
	if got := PublishedNodeIDs(atts); fmt.Sprint(got) != fmt.Sprint([]string{"uuid-nova", "uuid-2", ""}) {
		t.Fatalf("%q", got)
	}
}

func TestDeleteDirectVolumeRemovesAttachments(t *testing.T) {
	ctx := context.Background()
	e := newTestEnv(t)
	e.vol("v1", "msa-slow-multiattach", "available", true)
	if _, _, err := ControllerPublish(ctx, e.cloud, "v1", "uuid-1"); err != nil {
		t.Fatal(err)
	}
	if err := BeforeDeleteVolume(ctx, e.cloud, "v1"); err != nil {
		t.Fatal(err)
	}
	if got := e.cin.callsWith("delete "); len(got) != 3 || e.cin.vols["v1"].Status != "available" {
		t.Fatalf("deletes %v, status %s", got, e.cin.vols["v1"].Status)
	}
	e.vol("legacy", "msa-slow", "available", false)
	e.cin.calls = nil
	if err := BeforeDeleteVolume(ctx, e.cloud, "legacy"); err != nil || len(e.cin.calls) != 0 {
		t.Fatalf("legacy: %v %v", err, e.cin.calls)
	}
	if err := BeforeDeleteVolume(ctx, e.cloud, "gone"); err != nil {
		t.Fatal(err)
	}
}

func TestTargetLUN(t *testing.T) {
	for _, tc := range []struct {
		info map[string]any
		lun  int
		ok   bool
	}{
		{map[string]any{"target_lun": float64(12)}, 12, true},
		{map[string]any{"target_luns": []any{1.0}}, 0, false},
		{map[string]any{"target_lun": float64(-1)}, -1, false},
	} {
		lun, ok := targetLUN(tc.info)
		if ok != tc.ok || ok && lun != tc.lun {
			t.Fatalf("%v: %d %v", tc.info, lun, ok)
		}
	}
}

func TestAttachmentCreateBodyHasNoInstance(t *testing.T) {
	m, _ := attachmentCreate{"v1", map[string]any{"host": "node-1"}}.ToAttachmentCreateMap()
	b, _ := json.Marshal(m)
	if strings.Contains(string(b), "instance") || !strings.Contains(string(b), `"mode":"rw"`) || !strings.Contains(string(b), `"volume_uuid":"v1"`) {
		t.Fatalf("body %s", b)
	}
}
