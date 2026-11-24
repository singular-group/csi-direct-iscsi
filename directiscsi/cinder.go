package directiscsi

import (
	"context"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/attachments"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/volumes"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/volumetypes"
)

// cinderAPI is the Cinder surface the hooks need beyond openstack.IOpenStack.
type cinderAPI interface {
	changeType(ctx context.Context, volumeID, newType string) error
	typeExists(ctx context.Context, name string) (bool, error)
	createAttachment(ctx context.Context, volumeID string, connector map[string]any) (*attachments.Attachment, error)
	completeAttachment(ctx context.Context, id string) error
	getAttachment(ctx context.Context, id string) (*attachments.Attachment, error)
	listAttachments(ctx context.Context, volumeID string) ([]attachments.Attachment, error)
	deleteAttachment(ctx context.Context, id string) error
}

type gopherCinder struct {
	client *gophercloud.ServiceClient
}

// mv returns a copy of the client pinned to a microversion.
func (g gopherCinder) mv(v string) *gophercloud.ServiceClient {
	c := *g.client
	c.Microversion = v
	return &c
}

func (g gopherCinder) changeType(ctx context.Context, volumeID, newType string) error {
	return volumes.ChangeType(ctx, g.client, volumeID, volumes.ChangeTypeOpts{
		NewType:         newType,
		MigrationPolicy: volumes.MigrationPolicyNever,
	}).ExtractErr()
}

func (g gopherCinder) typeExists(ctx context.Context, name string) (bool, error) {
	pages, err := volumetypes.List(g.client, volumetypes.ListOpts{}).AllPages(ctx)
	if err != nil {
		return false, err
	}
	types, err := volumetypes.ExtractVolumeTypes(pages)
	if err != nil {
		return false, err
	}
	for _, t := range types {
		if t.Name == name {
			return true, nil
		}
	}
	return false, nil
}

// attachmentCreate omits instance_uuid, which upstream's CreateOpts always
// sends and Cinder only accepts as a UUID.
type attachmentCreate struct {
	volumeID  string
	connector map[string]any
}

func (o attachmentCreate) ToAttachmentCreateMap() (map[string]any, error) {
	return map[string]any{"attachment": map[string]any{
		"volume_uuid": o.volumeID,
		"connector":   o.connector,
		"mode":        "rw",
	}}, nil
}

func (g gopherCinder) createAttachment(ctx context.Context, volumeID string, connector map[string]any) (*attachments.Attachment, error) {
	return attachments.Create(ctx, g.mv("3.54"), attachmentCreate{volumeID, connector}).Extract()
}

func (g gopherCinder) completeAttachment(ctx context.Context, id string) error {
	return attachments.Complete(ctx, g.mv("3.44"), id).ExtractErr()
}

func (g gopherCinder) getAttachment(ctx context.Context, id string) (*attachments.Attachment, error) {
	return attachments.Get(ctx, g.mv("3.27"), id).Extract()
}

func (g gopherCinder) listAttachments(ctx context.Context, volumeID string) ([]attachments.Attachment, error) {
	pages, err := attachments.List(g.mv("3.27"), attachments.ListOpts{VolumeID: volumeID}).AllPages(ctx)
	if err != nil {
		return nil, err
	}
	return attachments.ExtractAttachments(pages)
}

func (g gopherCinder) deleteAttachment(ctx context.Context, id string) error {
	return attachments.Delete(ctx, g.mv("3.27"), id).ExtractErr()
}
