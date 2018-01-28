package actions

import (
	"github.com/evoila/kubernetes-cpi/cpi"
	"github.com/evoila/kubernetes-cpi/kubecluster"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DiskDeleter struct {
	ClientProvider kubecluster.ClientProvider
}

func (d *DiskDeleter) DeleteDisk(diskCID cpi.DiskCID) error {
	context, diskID := ParseDiskCID(diskCID)
	client, err := d.ClientProvider.New(context)
	if err != nil {
		return err
	}

	return client.PersistentVolumeClaims().Delete("disk-"+diskID, &metav1.DeleteOptions{GracePeriodSeconds: int64Ptr(0)})
}
