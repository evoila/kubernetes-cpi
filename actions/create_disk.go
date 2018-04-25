package actions

import (
	"fmt"
	"time"

	"github.com/evoila/kubernetes-cpi/cpi"
	"github.com/evoila/kubernetes-cpi/kubecluster"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type CreateDiskCloudProperties struct {
	Context string `json:"context"`
}

// DiskCreator simply creates a PersistentVolumeClaim. The attach process will
// turn the claim into a volume mounted into the pod.
type DiskCreator struct {
	ClientProvider    kubecluster.ClientProvider
	GUIDGeneratorFunc func() (string, error)
}

func (d *DiskCreator) CreateDisk(size uint, cloudProps CreateDiskCloudProperties, vmcid cpi.VMCID) (cpi.DiskCID, error) {
	diskID, err := d.GUIDGeneratorFunc()
	if err != nil {
		return "", err
	}

	volumeSize, err := resource.ParseQuantity(fmt.Sprintf("%dMi", size))
	if err != nil {
		return "", err
	}

	client, err := d.ClientProvider.New(cloudProps.Context)
	if err != nil {
		return "", err
	}

	_, err = client.PersistentVolumeClaims().Create(&v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "disk-" + diskID,
			Namespace: client.Namespace(),
			Labels: map[string]string{
				"bosh.cloudfoundry.org/disk-id": diskID,
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: volumeSize,
				},
			},
		},
	})
	if err != nil {
		return "", err
	}

	volume, err := client.PersistentVolumeClaims().Get("disk-"+diskID, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	for bound := volume.Status.Phase; bound != "Bound"; bound = volume.Status.Phase {
		time.Sleep(1 * time.Second)
		volume, err = client.PersistentVolumeClaims().Get("disk-"+diskID, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
	}

	return NewDiskCID(client.Context(), diskID), nil
}
