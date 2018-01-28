package actions

import (
	"net/http"

	"github.com/evoila/kubernetes-cpi/cpi"
	"github.com/evoila/kubernetes-cpi/kubecluster"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	core "k8s.io/client-go/kubernetes/typed/core/v1"
)

type DiskGetter struct {
	ClientProvider kubecluster.ClientProvider
}

func (d *DiskGetter) GetDisks(vmcid cpi.VMCID) ([]cpi.DiskCID, error) {
	context, agentID := ParseVMCID(vmcid)
	client, err := d.ClientProvider.New(context)
	if err != nil {
		return nil, err
	}

	pod, err := client.Pods().Get("agent-"+agentID, metav1.GetOptions{})
	if err != nil {
		if statusError, ok := err.(*errors.StatusError); ok {
			if statusError.Status().Code == http.StatusNotFound {
				return []cpi.DiskCID{}, nil
			}
		}
		return nil, err
	}

	diskIDs := []cpi.DiskCID{}
	for _, v := range pod.Spec.Volumes {
		pvc, err := getPVClaim(client.PersistentVolumeClaims(), v.VolumeSource)
		if err != nil && !isNotFoundStatusError(err) {
			return nil, err
		}

		if pvc == nil {
			continue
		}

		if diskID, ok := pvc.Labels["bosh.cloudfoundry.org/disk-id"]; ok {
			diskIDs = append(diskIDs, NewDiskCID(context, diskID))
		}
	}

	return diskIDs, nil
}

func getPVClaim(pvcClient core.PersistentVolumeClaimInterface, volumeSource v1.VolumeSource) (*v1.PersistentVolumeClaim, error) {
	if volumeSource.PersistentVolumeClaim != nil {
		return pvcClient.Get(volumeSource.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
	}
	return nil, nil
}

func isNotFoundStatusError(err error) bool {
	if statusErr, ok := err.(*errors.StatusError); ok {
		return statusErr.Status().Code == http.StatusNotFound
	}
	return false
}
