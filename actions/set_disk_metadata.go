package actions

import (
	"encoding/json"
	"strings"

	"github.com/evoila/kubernetes-cpi/cpi"
	"github.com/evoila/kubernetes-cpi/kubecluster"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/validation"
)

type DiskMetadataSetter struct {
	ClientProvider kubecluster.ClientProvider
}

func (v *DiskMetadataSetter) SetDiskMetadata(diskcid cpi.DiskCID, metadata map[string]string) error {
	context, diskID := ParseDiskCID(diskcid)

	client, err := v.ClientProvider.New(context)
	if err != nil {
		return err
	}

	coreClient := client.Core()
	volume, err := coreClient.PersistentVolumeClaims("bosh").Get("disk-"+diskID, metav1.GetOptions{})
	if err != nil {
		return err
	}

	old, err := json.Marshal(volume)
	if err != nil {
		return err
	}

	for k, v := range metadata {
		k = "bosh.cloudfoundry.org/" + strings.ToLower(k)
		if len(validation.IsQualifiedName(k)) == 0 && len(validation.IsValidLabelValue(v)) == 0 {
			volume.ObjectMeta.Labels[k] = v
		}
	}

	new, err := json.Marshal(volume)
	if err != nil {
		return err
	}

	patch, err := strategicpatch.CreateTwoWayMergePatch(old, new, volume)
	if err != nil {
		return err
	}

	_, err = coreClient.PersistentVolumeClaims("bosh").Patch(volume.Name, types.StrategicMergePatchType, patch)
	if err != nil {
		return err
	}

	return nil
}
