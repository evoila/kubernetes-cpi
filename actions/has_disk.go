package actions

import (
	"github.com/evoila/kubernetes-cpi/cpi"
	"github.com/evoila/kubernetes-cpi/kubecluster"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type DiskFinder struct {
	ClientProvider kubecluster.ClientProvider
}

func (d *DiskFinder) HasDisk(diskCID cpi.DiskCID) (bool, error) {
	context, diskID := ParseDiskCID(diskCID)
	diskSelector, err := labels.Parse("bosh.cloudfoundry.org/disk-id=" + diskID)
	if err != nil {
		return false, err
	}

	client, err := d.ClientProvider.New(context)
	if err != nil {
		return false, err
	}

	listOptions := metav1.ListOptions{LabelSelector: diskSelector.String()}
	pvcList, err := client.PersistentVolumeClaims().List(listOptions)
	if err != nil {
		return false, err
	}

	return len(pvcList.Items) > 0, nil
}
