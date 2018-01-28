package actions

import (
	"github.com/evoila/kubernetes-cpi/cpi"
	"github.com/evoila/kubernetes-cpi/kubecluster"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	core "k8s.io/client-go/kubernetes/typed/core/v1"
)

type VMDeleter struct {
	ClientProvider kubecluster.ClientProvider
}

func (v *VMDeleter) Delete(vmcid cpi.VMCID) error {
	context, agentID := ParseVMCID(vmcid)

	client, err := v.ClientProvider.New(context)
	if err != nil {
		return err
	}

	err = deletePod(client.Pods(), agentID)
	if err != nil {
		return err
	}

	err = deleteServices(client.Services(), agentID)
	if err != nil {
		return err
	}

	err = deleteConfigMap(client.ConfigMaps(), agentID)
	if err != nil {
		return err
	}

	return nil
}

func deleteConfigMap(configMapService core.ConfigMapInterface, agentID string) error {
	err := configMapService.Delete("agent-"+agentID, &metav1.DeleteOptions{GracePeriodSeconds: int64Ptr(0)})
	if statusError, ok := err.(*kubeerrors.StatusError); ok {
		if statusError.Status().Reason == metav1.StatusReasonNotFound {
			return nil
		}
	}
	return err
}

func deleteServices(serviceClient core.ServiceInterface, agentID string) error {
	agentSelector, err := labels.Parse("bosh.cloudfoundry.org/agent-id=" + agentID)
	if err != nil {
		return err
	}

	serviceList, err := serviceClient.List(metav1.ListOptions{LabelSelector: agentSelector.String()})
	if err != nil {
		return err
	}

	for _, service := range serviceList.Items {
		err := serviceClient.Delete(service.Name, &metav1.DeleteOptions{GracePeriodSeconds: int64Ptr(0)})
		if err != nil {
			return err
		}
	}

	return nil
}

func deletePod(podClient core.PodInterface, agentID string) error {
	err := podClient.Delete("agent-"+agentID, &metav1.DeleteOptions{GracePeriodSeconds: int64Ptr(0)})
	if statusError, ok := err.(*kubeerrors.StatusError); ok {
		if statusError.Status().Reason == metav1.StatusReasonNotFound {
			return nil
		}
	}
	return err
}

func int64Ptr(i int64) *int64 {
	return &i
}
