package actions

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"code.cloudfoundry.org/clock"

	"github.com/evoila/kubernetes-cpi/agent"
	"github.com/evoila/kubernetes-cpi/cpi"
	"github.com/evoila/kubernetes-cpi/kubecluster"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/remotecommand"
)

type VolumeManager struct {
	ClientProvider kubecluster.ClientProvider

	Clock             clock.Clock
	PodReadyTimeout   time.Duration
	PostRecreateDelay time.Duration
}

type Operation int

const (
	Add Operation = iota
	Remove
)

func (v *VolumeManager) AttachDisk(vmcid cpi.VMCID, diskCID cpi.DiskCID) error {
	vmContext, agentID := ParseVMCID(vmcid)
	context, diskID := ParseDiskCID(diskCID)
	if context != vmContext {
		return fmt.Errorf("Kubernetes disk and resource pool contexts must be the same: disk: %q, resource pool: %q", context, vmContext)
	}

	client, err := v.ClientProvider.New(context)
	if err != nil {
		return err
	}

	err = v.recreatePod(client, Add, agentID, diskID)
	if err != nil {
		return err
	}

	return nil
}

func (v *VolumeManager) DetachDisk(vmcid cpi.VMCID, diskCID cpi.DiskCID) error {
	vmContext, agentID := ParseVMCID(vmcid)
	context, diskID := ParseDiskCID(diskCID)
	if context != vmContext {
		return fmt.Errorf("Kubernetes disk and resource pool contexts must be the same: disk: %q, resource pool: %q", context, vmContext)
	}

	client, err := v.ClientProvider.New(context)
	if err != nil {
		return err
	}

	err = v.recreatePod(client, Remove, agentID, diskID)
	if err != nil {
		return err
	}

	return nil
}

func (v *VolumeManager) recreatePod(client kubecluster.Client, op Operation, agentID string, diskID string) error {
	podService := client.Pods()
	pod, err := podService.Get("agent-"+agentID, metav1.GetOptions{})
	if err != nil {
		return err
	}

	err = updateConfigMapDisks(client, op, agentID, diskID)
	if err != nil {
		return err
	}

	updateVolumes(op, &pod.Spec, diskID)

	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}

	if len(pod.Annotations["bosh.cloudfoundry.org/ip-address"]) == 0 {
		pod.Annotations["bosh.cloudfoundry.org/ip-address"] = pod.Status.PodIP
	}

	pod.ObjectMeta = metav1.ObjectMeta{
		Name:        pod.Name,
		Namespace:   pod.Namespace,
		Annotations: pod.Annotations,
		Labels:      pod.Labels,
	}
	pod.Status = v1.PodStatus{}

	err = podService.Delete("agent-"+agentID, &metav1.DeleteOptions{GracePeriodSeconds: int64Ptr(0)})
	if err != nil {
		return err
	}

	updated, err := podService.Create(pod)
	if err != nil {
		return err
	}

	ready, err := v.waitForPod(podService, agentID, updated.ResourceVersion)
	if err != nil {
		return err
	}

	if !ready {
		return errors.New("Pod recreate failed with a timeout")
	}

	v.WaitForPostPodDelay(agentID, client)

	return nil
}

func (v *VolumeManager) WaitForPostPodDelay(agentID string, client kubecluster.Client) error {
	podService := client.Pods()
	var (
		execOut bytes.Buffer
		execErr bytes.Buffer
	)

	pod, err := podService.Get("agent-"+agentID, metav1.GetOptions{})
	if err != nil {
		return err
	}

	restConfig, err := v.ClientProvider.GetRestConfig(client.Context())
	if err != nil {
		return err
	}

	req := client.Core().RESTClient().Post().Resource("pods").Name(pod.Name).
		Namespace(pod.Namespace).SubResource("exec")
	req.VersionedParams(&v1.PodExecOptions{
		Container: pod.Spec.Containers[0].Name,
		Command:   []string{"curl", "127.0.0.1:2825", "--max-time", "1"},
		Stdout:    true,
		Stderr:    true,
	}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(restConfig, "POST", req.URL())
	if err != nil {
		return err
	}

	exec.Stream(remotecommand.StreamOptions{
		Stdout: &execOut,
		Stderr: &execErr,
	})

	for strings.Contains(execErr.String(), "Connection refused") {
		execOut.Reset()
		execErr.Reset()

		exec, err := remotecommand.NewSPDYExecutor(restConfig, "POST", req.URL())
		if err != nil {
			return err
		}

		exec.Stream(remotecommand.StreamOptions{
			Stdout: &execOut,
			Stderr: &execErr,
		})
		time.Sleep(1 * time.Second)
	}

	return nil
}

func updateConfigMapDisks(client kubecluster.Client, op Operation, agentID, diskID string) error {
	configMapService := client.ConfigMaps()
	cm, err := configMapService.Get("agent-"+agentID, metav1.GetOptions{})
	if err != nil {
		return err
	}

	var settings agent.Settings
	err = json.Unmarshal([]byte(cm.Data["instance_settings"]), &settings)
	if err != nil {
		return err
	}

	diskCID := string(NewDiskCID(client.Context(), diskID))
	if settings.Disks.Persistent == nil {
		settings.Disks.Persistent = map[string]string{}
	}

	switch op {
	case Add:
		settings.Disks.Persistent[diskCID] = "/mnt/" + diskID
	case Remove:
		delete(settings.Disks.Persistent, diskCID)
	}

	settingsJSON, err := json.Marshal(settings)
	if err != nil {
		return err
	}

	cm.Data["instance_settings"] = string(settingsJSON)

	_, err = configMapService.Update(cm)
	if err != nil {
		return err
	}

	return nil
}

func updateVolumes(op Operation, spec *v1.PodSpec, diskID string) {
	switch op {
	case Add:
		addVolume(spec, diskID)
	case Remove:
		removeVolume(spec, diskID)
	}
}

func addVolume(spec *v1.PodSpec, diskID string) {
	spec.Volumes = append(spec.Volumes, v1.Volume{
		Name: "disk-" + diskID,
		VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
				ClaimName: "disk-" + diskID,
			},
		},
	})

	for i, c := range spec.Containers {
		if c.Name == "bosh-job" {
			spec.Containers[i].VolumeMounts = append(c.VolumeMounts, v1.VolumeMount{
				Name:      "disk-" + diskID,
				MountPath: "/mnt/" + diskID,
			})
			break
		}
	}
}

func removeVolume(spec *v1.PodSpec, diskID string) {
	for i, v := range spec.Volumes {
		if v.Name == "disk-"+diskID {
			spec.Volumes = append(spec.Volumes[:i], spec.Volumes[i+1:]...)
			break
		}
	}

	for i, c := range spec.Containers {
		if c.Name == "bosh-job" {
			for j, v := range c.VolumeMounts {
				if v.Name == "disk-"+diskID {
					spec.Containers[i].VolumeMounts = append(c.VolumeMounts[:j], c.VolumeMounts[j+1:]...)
					break
				}
			}
		}
	}
}

func (v *VolumeManager) waitForPod(podService core.PodInterface, agentID string, resourceVersion string) (bool, error) {
	agentSelector := "bosh.cloudfoundry.org/agent-id=" + agentID

	listOptions := metav1.ListOptions{
		LabelSelector:   agentSelector,
		ResourceVersion: resourceVersion,
		Watch:           true,
	}

	timer := v.Clock.NewTimer(v.PodReadyTimeout)
	defer timer.Stop()

	podWatch, err := podService.Watch(listOptions)
	if err != nil {
		return false, err
	}
	defer podWatch.Stop()

	for {
		select {
		case event := <-podWatch.ResultChan():
			switch event.Type {
			case watch.Modified:
				pod, ok := event.Object.(*v1.Pod)
				if !ok {
					return false, fmt.Errorf("Unexpected object type: %v", reflect.TypeOf(event.Object))
				}

				if isAgentContainerRunning(pod) {
					return true, nil
				}

			default:
				return false, fmt.Errorf("Unexpected pod watch event: %s", event.Type)
			}

		case <-timer.C():
			return false, nil
		}
	}
}

func isAgentContainerRunning(pod *v1.Pod) bool {
	if pod.Status.Phase != v1.PodRunning {
		return false
	}

	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Name == "bosh-job" {
			return containerStatus.Ready && containerStatus.State.Running != nil
		}
	}

	return false
}
