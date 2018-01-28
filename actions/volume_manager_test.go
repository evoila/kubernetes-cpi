package actions_test

import (
	"encoding/json"
	"errors"
	"time"

	"code.cloudfoundry.org/clock/fakeclock"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/evoila/kubernetes-cpi/actions"
	"github.com/evoila/kubernetes-cpi/agent"
	"github.com/evoila/kubernetes-cpi/cpi"
	"github.com/evoila/kubernetes-cpi/kubecluster/fakes"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/testing"
)

var _ = Describe("VolumeManager", func() {
	var (
		fakeClient   *fakes.Client
		fakeProvider *fakes.ClientProvider
		fakeClock    *fakeclock.FakeClock
		fakeWatch    *watch.FakeWatcher
		vmcid        cpi.VMCID
		diskCID      cpi.DiskCID
		agentMeta    metav1.ObjectMeta

		volumeManager *actions.VolumeManager
	)

	BeforeEach(func() {
		vmcid = actions.NewVMCID("context-name", "agent-id")
		diskCID = actions.NewDiskCID("context-name", "disk-id")

		agentMeta = metav1.ObjectMeta{
			Name:      "agent-agent-id",
			Namespace: "bosh-namespace",
			Annotations: map[string]string{
				"annotation-key": "annotation-value",
			},
			Labels: map[string]string{
				"key": "value",
			},
		}

		fakeProvider = &fakes.ClientProvider{}
		fakeClock = fakeclock.NewFakeClock(time.Now())

		volumeManager = &actions.VolumeManager{
			ClientProvider:    fakeProvider,
			Clock:             fakeClock,
			PodReadyTimeout:   30 * time.Second,
			PostRecreateDelay: 0,
		}
	})

	Describe("AttachDisk", func() {
		var initialPodSpec v1.PodSpec
		var initialPod *v1.Pod

		BeforeEach(func() {
			initialPodSpec = v1.PodSpec{
				Volumes: []v1.Volume{},
				Containers: []v1.Container{{
					Name:  "bosh-job",
					Image: "stemcell-name",
				}, {
					Name:  "ignored-name",
					Image: "ignored-image",
				}},
			}

			initialPod = &v1.Pod{
				ObjectMeta: agentMeta,
				Spec:       initialPodSpec,
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					PodIP: "1.2.3.4",
					ContainerStatuses: []v1.ContainerStatus{{
						Name:  "bosh-job",
						Ready: true,
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{},
						},
					}, {
						Name:  "ignored-name",
						Ready: true,
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{},
						},
					}},
				},
			}

			fakeClient = fakes.NewClient(
				&v1.ConfigMap{
					ObjectMeta: agentMeta,
					Data: map[string]string{
						"instance_settings": `{}`,
					},
				},
				initialPod,
			)
			fakeClient.ContextReturns("context-name")
			fakeClient.NamespaceReturns("bosh-namespace")

			fakeWatch = watch.NewFakeWithChanSize(1, true)
			fakeWatch.Modify(&v1.Pod{
				ObjectMeta: agentMeta,
				Spec:       initialPodSpec,
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					PodIP: "1.2.3.4",
					ContainerStatuses: []v1.ContainerStatus{{
						Name:  "bosh-job",
						Ready: true,
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{},
						},
					}},
				},
			})
			fakeClient.PrependWatchReactor("pods", testing.DefaultWatchReactor(fakeWatch, nil))
			fakeProvider.NewReturns(fakeClient, nil)
		})

		It("gets a client for the appropriate context", func() {
			err := volumeManager.AttachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			Expect(fakeProvider.NewCallCount()).To(Equal(1))
			Expect(fakeProvider.NewArgsForCall(0)).To(Equal("context-name"))
		})

		It("retrieves and updates the the agent config map", func() {
			err := volumeManager.AttachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			matches := fakeClient.MatchingActions("get", "configmaps")
			Expect(matches).To(HaveLen(1))
			Expect(matches[0].(testing.GetAction).GetName()).To(Equal("agent-agent-id"))

			matches = fakeClient.MatchingActions("update", "configmaps")
			Expect(matches).To(HaveLen(1))

			updated := matches[0].(testing.UpdateAction).GetObject().(*v1.ConfigMap)
			Expect(updated.Name).To(Equal("agent-agent-id"))

			var settings agent.Settings
			Expect(json.Unmarshal([]byte(updated.Data["instance_settings"]), &settings)).To(Succeed())
			Expect(settings.Disks.Persistent).To(HaveKeyWithValue("context-name:disk-id", "/mnt/disk-id"))
		})

		It("retrieves, deletes, and recreates the pod", func() {
			err := volumeManager.AttachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			matches := fakeClient.MatchingActions("get", "pods")
			Expect(matches).To(HaveLen(1))
			Expect(matches[0].(testing.GetAction).GetName()).To(Equal("agent-agent-id"))

			matches = fakeClient.MatchingActions("delete", "pods")
			Expect(matches).To(HaveLen(1))
			Expect(matches[0].(testing.DeleteAction).GetName()).To(Equal("agent-agent-id"))

			matches = fakeClient.MatchingActions("create", "pods")
			Expect(matches).To(HaveLen(1))

			updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
			Expect(updated.Name).To(Equal("agent-agent-id"))
			Expect(updated.Spec).NotTo(Equal(initialPodSpec))
		})

		It("carries the pod metadata forward on recreate", func() {
			err := volumeManager.AttachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			matches := fakeClient.MatchingActions("create", "pods")
			Expect(matches).To(HaveLen(1))

			updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
			delete(updated.Annotations, "bosh.cloudfoundry.org/ip-address")
			Expect(updated.ObjectMeta).To(Equal(agentMeta))
		})

		It("propagates the PodIP to the ip-address annotation", func() {
			err := volumeManager.AttachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			matches := fakeClient.MatchingActions("create", "pods")
			Expect(matches).To(HaveLen(1))

			updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
			Expect(updated.Annotations["bosh.cloudfoundry.org/ip-address"]).To(Equal("1.2.3.4"))
		})

		Context("when the annotation map is nil", func() {
			BeforeEach(func() {
				fakeClient.PrependReactor("get", "pods", func(action testing.Action) (bool, runtime.Object, error) {
					initialPod.Annotations = nil
					return true, initialPod, nil
				})
			})

			It("propagates the PodIP to the ip-address annotation", func() {
				err := volumeManager.AttachDisk(vmcid, diskCID)
				Expect(err).NotTo(HaveOccurred())

				matches := fakeClient.MatchingActions("create", "pods")
				Expect(matches).To(HaveLen(1))

				updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
				Expect(updated.Annotations["bosh.cloudfoundry.org/ip-address"]).To(Equal("1.2.3.4"))
			})
		})

		Context("when the ip-address annotation is present", func() {
			BeforeEach(func() {
				fakeClient.PrependReactor("get", "pods", func(action testing.Action) (bool, runtime.Object, error) {
					initialPod.Annotations["bosh.cloudfoundry.org/ip-address"] = "10.10.10.10"
					return true, initialPod, nil
				})
			})

			It("does not overwrite the annotation", func() {
				err := volumeManager.AttachDisk(vmcid, diskCID)
				Expect(err).NotTo(HaveOccurred())

				matches := fakeClient.MatchingActions("create", "pods")
				Expect(matches).To(HaveLen(1))

				updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
				Expect(updated.Annotations["bosh.cloudfoundry.org/ip-address"]).To(Equal("10.10.10.10"))
			})
		})

		It("adds pvc volume for the disk to the pod", func() {
			err := volumeManager.AttachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			matches := fakeClient.MatchingActions("create", "pods")
			Expect(matches).To(HaveLen(1))

			updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
			Expect(updated.Spec.Volumes).To(ContainElement(
				v1.Volume{
					Name: "disk-disk-id",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "disk-disk-id",
						},
					},
				},
			))
		})

		It("mounts the volume to the bosh-job container", func() {
			err := volumeManager.AttachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			matches := fakeClient.MatchingActions("create", "pods")
			Expect(matches).To(HaveLen(1))

			updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
			Expect(updated.Spec.Containers[0].Name).To(Equal("bosh-job"))
			Expect(updated.Spec.Containers[0].VolumeMounts).To(ContainElement(
				v1.VolumeMount{
					Name:      "disk-disk-id",
					MountPath: "/mnt/disk-id",
				},
			))
		})

		It("does not carry the pod status forward", func() {
			err := volumeManager.AttachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			matches := fakeClient.MatchingActions("create", "pods")
			Expect(matches).To(HaveLen(1))

			updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
			Expect(updated.Status).To(BeZero())
		})

		It("waits for the pod to be running", func() {
			event, ok := <-fakeWatch.ResultChan()
			Expect(ok).To(BeTrue())

			fakeClient.PrependReactor("create", "pods", func(action testing.Action) (bool, runtime.Object, error) {
				pod := action.(testing.CreateAction).GetObject().(*v1.Pod)
				pod.ResourceVersion = "created-resource-version"
				return true, pod, nil
			})

			result := make(chan error)
			go func() { result <- volumeManager.AttachDisk(vmcid, diskCID) }()

			Eventually(func() []testing.Action {
				return fakeClient.MatchingActions("watch", "pods")
			}).Should(HaveLen(1))
			Expect(fakeWatch.IsStopped()).To(BeFalse())

			By("transitioning pod status to pending")
			fakeWatch.Modify(&v1.Pod{
				ObjectMeta: agentMeta,
				Spec:       initialPodSpec,
				Status: v1.PodStatus{
					Phase: v1.PodPending,
				},
			})
			Consistently(result).ShouldNot(Receive())

			By("transitioning bosh job container to running")
			fakeWatch.Modify(&v1.Pod{
				ObjectMeta: agentMeta,
				Spec:       initialPodSpec,
				Status: v1.PodStatus{
					Phase: v1.PodPending,
					PodIP: "1.2.3.4",
					ContainerStatuses: []v1.ContainerStatus{{
						Name:  "bosh-job",
						Ready: true,
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{},
						},
					}},
				},
			})
			Consistently(result).ShouldNot(Receive())

			By("transitioning pod and container to running")
			fakeWatch.Modify(event.Object)
			Eventually(result).Should(Receive(BeNil()))

			matches := fakeClient.MatchingActions("watch", "pods")
			Expect(matches).To(HaveLen(1))

			watchRestrictions := matches[0].(testing.WatchAction).GetWatchRestrictions()
			Expect(watchRestrictions.Labels.String()).To(Equal("bosh.cloudfoundry.org/agent-id=agent-id"))
			Expect(watchRestrictions.ResourceVersion).To(Equal("created-resource-version"))
			Expect(fakeWatch.IsStopped()).To(BeTrue())
		})

		It("waits for the post recreate duration", func() {
			volumeManager.PostRecreateDelay = 5 * time.Second

			result := make(chan error)
			go func() { result <- volumeManager.AttachDisk(vmcid, diskCID) }()

			Consistently(result).ShouldNot(Receive())
			fakeClock.Increment(3 * time.Second)
			Consistently(result).ShouldNot(Receive())

			fakeClock.Increment(3 * time.Second)
			Eventually(result).Should(Receive(BeNil()))
		})

		Context("when the vmcid context and diskcid context are different", func() {
			BeforeEach(func() {
				vmcid = actions.NewVMCID("rp-ctx", "agent-id")
				diskCID = actions.NewDiskCID("disk-ctx", "disk-id")
			})

			It("returns an error", func() {
				err := volumeManager.AttachDisk(vmcid, diskCID)
				Expect(err).To(MatchError(`Kubernetes disk and resource pool contexts must be the same: disk: "disk-ctx", resource pool: "rp-ctx"`))
			})
		})

		Context("when getting the config map fails", func() {
			BeforeEach(func() {
				fakeClient.PrependReactor("get", "configmaps", func(action testing.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("get-cm-welp")
				})
			})

			It("returns an error", func() {
				err := volumeManager.AttachDisk(vmcid, diskCID)
				Expect(err).To(MatchError("get-cm-welp"))
			})
		})

		Context("when updating the config map fails", func() {
			BeforeEach(func() {
				fakeClient.PrependReactor("update", "configmaps", func(action testing.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("update-cm-welp")
				})
			})

			It("returns an error", func() {
				err := volumeManager.AttachDisk(vmcid, diskCID)
				Expect(err).To(MatchError("update-cm-welp"))
			})
		})

		Context("when unmarshalling the instance settings fails", func() {
			BeforeEach(func() {
				cm := &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: "agent-agent-id", Namespace: "bosh-namespace"},
					Data:       map[string]string{"instance_settings": `!@$#@$#%!%`},
				}
				fakeClient.PrependReactor("get", "configmaps", func(action testing.Action) (bool, runtime.Object, error) {
					return true, cm, nil
				})
			})

			It("returns an error", func() {
				err := volumeManager.AttachDisk(vmcid, diskCID)
				Expect(err).To(BeAssignableToTypeOf(&json.SyntaxError{}))
			})
		})

		Context("when retrieving the pod fails", func() {
			BeforeEach(func() {
				fakeClient.PrependReactor("get", "pods", func(action testing.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("get-pods-welp")
				})
			})

			It("returns an error", func() {
				err := volumeManager.AttachDisk(vmcid, diskCID)
				Expect(err).To(MatchError("get-pods-welp"))
			})
		})

		Context("when deleting the pod fails", func() {
			BeforeEach(func() {
				fakeClient.PrependReactor("delete", "pods", func(action testing.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("delete-pods-welp")
				})
			})

			It("returns an error", func() {
				err := volumeManager.AttachDisk(vmcid, diskCID)
				Expect(err).To(MatchError("delete-pods-welp"))
			})
		})

		Context("when recreating the pod fails", func() {
			BeforeEach(func() {
				fakeClient.PrependReactor("create", "pods", func(action testing.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("create-pods-welp")
				})
			})

			It("returns an error", func() {
				err := volumeManager.AttachDisk(vmcid, diskCID)
				Expect(err).To(MatchError("create-pods-welp"))
			})
		})

		Context("when starting the pod watch fails", func() {
			BeforeEach(func() {
				fakeClient.PrependWatchReactor("pods", func(action testing.Action) (bool, watch.Interface, error) {
					return true, nil, errors.New("watch-pods-welp")
				})
			})

			It("returns an error", func() {
				err := volumeManager.AttachDisk(vmcid, diskCID)
				Expect(err).To(MatchError("watch-pods-welp"))
			})
		})

		Context("when the pod watch receives an unexpected object", func() {
			BeforeEach(func() {
				_, ok := <-fakeWatch.ResultChan()
				Expect(ok).To(BeTrue())
				fakeWatch.Modify(&v1.ReplicationController{})
			})

			It("returns an error", func() {
				err := volumeManager.AttachDisk(vmcid, diskCID)
				Expect(err).To(MatchError("Unexpected object type: *v1.ReplicationController"))
			})
		})

		Context("when the pod is not ready before the ready timeout", func() {
			BeforeEach(func() {
				_, ok := <-fakeWatch.ResultChan()
				Expect(ok).To(BeTrue())
			})

			It("returns a timeout error", func() {
				result := make(chan error)
				go func() { result <- volumeManager.AttachDisk(vmcid, diskCID) }()

				Consistently(result).ShouldNot(Receive())
				fakeClock.Increment(volumeManager.PodReadyTimeout + time.Second)
				Eventually(result).Should(Receive(MatchError("Pod recreate failed with a timeout")))
			})
		})
	})

	Describe("DetachDisk", func() {
		var initialPodSpec v1.PodSpec
		var initialPod *v1.Pod

		BeforeEach(func() {
			initialPodSpec = v1.PodSpec{
				Volumes: []v1.Volume{{
					Name: "disk-disk-id",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "disk-disk-id",
						},
					},
				}},
				Containers: []v1.Container{{
					Name:  "bosh-job",
					Image: "stemcell-name",
					VolumeMounts: []v1.VolumeMount{{
						Name:      "disk-disk-id",
						MountPath: "/mnt/disk-id",
					}},
				}},
			}

			initialPod = &v1.Pod{
				ObjectMeta: agentMeta,
				Spec:       initialPodSpec,
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					PodIP: "1.2.3.4",
					ContainerStatuses: []v1.ContainerStatus{{
						Name:  "bosh-job",
						Ready: true,
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{},
						},
					}},
				},
			}

			fakeClient = fakes.NewClient(
				&v1.ConfigMap{
					ObjectMeta: agentMeta,
					Data: map[string]string{
						"instance_settings": `{ "disks": {"persistent": { "context-name:disk-id": "/mnt/disk-id" }} }`,
					},
				},
				initialPod,
			)
			fakeClient.ContextReturns("context-name")
			fakeClient.NamespaceReturns("bosh-namespace")

			fakeWatch = watch.NewFakeWithChanSize(1, true)
			fakeWatch.Modify(&v1.Pod{
				ObjectMeta: agentMeta,
				Spec:       initialPodSpec,
				Status: v1.PodStatus{
					PodIP: "1.2.3.4",
					Phase: v1.PodRunning,
					ContainerStatuses: []v1.ContainerStatus{{
						Name:  "bosh-job",
						Ready: true,
						State: v1.ContainerState{Running: &v1.ContainerStateRunning{}},
					}},
				},
			})
			fakeClient.PrependWatchReactor("pods", testing.DefaultWatchReactor(fakeWatch, nil))
			fakeProvider.NewReturns(fakeClient, nil)
		})

		It("gets a client for the appropriate context", func() {
			err := volumeManager.DetachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			Expect(fakeProvider.NewCallCount()).To(Equal(1))
			Expect(fakeProvider.NewArgsForCall(0)).To(Equal("context-name"))
		})

		It("retrieves and updates the the agent config map without the persistent disk", func() {
			err := volumeManager.DetachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			matches := fakeClient.MatchingActions("get", "configmaps")
			Expect(matches).To(HaveLen(1))
			Expect(matches[0].(testing.GetAction).GetName()).To(Equal("agent-agent-id"))

			matches = fakeClient.MatchingActions("update", "configmaps")
			Expect(matches).To(HaveLen(1))

			updated := matches[0].(testing.UpdateAction).GetObject().(*v1.ConfigMap)
			Expect(updated.Name).To(Equal("agent-agent-id"))

			var settings agent.Settings
			Expect(json.Unmarshal([]byte(updated.Data["instance_settings"]), &settings)).To(Succeed())
			Expect(settings.Disks.Persistent).NotTo(HaveKey("context-name:disk-id"))
		})

		It("retrieves, deletes, and recreates the pod", func() {
			err := volumeManager.DetachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			matches := fakeClient.MatchingActions("get", "pods")
			Expect(matches).To(HaveLen(1))
			Expect(matches[0].(testing.GetAction).GetName()).To(Equal("agent-agent-id"))

			matches = fakeClient.MatchingActions("delete", "pods")
			Expect(matches).To(HaveLen(1))
			Expect(matches[0].(testing.DeleteAction).GetName()).To(Equal("agent-agent-id"))

			matches = fakeClient.MatchingActions("create", "pods")
			Expect(matches).To(HaveLen(1))

			updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
			Expect(updated.Name).To(Equal("agent-agent-id"))
			Expect(updated.Spec).NotTo(Equal(initialPodSpec))
		})

		It("carries the pod metadata forward on recreate", func() {
			err := volumeManager.DetachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			matches := fakeClient.MatchingActions("create", "pods")
			Expect(matches).To(HaveLen(1))

			updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
			delete(updated.Annotations, "bosh.cloudfoundry.org/ip-address")
			Expect(updated.ObjectMeta).To(Equal(agentMeta))
		})

		It("propagates the PodIP to the ip-address annotation", func() {
			err := volumeManager.DetachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			matches := fakeClient.MatchingActions("create", "pods")
			Expect(matches).To(HaveLen(1))

			updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
			Expect(updated.Annotations["bosh.cloudfoundry.org/ip-address"]).To(Equal("1.2.3.4"))
		})

		Context("when the annotation map is nil", func() {
			BeforeEach(func() {
				fakeClient.PrependReactor("get", "pods", func(action testing.Action) (bool, runtime.Object, error) {
					initialPod.Annotations = nil
					return true, initialPod, nil
				})
			})

			It("propagates the PodIP to the ip-address annotation", func() {
				err := volumeManager.DetachDisk(vmcid, diskCID)
				Expect(err).NotTo(HaveOccurred())

				matches := fakeClient.MatchingActions("create", "pods")
				Expect(matches).To(HaveLen(1))

				updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
				Expect(updated.Annotations["bosh.cloudfoundry.org/ip-address"]).To(Equal("1.2.3.4"))
			})
		})

		Context("when the ip-address annotation is present", func() {
			BeforeEach(func() {
				fakeClient.PrependReactor("get", "pods", func(action testing.Action) (bool, runtime.Object, error) {
					initialPod.Annotations["bosh.cloudfoundry.org/ip-address"] = "10.10.10.10"
					return true, initialPod, nil
				})
			})

			It("does not overwrite the annotation", func() {
				err := volumeManager.DetachDisk(vmcid, diskCID)
				Expect(err).NotTo(HaveOccurred())

				matches := fakeClient.MatchingActions("create", "pods")
				Expect(matches).To(HaveLen(1))

				updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
				Expect(updated.Annotations["bosh.cloudfoundry.org/ip-address"]).To(Equal("10.10.10.10"))
			})
		})

		It("removes the pvc volume for the disk from the pod", func() {
			err := volumeManager.DetachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			matches := fakeClient.MatchingActions("create", "pods")
			Expect(matches).To(HaveLen(1))

			updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
			Expect(updated.Spec.Volumes).NotTo(ContainElement(
				v1.Volume{
					Name: "disk-disk-id",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "disk-disk-id",
						},
					},
				},
			))
		})

		It("removes the mount for the the volume from the bosh-job container", func() {
			err := volumeManager.DetachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			matches := fakeClient.MatchingActions("create", "pods")
			Expect(matches).To(HaveLen(1))

			updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
			Expect(updated.Spec.Containers[0].Name).To(Equal("bosh-job"))
			Expect(updated.Spec.Containers[0].VolumeMounts).NotTo(ContainElement(
				v1.VolumeMount{
					Name:      "disk-disk-id",
					MountPath: "/mnt/disk-id",
				},
			))
		})

		It("does not carry the pod status forward", func() {
			err := volumeManager.DetachDisk(vmcid, diskCID)
			Expect(err).NotTo(HaveOccurred())

			matches := fakeClient.MatchingActions("create", "pods")
			Expect(matches).To(HaveLen(1))

			updated := matches[0].(testing.CreateAction).GetObject().(*v1.Pod)
			Expect(updated.Status).To(BeZero())
		})

		It("waits for the pod to be running", func() {
			event, ok := <-fakeWatch.ResultChan()
			Expect(ok).To(BeTrue())

			fakeClient.PrependReactor("create", "pods", func(action testing.Action) (bool, runtime.Object, error) {
				pod := action.(testing.CreateAction).GetObject().(*v1.Pod)
				pod.ResourceVersion = "created-resource-version"
				return true, pod, nil
			})

			result := make(chan error)
			go func() { result <- volumeManager.DetachDisk(vmcid, diskCID) }()

			Eventually(func() []testing.Action {
				return fakeClient.MatchingActions("watch", "pods")
			}).Should(HaveLen(1))
			Expect(fakeWatch.IsStopped()).To(BeFalse())

			By("transitioning pod status to pending")
			fakeWatch.Modify(&v1.Pod{
				ObjectMeta: agentMeta,
				Spec:       initialPodSpec,
				Status: v1.PodStatus{
					Phase: v1.PodPending,
				},
			})
			Consistently(result).ShouldNot(Receive())

			By("transitioning bosh job container to running")
			fakeWatch.Modify(&v1.Pod{
				ObjectMeta: agentMeta,
				Spec:       initialPodSpec,
				Status: v1.PodStatus{
					Phase: v1.PodPending,
					PodIP: "1.2.3.4",
					ContainerStatuses: []v1.ContainerStatus{{
						Name:  "bosh-job",
						Ready: true,
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{},
						},
					}},
				},
			})
			Consistently(result).ShouldNot(Receive())

			By("transitioning pod and container to running")
			fakeWatch.Modify(event.Object)
			Eventually(result).Should(Receive(BeNil()))

			matches := fakeClient.MatchingActions("watch", "pods")
			Expect(matches).To(HaveLen(1))

			watchRestrictions := matches[0].(testing.WatchAction).GetWatchRestrictions()
			Expect(watchRestrictions.Labels.String()).To(Equal("bosh.cloudfoundry.org/agent-id=agent-id"))
			Expect(watchRestrictions.ResourceVersion).To(Equal("created-resource-version"))
			Expect(fakeWatch.IsStopped()).To(BeTrue())
		})

		It("waits for the post recreate duration", func() {
			volumeManager.PostRecreateDelay = 5 * time.Second

			result := make(chan error)
			go func() { result <- volumeManager.DetachDisk(vmcid, diskCID) }()

			Consistently(result).ShouldNot(Receive())
			fakeClock.Increment(3 * time.Second)
			Consistently(result).ShouldNot(Receive())

			fakeClock.Increment(3 * time.Second)
			Eventually(result).Should(Receive(BeNil()))
		})

		Context("when the vmcid context and diskcid context are different", func() {
			BeforeEach(func() {
				vmcid = actions.NewVMCID("rp-ctx", "agent-id")
				diskCID = actions.NewDiskCID("disk-ctx", "disk-id")
			})

			It("returns an error", func() {
				err := volumeManager.DetachDisk(vmcid, diskCID)
				Expect(err).To(MatchError(`Kubernetes disk and resource pool contexts must be the same: disk: "disk-ctx", resource pool: "rp-ctx"`))
			})
		})
	})
})
