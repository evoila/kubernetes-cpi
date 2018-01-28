package actions_test

import (
	"errors"

	"github.com/evoila/kubernetes-cpi/actions"
	"github.com/evoila/kubernetes-cpi/cpi"
	"github.com/evoila/kubernetes-cpi/kubecluster/fakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/testing"
)

var _ = Describe("VMFinder", func() {
	var (
		fakeClient   *fakes.Client
		fakeProvider *fakes.ClientProvider

		vmFinder *actions.VMFinder
	)

	BeforeEach(func() {
		fakeClient = fakes.NewClient(&v1.PodList{
			Items: []v1.Pod{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "agent-agentID",
					Namespace: "bosh-namespace",
					Labels:    map[string]string{"bosh.cloudfoundry.org/agent-id": "agentID"},
				},
			}},
		})

		fakeProvider = &fakes.ClientProvider{}
		fakeProvider.NewReturns(fakeClient, nil)

		vmFinder = &actions.VMFinder{
			ClientProvider: fakeProvider,
		}
	})

	Describe("HasVM", func() {
		It("gets a client with the context from the VMCID", func() {
			_, err := vmFinder.HasVM(cpi.VMCID("context-name:agentID"))
			Expect(err).NotTo(HaveOccurred())

			Expect(fakeProvider.NewCallCount()).To(Equal(1))
			Expect(fakeProvider.NewArgsForCall(0)).To(Equal("context-name"))
		})

		It("returns true when the pod is found", func() {
			found, err := vmFinder.HasVM(cpi.VMCID("context-name:agentID"))
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
		})

		It("returns false when the pod is not found", func() {
			found, err := vmFinder.HasVM(cpi.VMCID("context-name:missing"))
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeFalse())
		})

		Context("when FindVM fails", func() {
			BeforeEach(func() {
				fakeProvider.NewReturns(nil, errors.New("welp"))
			})

			It("returns an error", func() {
				_, err := vmFinder.HasVM(cpi.VMCID("context-name:agentID"))
				Expect(err).To(MatchError("welp"))
			})
		})
	})

	Describe("FindVM", func() {
		It("uses the client for the context in the VMCID", func() {
			_, _, err := vmFinder.FindVM(cpi.VMCID("context-name:agentID"))
			Expect(err).NotTo(HaveOccurred())

			Expect(fakeProvider.NewCallCount()).To(Equal(1))
			Expect(fakeProvider.NewArgsForCall(0)).To(Equal("context-name"))
		})

		It("selects pods labeled with the agentID in the VMCID", func() {
			_, _, err := vmFinder.FindVM(cpi.VMCID("context-name:agentID"))
			Expect(err).NotTo(HaveOccurred())

			Expect(fakeClient.Actions()).To(HaveLen(1))
			listAction := fakeClient.Actions()[0].(testing.ListAction)
			Expect(listAction.GetListRestrictions().Labels.String()).To(Equal("bosh.cloudfoundry.org/agent-id=agentID"))
		})

		It("returns the context name and matching pod", func() {
			context, pod, err := vmFinder.FindVM(cpi.VMCID("context-name:agentID"))
			Expect(err).NotTo(HaveOccurred())

			Expect(context).To(Equal("context-name"))

			Expect(pod).NotTo(BeNil())
			Expect(pod.Name).To(Equal("agent-agentID"))
		})

		Context("when the client cannot be created", func() {
			BeforeEach(func() {
				fakeProvider.NewReturns(nil, errors.New("welp"))
			})

			It("returns an error", func() {
				_, _, err := vmFinder.FindVM(cpi.VMCID("context-name:agentID"))
				Expect(err).To(MatchError("welp"))
			})
		})

		Context("when the label can't be parsed", func() {
			It("returns an error", func() {
				_, _, err := vmFinder.FindVM(cpi.VMCID("context-name:%&^*****@*^"))
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when listing the pods fails", func() {
			BeforeEach(func() {
				fakeClient.PrependReactor("list", "*", func(action testing.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("welp")
				})
			})

			It("returns an error", func() {
				_, _, err := vmFinder.FindVM(cpi.VMCID("context-name:agentID"))
				Expect(err).To(MatchError("welp"))
				Expect(fakeClient.Actions()).To(HaveLen(1))
			})
		})
	})
})
