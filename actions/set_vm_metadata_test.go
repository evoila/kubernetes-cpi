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
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/testing"
)

var _ = Describe("SetVMMetadata", func() {
	var (
		fakeClient   *fakes.Client
		fakeProvider *fakes.ClientProvider
		vmcid        cpi.VMCID
		metadata     map[string]string

		vmMetadataSetter *actions.VMMetadataSetter
	)

	BeforeEach(func() {
		fakeClient = fakes.NewClient()
		fakeClient.ContextReturns("bosh")
		fakeClient.NamespaceReturns("bosh-namespace")

		fakeProvider = &fakes.ClientProvider{}
		fakeProvider.NewReturns(fakeClient, nil)

		vmcid = actions.NewVMCID("bosh", "agent-id")
		metadata = map[string]string{
			"deployment":       "kube-test-bosh",
			"director":         "bosh-init",
			"job":              "bosh",
			"index":            "0",
			"invalid key name": "good-value",
			"valid-key-name":   "***invalid value***",
		}

		fakeClient.Clientset = *fake.NewSimpleClientset(
			&v1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name:      "agent-agent-id",
				Namespace: "bosh-namespace",
				Labels: map[string]string{
					"key": "value",
				},
			}},
		)

		vmMetadataSetter = &actions.VMMetadataSetter{ClientProvider: fakeProvider}
	})

	It("gets a client for the appropriate context", func() {
		err := vmMetadataSetter.SetVMMetadata(vmcid, metadata)
		Expect(err).NotTo(HaveOccurred())

		Expect(fakeProvider.NewCallCount()).To(Equal(1))
		Expect(fakeProvider.NewArgsForCall(0)).To(Equal("bosh"))
	})

	It("retrieves the pod", func() {
		err := vmMetadataSetter.SetVMMetadata(vmcid, metadata)
		Expect(err).NotTo(HaveOccurred())

		matches := fakeClient.MatchingActions("get", "pods")
		Expect(matches).To(HaveLen(1))

		Expect(matches[0].(testing.GetAction).GetName()).To(Equal("agent-agent-id"))
	})

	It("patches the pod with prefixed labels and omits invalid labels", func() {
		err := vmMetadataSetter.SetVMMetadata(vmcid, metadata)
		Expect(err).NotTo(HaveOccurred())

		matches := fakeClient.MatchingActions("patch", "pods")
		Expect(matches).To(HaveLen(1))

		patch := matches[0].(testing.PatchActionImpl)
		Expect(patch.GetName()).To(Equal("agent-agent-id"))
		Expect(patch.GetPatch()).To(MatchJSON(`{
				"metadata": {
					"labels": {
						"bosh.cloudfoundry.org/deployment": "kube-test-bosh",
						"bosh.cloudfoundry.org/director": "bosh-init",
						"bosh.cloudfoundry.org/index": "0",
						"bosh.cloudfoundry.org/job": "bosh"
					}
				}
			}`,
		))
	})

	Context("when getting the client fails", func() {
		BeforeEach(func() {
			fakeProvider.NewReturns(nil, errors.New("boom"))
		})

		It("gets a client for the appropriate context", func() {
			err := vmMetadataSetter.SetVMMetadata(vmcid, metadata)
			Expect(err).To(MatchError("boom"))
		})
	})

	Context("when getting the pod fails", func() {
		BeforeEach(func() {
			fakeClient.PrependReactor("get", "pods", func(action testing.Action) (bool, runtime.Object, error) {
				return true, nil, errors.New("get-pods-welp")
			})
		})

		It("returns an error", func() {
			err := vmMetadataSetter.SetVMMetadata(vmcid, metadata)
			Expect(err).To(MatchError("get-pods-welp"))
			Expect(fakeClient.MatchingActions("get", "pods")).To(HaveLen(1))
		})
	})

	Context("when patching the pod fails", func() {
		BeforeEach(func() {
			fakeClient.PrependReactor("patch", "pods", func(action testing.Action) (bool, runtime.Object, error) {
				return true, nil, errors.New("patch-pods-welp")
			})
		})

		It("returns an error", func() {
			err := vmMetadataSetter.SetVMMetadata(vmcid, metadata)
			Expect(err).To(MatchError("patch-pods-welp"))
			Expect(fakeClient.MatchingActions("patch", "pods")).To(HaveLen(1))
		})
	})
})
