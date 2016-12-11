/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resourcequota

import (
	"flag"
	"fmt"
	"testing"
	"time"

	fedv1 "k8s.io/kubernetes/federation/apis/federation/v1beta1"
	fedclientfake "k8s.io/kubernetes/federation/client/clientset_generated/federation_release_1_5/fake"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util/test"
	"k8s.io/kubernetes/pkg/api/meta"
	"k8s.io/kubernetes/pkg/api/resource"
	apiv1 "k8s.io/kubernetes/pkg/api/v1"
	kubeclientset "k8s.io/kubernetes/pkg/client/clientset_generated/release_1_5"
	kubeclientfake "k8s.io/kubernetes/pkg/client/clientset_generated/release_1_5/fake"
	"k8s.io/kubernetes/pkg/client/testing/core"
	"k8s.io/kubernetes/pkg/watch"

	"github.com/stretchr/testify/assert"
)

func TestParseFederationResourceQuotaReference(t *testing.T) {
	successPrefs := []string{
		`{"rebalance": true,
		  "clusters": {
		    "c1": {"weight": 1}
		}}`,
	}
	failedPrefes := []string{
		`{`, // bad json
	}

	rq := newResourceQuotaWithResourceQuota("rq-1", getResourceList("2m", "10Mi"), []apiv1.ResourceQuotaScope{apiv1.ResourceQuotaScopeBestEffort})
	accessor, _ := meta.Accessor(rq)
	anno := accessor.GetAnnotations()
	if anno == nil {
		anno = make(map[string]string)
		accessor.SetAnnotations(anno)
	}
	for _, prefString := range successPrefs {
		anno[FedResourceQuotaPreferencesAnnotation] = prefString
		pref, err := parseFederationResourceQuotaPreference(rq)
		assert.NotNil(t, pref)
		assert.Nil(t, err)
	}
	for _, prefString := range failedPrefes {
		anno[FedResourceQuotaPreferencesAnnotation] = prefString
		pref, err := parseFederationResourceQuotaPreference(rq)
		assert.Nil(t, pref)
		assert.NotNil(t, err)
	}
}

func TestResourceQuotaController(t *testing.T) {
	flag.Set("logtostderr", "true")
	flag.Set("v", "5")
	flag.Parse()

	fedclientset := fedclientfake.NewSimpleClientset()
	fedResourceQuotaWatch := watch.NewFake()
	fedclientset.PrependWatchReactor("resourcequotas", core.DefaultWatchReactor(fedResourceQuotaWatch, nil))

	fedclientset.Federation().Clusters().Create(testutil.NewCluster("k8s-1", apiv1.ConditionTrue))
	fedclientset.Federation().Clusters().Create(testutil.NewCluster("k8s-2", apiv1.ConditionTrue))

	kube1clientset := kubeclientfake.NewSimpleClientset()
	kube1ResourceQuotaWatch := watch.NewFake()
	kube1clientset.PrependWatchReactor("resourcequotas", core.DefaultWatchReactor(kube1ResourceQuotaWatch, nil))
	kube2clientset := kubeclientfake.NewSimpleClientset()
	kube2ResourceQuotaWatch := watch.NewFake()
	kube2clientset.PrependWatchReactor("resourcequotas", core.DefaultWatchReactor(kube2ResourceQuotaWatch, nil))

	fedInformerClientFactory := func(cluster *fedv1.Cluster) (kubeclientset.Interface, error) {
		switch cluster.Name {
		case "k8s-1":
			return kube1clientset, nil
		case "k8s-2":
			return kube2clientset, nil
		default:
			return nil, fmt.Errorf("Unknown cluster: %v", cluster.Name)
		}
	}
	resourceQuotaController := NewResourceQuotaController(fedclientset)

	resourceQuotaController.resourceQuotaReviewDelay = 10 * time.Millisecond
	resourceQuotaController.clusterAvailableDelay = 20 * time.Millisecond
	resourceQuotaController.smallDelay = 60 * time.Millisecond
	resourceQuotaController.updateTimeout = 120 * time.Millisecond

	rsFedinformer := testutil.ToFederatedInformerForTestOnly(resourceQuotaController.resourceQuotaFederatedInformer)
	rsFedinformer.SetClientFactory(fedInformerClientFactory)

	stopChan := make(chan struct{})
	defer close(stopChan)
	go resourceQuotaController.Run(stopChan)

	frq := newResourceQuotaWithResourceQuota("frq", getResourceList("2m", "10Mi"), []apiv1.ResourceQuotaScope{apiv1.ResourceQuotaScopeBestEffort})
	frq, _ = fedclientset.Core().ResourceQuotas(apiv1.NamespaceDefault).Create(frq)
	fedResourceQuotaWatch.Add(frq)

	time.Sleep(1 * time.Second)

	rq1, _ := kube1clientset.Core().ResourceQuotas(apiv1.NamespaceDefault).Get(frq.Name)
	kube1ResourceQuotaWatch.Add(rq1)
	rq1.Status.Hard = rq1.Spec.Hard

	rq1, _ = kube1clientset.Core().ResourceQuotas(apiv1.NamespaceDefault).UpdateStatus(rq1)
	kube1ResourceQuotaWatch.Modify(rq1)

	rq2, _ := kube2clientset.Core().ResourceQuotas(apiv1.NamespaceDefault).Get(frq.Name)
	kube2ResourceQuotaWatch.Add(rq2)
	rq2.Status.Hard = rq2.Spec.Hard
	rq2, _ = kube2clientset.Core().ResourceQuotas(apiv1.NamespaceDefault).UpdateStatus(rq2)
	kube2ResourceQuotaWatch.Modify(rq2)

	time.Sleep(1 * time.Second)

	frq, _ = fedclientset.Core().ResourceQuotas(apiv1.NamespaceDefault).Get(frq.Name)
	//assert.Equal(t, *frq.Spec.Hard, *rq1.Spec.Hard+*rq2.Spec.Hard)
	//assert.Equal(t, frq.Status.Hard, rq1.Status.Hard+rq2.Status.Hard)
	assert.Equal(t, frq.Spec, rq1.Spec)
	assert.Equal(t, frq.Spec, rq2.Spec)
	assert.Equal(t, rq1.Status, rq2.Status)

	frq.Spec.Hard = getResourceList("4m", "20Mi")
	frq, _ = fedclientset.Core().ResourceQuotas(apiv1.NamespaceDefault).Update(frq)
	fedResourceQuotaWatch.Modify(frq)
	time.Sleep(1 * time.Second)

	rq1, _ = kube1clientset.Core().ResourceQuotas(apiv1.NamespaceDefault).Get(frq.Name)
	rq1.Status.Hard = rq1.Spec.Hard
	rq1, _ = kube1clientset.Core().ResourceQuotas(apiv1.NamespaceDefault).UpdateStatus(rq1)
	kube1ResourceQuotaWatch.Modify(rq1)

	rq2, _ = kube2clientset.Core().ResourceQuotas(apiv1.NamespaceDefault).Get(frq.Name)
	rq2.Status.Hard = rq2.Spec.Hard
	rq2, _ = kube2clientset.Core().ResourceQuotas(apiv1.NamespaceDefault).UpdateStatus(rq2)
	kube2ResourceQuotaWatch.Modify(rq2)

	time.Sleep(1 * time.Second)
	frq, _ = fedclientset.Core().ResourceQuotas(apiv1.NamespaceDefault).Get(frq.Name)
	//assert.Equal(t, *rq.Spec.Hard, *rq1.Spec.Hard+*rq2.Spec.Hard)
	//assert.Equal(t, rq.Status.Hard, rq1.Status.Hard+rq2.Status.Hard)
	assert.Equal(t, frq.Spec, rq1.Spec)
	assert.Equal(t, frq.Spec, rq2.Spec)
	assert.Equal(t, rq1.Status, rq2.Status)
}

func newResourceQuotaWithResourceQuota(name string, hard apiv1.ResourceList, scopes []apiv1.ResourceQuotaScope) *apiv1.ResourceQuota {
	return &apiv1.ResourceQuota {
		ObjectMeta: apiv1.ObjectMeta{
			Name:      name,
			Namespace: apiv1.NamespaceDefault,
			SelfLink:  "/api/v1/namespaces/default/resourcequota/" + name,
		},
		Spec: apiv1.ResourceQuotaSpec{
			Hard: hard,
			Scopes: scopes,
		},
	}
}

// getResourceList returns a ResourceList with the
// specified cpu and memory resource values
func getResourceList(cpu, memory string) apiv1.ResourceList {
	res := apiv1.ResourceList{}
	if cpu != "" {
		res[apiv1.ResourceCPU] = resource.MustParse(cpu)
	}
	if memory != "" {
		res[apiv1.ResourceMemory] = resource.MustParse(memory)
	}
	return res
}
