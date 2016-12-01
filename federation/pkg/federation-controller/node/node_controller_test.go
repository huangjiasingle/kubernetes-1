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

package node

import (
	"fmt"
	"testing"
	"time"

	federation_api "k8s.io/kubernetes/federation/apis/federation/v1beta1"
	fake_fedclientset "k8s.io/kubernetes/federation/client/clientset_generated/federation_release_1_5/fake"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util"
	. "k8s.io/kubernetes/federation/pkg/federation-controller/util/test"
	api_v1 "k8s.io/kubernetes/pkg/api/v1"
	kubeclientset "k8s.io/kubernetes/pkg/client/clientset_generated/release_1_5"
	fake_kubeclientset "k8s.io/kubernetes/pkg/client/clientset_generated/release_1_5/fake"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/util/wait"
	"github.com/stretchr/testify/assert"
)

func TestNodeController(t *testing.T) {
	cluster1 := NewCluster("cluster1", api_v1.ConditionTrue)
	cluster2 := NewCluster("cluster2", api_v1.ConditionTrue)

	fakeClient := &fake_fedclientset.Clientset{}
	RegisterFakeList("clusters", &fakeClient.Fake, &federation_api.ClusterList{Items: []federation_api.Cluster{*cluster1}})
	RegisterFakeList("nodes", &fakeClient.Fake, &api_v1.NodeList{Items: []api_v1.Node{}})
	nodeWatch := RegisterFakeWatch("nodes", &fakeClient.Fake)
	clusterWatch := RegisterFakeWatch("clusters", &fakeClient.Fake)

	cluster1Client := &fake_kubeclientset.Clientset{}
	cluster1Watch := RegisterFakeWatch("nodes", &cluster1Client.Fake)
	RegisterFakeList("nodes", &cluster1Client.Fake, &api_v1.NodeList{Items: []api_v1.Node{}})
	cluster1CreateChan := RegisterFakeCopyOnCreate("nodes", &cluster1Client.Fake, cluster1Watch)
	cluster1UpdateChan := RegisterFakeCopyOnUpdate("nodes", &cluster1Client.Fake, cluster1Watch)

	cluster2Client := &fake_kubeclientset.Clientset{}
	cluster2Watch := RegisterFakeWatch("nodes", &cluster2Client.Fake)
	RegisterFakeList("nodes", &cluster2Client.Fake, &api_v1.NodeList{Items: []api_v1.Node{}})
	cluster2CreateChan := RegisterFakeCopyOnCreate("nodes", &cluster2Client.Fake, cluster2Watch)

	nodeController := NewNodeController(fakeClient)
	informer := ToFederatedInformerForTestOnly(nodeController.nodeFederatedInformer)
	informer.SetClientFactory(func(cluster *federation_api.Cluster) (kubeclientset.Interface, error) {
		switch cluster.Name {
		case cluster1.Name:
			return cluster1Client, nil
		case cluster2.Name:
			return cluster2Client, nil
		default:
			return nil, fmt.Errorf("Unknown cluster")
		}
	})

	nodeController.clusterAvailableDelay = time.Second
	nodeController.nodeReviewDelay = 50 * time.Millisecond
	nodeController.smallDelay = 20 * time.Millisecond
	nodeController.updateTimeout = 5 * time.Second

	stop := make(chan struct{})
	nodeController.Run(stop)

	node1 := &api_v1.Node{
		ObjectMeta: api_v1.ObjectMeta{
			Name:      "10.0.0.1",
			SelfLink:  "/api/v1/nodes/10.0.0.1",
		},
	}

	// Test add federated node.
	nodeWatch.Add(node1)
	createdNode := GetNodeFromChan(cluster1CreateChan)
	assert.NotNil(t, createdNode)
	assert.Equal(t, node1.Name, createdNode.Name)

	// Wait for the node to appear in the informer store
	err := WaitForStoreUpdate(
		nodeController.nodeFederatedInformer.GetTargetStore(),
		cluster1.Name, node1.Name, wait.ForeverTestTimeout)
	assert.Nil(t, err, "node should have appeared in the informer store")

	// Test update federated node.
	node1.Annotations = map[string]string{
		"A": "B",
	}
	nodeWatch.Modify(node1)
	updatedNode := GetNodeFromChan(cluster1UpdateChan)
	assert.NotNil(t, updatedNode)
	assert.Equal(t, node1.Name, updatedNode.Name)
	assert.True(t, util.NodeEquivalent(node1, updatedNode))

	// Test update federated node.
	node1.Name = "10.0.0.2"
	nodeWatch.Modify(node1)
	updatedNode2 := GetNodeFromChan(cluster1UpdateChan)
	assert.NotNil(t, updatedNode2)
	assert.Equal(t, node1.Name, updatedNode2.Name)
	assert.True(t, util.NodeEquivalent(node1, updatedNode2))

	// Test add cluster
	clusterWatch.Add(cluster2)
	createdNode2 := GetNodeFromChan(cluster2CreateChan)
	assert.NotNil(t, createdNode2)
	assert.Equal(t, node1.Name, createdNode2.Name)
	assert.True(t, util.NodeEquivalent(node1, createdNode2))

	close(stop)
}

func GetNodeFromChan(c chan runtime.Object) *api_v1.Node {
	node := GetObjectFromChan(c).(*api_v1.Node)
	return node
}
