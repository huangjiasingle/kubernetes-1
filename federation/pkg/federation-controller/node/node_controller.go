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
	"time"

	federation_api "k8s.io/kubernetes/federation/apis/federation/v1beta1"
	federationclientset "k8s.io/kubernetes/federation/client/clientset_generated/federation_release_1_5"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util/eventsink"
	"k8s.io/kubernetes/pkg/api"
	api_v1 "k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/client/cache"
	kubeclientset "k8s.io/kubernetes/pkg/client/clientset_generated/release_1_5"
	"k8s.io/kubernetes/pkg/client/record"
	"k8s.io/kubernetes/pkg/controller"
	pkg_runtime "k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/types"
	"k8s.io/kubernetes/pkg/util/flowcontrol"
	"k8s.io/kubernetes/pkg/watch"

	"github.com/golang/glog"
)

const (
	allClustersKey = "ALL_CLUSTERS"
)

type NodeController struct {
	// For triggering single node reconciliation. This is used when there is an
	// add/update/delete operation on a node in either federated API server or
	// in some member of the federation.
	nodeDeliverer *util.DelayingDeliverer

	// For triggering all nodes reconciliation. This is used when
	// a new cluster becomes available.
	clusterDeliverer *util.DelayingDeliverer

	// Contains nodes present in members of federation.
	nodeFederatedInformer util.FederatedInformer
	// For updating members of federation.
	federatedUpdater util.FederatedUpdater
	// Definitions of nodes that should be federated.
	nodeInformerStore cache.Store
	// Informer controller for nodes that should be federated.
	nodeInformerController cache.ControllerInterface

	// Client to federated api server.
	federatedApiClient federationclientset.Interface

	// Backoff manager for nodes
	nodeBackoff *flowcontrol.Backoff

	// For events
	eventRecorder record.EventRecorder

	nodeReviewDelay  time.Duration
	clusterAvailableDelay time.Duration
	smallDelay            time.Duration
	updateTimeout         time.Duration
}

// NewNodeController returns a new node controller
func NewNodeController(client federationclientset.Interface) *NodeController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(eventsink.NewFederatedEventSink(client))
	recorder := broadcaster.NewRecorder(api.EventSource{Component: "federated-nodes-controller"})

	nodecontroller := &NodeController{
		federatedApiClient:    client,
		nodeReviewDelay:  time.Second * 10,
		clusterAvailableDelay: time.Second * 20,
		smallDelay:            time.Second * 3,
		updateTimeout:         time.Second * 30,
		nodeBackoff:      flowcontrol.NewBackOff(5*time.Second, time.Minute),
		eventRecorder:         recorder,
	}

	// Build delivereres for triggering reconciliations.
	nodecontroller.nodeDeliverer = util.NewDelayingDeliverer()
	nodecontroller.clusterDeliverer = util.NewDelayingDeliverer()

	// Start informer on federated API servers on nodes that should be federated.
	nodecontroller.nodeInformerStore, nodecontroller.nodeInformerController = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options api.ListOptions) (pkg_runtime.Object, error) {
				versionedOptions := util.VersionizeV1ListOptions(options)
				return client.Core().Nodes().List(versionedOptions)
			},
			WatchFunc: func(options api.ListOptions) (watch.Interface, error) {
				versionedOptions := util.VersionizeV1ListOptions(options)
				return client.Core().Nodes().Watch(versionedOptions)
			},
		},
		&api_v1.Node{},
		controller.NoResyncPeriodFunc(),
		util.NewTriggerOnAllChanges(func(obj pkg_runtime.Object) { nodecontroller.deliverNodeObj(obj, 0, false) }))

	// Federated informer on nodes in members of federation.
	nodecontroller.nodeFederatedInformer = util.NewFederatedInformer(
		client,
		func(cluster *federation_api.Cluster, targetClient kubeclientset.Interface) (cache.Store, cache.ControllerInterface) {
			return cache.NewInformer(
				&cache.ListWatch{
					ListFunc: func(options api.ListOptions) (pkg_runtime.Object, error) {
						versionedOptions := util.VersionizeV1ListOptions(options)
						return targetClient.Core().Nodes().List(versionedOptions)
					},
					WatchFunc: func(options api.ListOptions) (watch.Interface, error) {
						versionedOptions := util.VersionizeV1ListOptions(options)
						return targetClient.Core().Nodes().Watch(versionedOptions)
					},
				},
				&api_v1.Node{},
				controller.NoResyncPeriodFunc(),
				// Trigger reconciliation whenever something in federated cluster is changed. In most cases it
				// would be just confirmation that some node opration succeeded.
				util.NewTriggerOnAllChanges(
					func(obj pkg_runtime.Object) {
						nodecontroller.deliverNodeObj(obj, nodecontroller.nodeReviewDelay, false)
					},
				))
		},

		&util.ClusterLifecycleHandlerFuncs{
			ClusterAvailable: func(cluster *federation_api.Cluster) {
				// When new cluster becomes available process all the nodes again.
				nodecontroller.clusterDeliverer.DeliverAt(allClustersKey, nil, time.Now().Add(nodecontroller.clusterAvailableDelay))
			},
		},
	)

	// Federated updater along with Create/Update/Delete operations.
	nodecontroller.federatedUpdater = util.NewFederatedUpdater(nodecontroller.nodeFederatedInformer,
		func(client kubeclientset.Interface, obj pkg_runtime.Object) error {
			node := obj.(*api_v1.Node)
			_, err := client.Core().Nodes().Create(node)
			return err
		},
		func(client kubeclientset.Interface, obj pkg_runtime.Object) error {
			node := obj.(*api_v1.Node)
			_, err := client.Core().Nodes().Update(node)
			return err
		},
		func(client kubeclientset.Interface, obj pkg_runtime.Object) error {
			node := obj.(*api_v1.Node)
			err := client.Core().Nodes().Delete(node.Name, &api_v1.DeleteOptions{})
			return err
		})
	return nodecontroller
}

func (nodecontroller *NodeController) Run(stopChan <-chan struct{}) {
	go nodecontroller.nodeInformerController.Run(stopChan)
	nodecontroller.nodeFederatedInformer.Start()
	go func() {
		<-stopChan
		nodecontroller.nodeFederatedInformer.Stop()
	}()
	nodecontroller.nodeDeliverer.StartWithHandler(func(item *util.DelayingDelivererItem) {
		node := item.Value.(*types.NamespacedName)
		nodecontroller.reconcileNode(*node)
	})
	nodecontroller.clusterDeliverer.StartWithHandler(func(_ *util.DelayingDelivererItem) {
		nodecontroller.reconcileNodesOnClusterChange()
	})
	util.StartBackoffGC(nodecontroller.nodeBackoff, stopChan)
}

func (nodecontroller *NodeController) deliverNodeObj(obj interface{}, delay time.Duration, failed bool) {
	node := obj.(*api_v1.Node)
	nodecontroller.deliverNode(types.NamespacedName{Namespace: node.Namespace, Name: node.Name}, delay, failed)
}

// Adds backoff to delay if this delivery is related to some failure. Resets backoff if there was no failure.
func (nodecontroller *NodeController) deliverNode(node types.NamespacedName, delay time.Duration, failed bool) {
	key := node.String()
	if failed {
		nodecontroller.nodeBackoff.Next(key, time.Now())
		delay = delay + nodecontroller.nodeBackoff.Get(key)
	} else {
		nodecontroller.nodeBackoff.Reset(key)
	}
	nodecontroller.nodeDeliverer.DeliverAfter(key, &node, delay)
}

// Check whether all data stores are in sync. False is returned if any of the informer/stores is not yet
// synced with the corresponding api server.
func (nodecontroller *NodeController) isSynced() bool {
	if !nodecontroller.nodeFederatedInformer.ClustersSynced() {
		glog.V(2).Infof("Cluster list not synced")
		return false
	}
	clusters, err := nodecontroller.nodeFederatedInformer.GetReadyClusters()
	if err != nil {
		glog.Errorf("Failed to get ready clusters: %v", err)
		return false
	}
	if !nodecontroller.nodeFederatedInformer.GetTargetStore().ClustersSynced(clusters) {
		return false
	}
	return true
}

// The function triggers reconciliation of all federated nodes.
func (nodecontroller *NodeController) reconcileNodesOnClusterChange() {
	if !nodecontroller.isSynced() {
		glog.V(4).Infof("Node controller not synced")
		nodecontroller.clusterDeliverer.DeliverAt(allClustersKey, nil, time.Now().Add(nodecontroller.clusterAvailableDelay))
	}
	for _, obj := range nodecontroller.nodeInformerStore.List() {
		node := obj.(*api_v1.Node)
		nodecontroller.deliverNode(types.NamespacedName{Namespace: node.Namespace, Name: node.Name},
			nodecontroller.smallDelay, false)
	}
}

func (nodecontroller *NodeController) reconcileNode(node types.NamespacedName) {

	if !nodecontroller.isSynced() {
		glog.V(4).Infof("Node controller not synced")
		nodecontroller.deliverNode(node, nodecontroller.clusterAvailableDelay, false)
		return
	}

	key := node.String()
	baseNodeObj, exist, err := nodecontroller.nodeInformerStore.GetByKey(key)
	if err != nil {
		glog.Errorf("Failed to query main node store for %v: %v", key, err)
		nodecontroller.deliverNode(node, 0, true)
		return
	}

	if !exist {
		// Not federated node, ignoring.
		glog.V(8).Infof("Skipping not federated node: %s", key)
		return
	}
	baseNode := baseNodeObj.(*api_v1.Node)

	clusters, err := nodecontroller.nodeFederatedInformer.GetReadyClusters()
	if err != nil {
		glog.Errorf("Failed to get cluster list: %v, retrying shortly", err)
		nodecontroller.deliverNode(node, nodecontroller.clusterAvailableDelay, false)
		return
	}

	operations := make([]util.FederatedOperation, 0)
	for _, cluster := range clusters {
		clusterNodeObj, found, err := nodecontroller.nodeFederatedInformer.GetTargetStore().GetByKey(cluster.Name, key)
		if err != nil {
			glog.Errorf("Failed to get %s from %s: %v, retrying shortly", key, cluster.Name, err)
			nodecontroller.deliverNode(node, 0, true)
			return
		}

		desiredNode := &api_v1.Node{
			ObjectMeta: util.CopyObjectMeta(baseNode.ObjectMeta),
		}

		if !found {
			nodecontroller.eventRecorder.Eventf(baseNode, api.EventTypeNormal, "CreateInCluster",
				"Creating node in cluster %s", cluster.Name)

			operations = append(operations, util.FederatedOperation{
				Type:        util.OperationTypeAdd,
				Obj:         desiredNode,
				ClusterName: cluster.Name,
			})
		} else {
			clusterNode := clusterNodeObj.(*api_v1.Node)

			// Update existing node, if needed.
			if !util.NodeEquivalent(desiredNode, clusterNode) {
				nodecontroller.eventRecorder.Eventf(baseNode, api.EventTypeNormal, "UpdateInCluster",
					"Updating node in cluster %s", cluster.Name)
				operations = append(operations, util.FederatedOperation{
					Type:        util.OperationTypeUpdate,
					Obj:         desiredNode,
					ClusterName: cluster.Name,
				})
			}
		}
	}

	if len(operations) == 0 {
		// Everything is in order
		glog.V(8).Infof("No operations needed for %s", key)
		return
	}
	err = nodecontroller.federatedUpdater.UpdateWithOnError(operations, nodecontroller.updateTimeout,
		func(op util.FederatedOperation, operror error) {
			nodecontroller.eventRecorder.Eventf(baseNode, api.EventTypeNormal, "UpdateInClusterFailed",
				"Node update in cluster %s failed: %v", op.ClusterName, operror)
		})

	if err != nil {
		glog.Errorf("Failed to execute updates for %s: %v, retrying shortly", key, err)
		nodecontroller.deliverNode(node, 0, true)
		return
	}
}
