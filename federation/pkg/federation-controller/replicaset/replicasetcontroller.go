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

package replicaset

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/golang/glog"

	fed "k8s.io/kubernetes/federation/apis/federation"
	fedv1 "k8s.io/kubernetes/federation/apis/federation/v1beta1"
	fedclientset "k8s.io/kubernetes/federation/client/clientset_generated/federation_release_1_5"
	fedutil "k8s.io/kubernetes/federation/pkg/federation-controller/util"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util/deletionhelper"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util/eventsink"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util/planner"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util/podanalyzer"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/errors"
	apiv1 "k8s.io/kubernetes/pkg/api/v1"
	extensionsv1 "k8s.io/kubernetes/pkg/apis/extensions/v1beta1"
	"k8s.io/kubernetes/pkg/client/cache"
	kubeclientset "k8s.io/kubernetes/pkg/client/clientset_generated/release_1_5"
	"k8s.io/kubernetes/pkg/client/record"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/conversion"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/util/flowcontrol"
	"k8s.io/kubernetes/pkg/util/wait"
	"k8s.io/kubernetes/pkg/util/workqueue"
	"k8s.io/kubernetes/pkg/watch"

	"sync"
)

const (
	FedReplicaSetPreferencesAnnotation = "federation.kubernetes.io/replica-set-preferences"
	allClustersKey                     = "THE_ALL_CLUSTER_KEY"
	UserAgentName                      = "Federation-replicaset-Controller"
)

var (
	replicaSetReviewDelay    = 10 * time.Second
	clusterAvailableDelay    = 20 * time.Second
	clusterUnavailableDelay  = 60 * time.Second
	allReplicaSetReviewDelay = 2 * time.Minute
	updateTimeout            = 30 * time.Second
)

func parseFederationReplicaSetReference(frs *extensionsv1.ReplicaSet) (*fed.FederatedReplicaSetPreferences, error) {
	if frs.Annotations == nil {
		return nil, nil
	}
	frsPrefString, found := frs.Annotations[FedReplicaSetPreferencesAnnotation]
	if !found {
		return nil, nil
	}
	var frsPref fed.FederatedReplicaSetPreferences
	if err := json.Unmarshal([]byte(frsPrefString), &frsPref); err != nil {
		return nil, err
	}
	return &frsPref, nil
}

type ReplicaSetController struct {
	fedClient fedclientset.Interface

	replicaSetController *cache.Controller
	replicaSetStore      cache.StoreToReplicaSetLister

	fedReplicaSetInformer fedutil.FederatedInformer
	fedPodInformer        fedutil.FederatedInformer

	replicasetDeliverer *fedutil.DelayingDeliverer
	clusterDeliverer    *fedutil.DelayingDeliverer
	replicasetWorkQueue workqueue.Interface
	// For updating members of federation.
	fedUpdater fedutil.FederatedUpdater

	replicaSetBackoff *flowcontrol.Backoff
	// For events
	eventRecorder record.EventRecorder

	deletionHelper *deletionhelper.DeletionHelper

	defaultPlanner *planner.Planner
	// enableResourceMetricBasedScheduling enables Distribution of replicas to different clusters based on
	// the accumulated available resources of each cluster
	enableResourceMetricBasedScheduling bool
	// clusterResourceRefreshPeriod is the period for getting the latest resources from each cluster in replica controller.
	clusterResourceRefreshPeriod time.Duration

	schedData MetricSchedulerData
}

// NewclusterController returns a new cluster controller
func NewReplicaSetController(federationClient fedclientset.Interface, refreshDuration time.Duration,
	enableMetricBasedScheduler bool) *ReplicaSetController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(eventsink.NewFederatedEventSink(federationClient))
	recorder := broadcaster.NewRecorder(api.EventSource{Component: "federated-replicaset-controller"})

	frsc := &ReplicaSetController{
		fedClient:           federationClient,
		replicasetDeliverer: fedutil.NewDelayingDeliverer(),
		clusterDeliverer:    fedutil.NewDelayingDeliverer(),
		replicasetWorkQueue: workqueue.New(),
		replicaSetBackoff:   flowcontrol.NewBackOff(5*time.Second, time.Minute),
		defaultPlanner: planner.NewPlanner(&fed.FederatedReplicaSetPreferences{
			Clusters: map[string]fed.ClusterReplicaSetPreferences{
				"*": {Weight: 1},
			},
		}),
		eventRecorder:                       recorder,
		enableResourceMetricBasedScheduling: enableMetricBasedScheduler,
		clusterResourceRefreshPeriod:        refreshDuration,
	}

	replicaSetFedInformerFactory := func(cluster *fedv1.Cluster, clientset kubeclientset.Interface) (cache.Store, cache.ControllerInterface) {
		return cache.NewInformer(
			&cache.ListWatch{
				ListFunc: func(options api.ListOptions) (runtime.Object, error) {
					versionedOptions := fedutil.VersionizeV1ListOptions(options)
					return clientset.Extensions().ReplicaSets(apiv1.NamespaceAll).List(versionedOptions)
				},
				WatchFunc: func(options api.ListOptions) (watch.Interface, error) {
					versionedOptions := fedutil.VersionizeV1ListOptions(options)
					return clientset.Extensions().ReplicaSets(apiv1.NamespaceAll).Watch(versionedOptions)
				},
			},
			&extensionsv1.ReplicaSet{},
			controller.NoResyncPeriodFunc(),
			fedutil.NewTriggerOnAllChanges(
				func(obj runtime.Object) { frsc.deliverLocalReplicaSet(obj, replicaSetReviewDelay) },
			),
		)
	}
	clusterLifecycle := fedutil.ClusterLifecycleHandlerFuncs{
		ClusterAvailable: func(cluster *fedv1.Cluster) {
			frsc.clusterDeliverer.DeliverAfter(allClustersKey, nil, clusterAvailableDelay)
		},
		ClusterUnavailable: func(cluster *fedv1.Cluster, _ []interface{}) {
			frsc.clusterDeliverer.DeliverAfter(allClustersKey, nil, clusterUnavailableDelay)
		},
	}
	frsc.fedReplicaSetInformer = fedutil.NewFederatedInformer(federationClient, replicaSetFedInformerFactory, &clusterLifecycle)

	podFedInformerFactory := func(cluster *fedv1.Cluster, clientset kubeclientset.Interface) (cache.Store, cache.ControllerInterface) {
		return cache.NewInformer(
			&cache.ListWatch{
				ListFunc: func(options api.ListOptions) (runtime.Object, error) {
					versionedOptions := fedutil.VersionizeV1ListOptions(options)
					return clientset.Core().Pods(apiv1.NamespaceAll).List(versionedOptions)
				},
				WatchFunc: func(options api.ListOptions) (watch.Interface, error) {
					versionedOptions := fedutil.VersionizeV1ListOptions(options)
					return clientset.Core().Pods(apiv1.NamespaceAll).Watch(versionedOptions)
				},
			},
			&apiv1.Pod{},
			controller.NoResyncPeriodFunc(),
			fedutil.NewTriggerOnAllChanges(
				func(obj runtime.Object) {
					frsc.clusterDeliverer.DeliverAfter(allClustersKey, nil, allReplicaSetReviewDelay)
				},
			),
		)
	}
	frsc.fedPodInformer = fedutil.NewFederatedInformer(federationClient, podFedInformerFactory, &fedutil.ClusterLifecycleHandlerFuncs{})

	frsc.replicaSetStore.Indexer, frsc.replicaSetController = cache.NewIndexerInformer(
		&cache.ListWatch{
			ListFunc: func(options api.ListOptions) (runtime.Object, error) {
				versionedOptions := fedutil.VersionizeV1ListOptions(options)
				return frsc.fedClient.Extensions().ReplicaSets(apiv1.NamespaceAll).List(versionedOptions)
			},
			WatchFunc: func(options api.ListOptions) (watch.Interface, error) {
				versionedOptions := fedutil.VersionizeV1ListOptions(options)
				return frsc.fedClient.Extensions().ReplicaSets(apiv1.NamespaceAll).Watch(versionedOptions)
			},
		},
		&extensionsv1.ReplicaSet{},
		controller.NoResyncPeriodFunc(),
		fedutil.NewTriggerOnMetaAndSpecChanges(
			func(obj runtime.Object) { frsc.deliverFedReplicaSetObj(obj, replicaSetReviewDelay) },
		),
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)

	frsc.fedUpdater = fedutil.NewFederatedUpdater(frsc.fedReplicaSetInformer,
		func(client kubeclientset.Interface, obj runtime.Object) error {
			rs := obj.(*extensionsv1.ReplicaSet)
			_, err := client.Extensions().ReplicaSets(rs.Namespace).Create(rs)
			return err
		},
		func(client kubeclientset.Interface, obj runtime.Object) error {
			rs := obj.(*extensionsv1.ReplicaSet)
			_, err := client.Extensions().ReplicaSets(rs.Namespace).Update(rs)
			return err
		},
		func(client kubeclientset.Interface, obj runtime.Object) error {
			rs := obj.(*extensionsv1.ReplicaSet)
			err := client.Extensions().ReplicaSets(rs.Namespace).Delete(rs.Name, &apiv1.DeleteOptions{})
			return err
		})

	frsc.deletionHelper = deletionhelper.NewDeletionHelper(
		frsc.hasFinalizerFunc,
		frsc.removeFinalizerFunc,
		frsc.addFinalizerFunc,
		// objNameFunc
		func(obj runtime.Object) string {
			replicaset := obj.(*extensionsv1.ReplicaSet)
			return replicaset.Name
		},
		updateTimeout,
		frsc.eventRecorder,
		frsc.fedReplicaSetInformer,
		frsc.fedUpdater,
	)

	return frsc
}

// Returns true if the given object has the given finalizer in its ObjectMeta.
func (frsc *ReplicaSetController) hasFinalizerFunc(obj runtime.Object, finalizer string) bool {
	replicaset := obj.(*extensionsv1.ReplicaSet)
	for i := range replicaset.ObjectMeta.Finalizers {
		if string(replicaset.ObjectMeta.Finalizers[i]) == finalizer {
			return true
		}
	}
	return false
}

// Removes the finalizer from the given objects ObjectMeta.
// Assumes that the given object is a replicaset.
func (frsc *ReplicaSetController) removeFinalizerFunc(obj runtime.Object, finalizer string) (runtime.Object, error) {
	replicaset := obj.(*extensionsv1.ReplicaSet)
	newFinalizers := []string{}
	hasFinalizer := false
	for i := range replicaset.ObjectMeta.Finalizers {
		if string(replicaset.ObjectMeta.Finalizers[i]) != finalizer {
			newFinalizers = append(newFinalizers, replicaset.ObjectMeta.Finalizers[i])
		} else {
			hasFinalizer = true
		}
	}
	if !hasFinalizer {
		// Nothing to do.
		return obj, nil
	}
	replicaset.ObjectMeta.Finalizers = newFinalizers
	replicaset, err := frsc.fedClient.Extensions().ReplicaSets(replicaset.Namespace).Update(replicaset)
	if err != nil {
		return nil, fmt.Errorf("failed to remove finalizer %s from replicaset %s: %v", finalizer, replicaset.Name, err)
	}
	return replicaset, nil
}

// Adds the given finalizer to the given objects ObjectMeta.
// Assumes that the given object is a replicaset.
func (frsc *ReplicaSetController) addFinalizerFunc(obj runtime.Object, finalizer string) (runtime.Object, error) {
	replicaset := obj.(*extensionsv1.ReplicaSet)
	replicaset.ObjectMeta.Finalizers = append(replicaset.ObjectMeta.Finalizers, finalizer)
	replicaset, err := frsc.fedClient.Extensions().ReplicaSets(replicaset.Namespace).Update(replicaset)
	if err != nil {
		return nil, fmt.Errorf("failed to add finalizer %s to replicaset %s: %v", finalizer, replicaset.Name, err)
	}
	return replicaset, nil
}

func (frsc *ReplicaSetController) Run(workers int, stopCh <-chan struct{}) {
	go frsc.replicaSetController.Run(stopCh)
	frsc.fedReplicaSetInformer.Start()
	frsc.fedPodInformer.Start()

	frsc.replicasetDeliverer.StartWithHandler(func(item *fedutil.DelayingDelivererItem) {
		frsc.replicasetWorkQueue.Add(item.Key)
	})
	frsc.clusterDeliverer.StartWithHandler(func(_ *fedutil.DelayingDelivererItem) {
		frsc.reconcileReplicaSetsOnClusterChange()
	})

	for !frsc.isSynced() {
		time.Sleep(5 * time.Millisecond)
	}

	for i := 0; i < workers; i++ {
		go wait.Until(frsc.worker, time.Second, stopCh)
	}

	if frsc.enableResourceMetricBasedScheduling {
		go wait.Forever(func() { frsc.updateClusterResources() }, frsc.clusterResourceRefreshPeriod)
	}
	fedutil.StartBackoffGC(frsc.replicaSetBackoff, stopCh)

	<-stopCh
	glog.Infof("Shutting down ReplicaSetController")
	frsc.replicasetDeliverer.Stop()
	frsc.clusterDeliverer.Stop()
	frsc.replicasetWorkQueue.ShutDown()
	frsc.fedReplicaSetInformer.Stop()
	frsc.fedPodInformer.Stop()
}

type ClusterResourceRequest struct {
	clusterName string
	wg          *sync.WaitGroup
	res         []ClusterNodesResources
	required    *ClusterNodesResources
	respChan    chan ClusterResourceResponse
}
type ClusterResourceResponse struct {
	clusterName string
	weight      int64
}
type ClusterNodesResources struct {
	nodeName  string
	milliCpu  int64
	mem       int64
	numOfPods int64
	nvidiaGPU int64
}

type ClusterResponse struct {
	clusterName string
	res         []ClusterNodesResources
	err         error
}

type ClusterRequest struct {
	clusterName string
	frscLocal   *ReplicaSetController
	wg          *sync.WaitGroup
	respChan    chan ClusterResponse
}

type MetricSchedulerData struct {
	sync.Mutex
	clustersResources map[string][]ClusterNodesResources
}

func createpodsMapOfNodeName(podsInclust *apiv1.PodList) map[string][]*apiv1.Pod {
	podsNodeName := make(map[string][]*apiv1.Pod)

	for _, pod := range podsInclust.Items {
		nodeName := pod.Spec.NodeName
		if _, ok := podsNodeName[nodeName]; !ok {
			podsNodeName[nodeName] = make([]*apiv1.Pod, 0, 1)
			podsNodeName[nodeName] = append(podsNodeName[nodeName], &pod)
		} else {
			podsNodeName[nodeName] = append(podsNodeName[nodeName], &pod)
		}
	}
	return podsNodeName
}
func (frsc *ReplicaSetController) updateClusterResources() {

	clusters, err := frsc.fedPodInformer.GetReadyClusters()
	if err != nil {
		glog.Errorf("Failed GetReadyClusters in updateClusterResources: %v", err)
		return
	}
	var numReq int
	var numResp int
	numReq = len(clusters)

	respChan := make(chan ClusterResponse, numReq)
	var wg sync.WaitGroup

	//wg.Add(numReq)
	for _, cluster := range clusters {
		wg.Add(1)
		var req ClusterRequest
		req.clusterName = cluster.Name
		req.frscLocal = frsc
		req.wg = &wg
		req.respChan = respChan

		go func(request ClusterRequest) {

			clusterName := request.clusterName
			defer request.wg.Done()
			client, err := request.frscLocal.fedPodInformer.GetClientsetForCluster(clusterName)
			if err != nil {
				glog.Errorf("Failed GetClientsetForCluster in updateClusterResources: %v for cluster %s", err, clusterName)
				request.respChan <- ClusterResponse{clusterName, nil, err}
				return
			}
			//get all the nodes in a cluster
			nodeList, err := client.CoreV1().Nodes().List(apiv1.ListOptions{})
			if err != nil {
				glog.Errorf("Failed to get the Nodes List in updateClusterResources: %v for cluster %s", err, clusterName)
				request.respChan <- ClusterResponse{clusterName, nil, err}
				return
			}
			glog.V(2).Infof("Num of Nodes %d in a cluster %s", len(nodeList.Items), clusterName)
			//get all the deployed pods in a cluster
			podCustList, err := client.Core().Pods(apiv1.NamespaceAll).List(apiv1.ListOptions{})
			if err != nil {
				glog.Errorf("Failed to get all Pods deployed in a cluster in updateClusterResources: %v for cluster %s", err, clusterName)
				request.respChan <- ClusterResponse{clusterName, nil, err}
				return
			}
			glog.V(2).Infof("Num of pods %d deployed in a cluster %s", len(podCustList.Items), clusterName)

			podsMap := createpodsMapOfNodeName(podCustList)
			clustRes := make([]ClusterNodesResources, 0, len(nodeList.Items))
			for i, _ := range nodeList.Items {
				var res api.ResourceList
				nodeName := nodeList.Items[i].Name

				err = apiv1.Convert_v1_ResourceList_To_api_ResourceList(&(nodeList.Items[i].Status.Allocatable), &res, nil)
				if err != nil {
					glog.Errorf("Failed to Convert_v1_ResourceList_To_api_ResourceList in updateClusterResources: %v for cluster %s", err, clusterName)
					request.respChan <- ClusterResponse{clusterName, nil, err}
					return
				}

				var nodeUsedRes ClusterNodesResources
				var nodeAvailRes ClusterNodesResources

				nodeUsedRes.nodeName = nodeName
				nodeAvailRes = ClusterNodesResources{nodeName, res.Cpu().MilliValue(),
					res.Memory().Value(), res.Pods().Value(), res.NvidiaGPU().Value()}

				podsInNode, _ := podsMap[nodeName]
				glog.V(2).Infof("Num of pods %d in a Node %s in cluster %s", len(podsInNode), nodeName, clusterName)
				for _, pod := range podsInNode {
					podRes := GetRequiredResourceInfo(pod.Spec)
					nodeUsedRes.milliCpu += podRes.milliCpu
					nodeUsedRes.mem += podRes.mem
					nodeUsedRes.numOfPods += podRes.numOfPods
					nodeUsedRes.nvidiaGPU += podRes.nvidiaGPU
				}
				glog.V(2).Infof("Used resources %v in a Node %s in cluster:%s", nodeUsedRes, nodeName, clusterName)

				if (nodeAvailRes.milliCpu - nodeUsedRes.milliCpu) < 0 {
					nodeAvailRes.milliCpu = 0
				} else {
					nodeAvailRes.milliCpu -= nodeUsedRes.milliCpu
				}

				if (nodeAvailRes.mem - nodeUsedRes.mem) < 0 {
					nodeAvailRes.mem = 0
				} else {
					nodeAvailRes.mem -= nodeUsedRes.mem
				}

				if (nodeAvailRes.numOfPods - nodeUsedRes.numOfPods) < 0 {
					nodeAvailRes.numOfPods = 0
				} else {
					nodeAvailRes.numOfPods -= nodeUsedRes.numOfPods
				}

				if (nodeAvailRes.nvidiaGPU - nodeUsedRes.nvidiaGPU) < 0 {
					nodeAvailRes.nvidiaGPU = 0
				} else {
					nodeAvailRes.nvidiaGPU -= nodeUsedRes.nvidiaGPU
				}

				clustRes = append(clustRes, nodeAvailRes)
			}
			request.respChan <- ClusterResponse{clusterName, clustRes, err}
		}(req)

	}
	//wait till all the resources received from all the clusters
	wg.Wait()

	clustersRes := make(map[string][]ClusterNodesResources)
	for i := 0; i < numReq; i++ {
		numResp++
		resp := <-respChan

		if resp.err != nil {
			glog.Errorf("Failed to get the Nodes List in updateClusterResources: %v for cluster %s", err, resp.clusterName)
		} else {
			clustersRes[resp.clusterName] = resp.res
		}
		// once all the responses are read close the channel
		if numResp == numReq {
			close(respChan)
		}
	}

	//store the cluster resources global schedData
	frsc.schedData.Lock()
	frsc.schedData.clustersResources = clustersRes
	frsc.schedData.Unlock()

}
func GetRequiredResourceInfo(podSpec apiv1.PodSpec) *ClusterNodesResources {
	result := ClusterNodesResources{}
	for _, container := range podSpec.Containers {
		for rName, rQuantity := range container.Resources.Requests {
			switch rName {
			case apiv1.ResourceMemory:
				result.mem += rQuantity.Value()
			case apiv1.ResourceCPU:
				result.milliCpu += rQuantity.MilliValue()
			case apiv1.ResourceNvidiaGPU:
				result.nvidiaGPU += rQuantity.Value()
			case apiv1.ResourcePods:
				result.numOfPods += rQuantity.Value()
			default:

			}
		}
	}
	// take max_resource(sum_pod, any_init_container)
	for _, container := range podSpec.InitContainers {
		for rName, rQuantity := range container.Resources.Requests {
			switch rName {
			case apiv1.ResourceMemory:
				if mem := rQuantity.Value(); mem > result.mem {
					result.mem = mem
				}
			case apiv1.ResourceCPU:
				if cpu := rQuantity.MilliValue(); cpu > result.milliCpu {
					result.milliCpu = cpu
				}
			case apiv1.ResourceNvidiaGPU:
				if gpu := rQuantity.Value(); gpu > result.nvidiaGPU {
					result.nvidiaGPU = gpu
				}
			case apiv1.ResourcePods:
				if pods := rQuantity.Value(); pods > result.numOfPods {
					result.numOfPods = pods
				}
			default:

			}
		}
	}
	glog.V(2).Infof("In GetRequiredResourceInfo Replica required resources %v", result)
	return &result
}

func minInt64(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
func gcdForTwoNums(x, y int64) int64 {
	var t int64
	if x < y {
		t = x
		x = y
		y = t
	}

	for y != 0 {
		t = x % y
		x = y
		y = t
	}
	return x
}
func gcdVal_FromASlice(s []int64) int64 {
	n := len(s)
	if n == 1 {
		return s[0]
	}
	first := s[0]
	for i := 1; i < n; i++ {
		first = gcdForTwoNums(first, s[i])
	}
	return first
}

//http://kubernetes.io/docs/user-guide/compute-resources/
var defaultMilliCpu int64 = 100 //
var defaultmemory int64 = 1000  //1000 bytes

func (frsc *ReplicaSetController) calculatePrefBasedOnResources(podSpec apiv1.PodSpec) map[string]fed.ClusterReplicaSetPreferences {

	required := GetRequiredResourceInfo(podSpec)

	if required.milliCpu == 0 {
		required.milliCpu = defaultMilliCpu
	}

	if required.mem == 0 {
		required.mem = defaultmemory
	}

	frsc.schedData.Lock()
	resources := frsc.schedData.clustersResources
	frsc.schedData.Unlock()

	var numReq int
	var numResp int
	numReq = len(resources)

	respChan := make(chan ClusterResourceResponse, numReq)
	var wg sync.WaitGroup

	for name, resource := range resources {
		wg.Add(1)
		var req ClusterResourceRequest
		req.clusterName = name
		req.required = required
		req.res = resource
		req.wg = &wg
		req.respChan = respChan

		go func(request ClusterResourceRequest) {
			var weight int64
			defer request.wg.Done()
			// num of maximum pods can be deployable on each node and each cluster
			for _, resource := range request.res {
				weight += minInt64(resource.milliCpu/request.required.milliCpu, resource.mem/request.required.mem)
			}

			request.respChan <- ClusterResourceResponse{request.clusterName, weight}

		}(req)

	}
	//wait till all the weights calculation for all clusters
	wg.Wait()

	clustPref := make(map[string]fed.ClusterReplicaSetPreferences)
	clustWeights := make([]int64, 0, numReq)
	for i := 0; i < numReq; i++ {
		numResp++
		resp := <-respChan
		clustPref[resp.clusterName] = fed.ClusterReplicaSetPreferences{Weight: resp.weight}
		clustWeights = append(clustWeights, resp.weight)
		// once all the responses are read close the channel
		if numResp == numReq {
			close(respChan)
		}
	}
	//better to normalize the weights in clustPref based on gcd value of clustWeights
	gcdV := gcdVal_FromASlice(clustWeights)

	if (gcdV != 0) && (gcdV != 1) {
		for key, val := range clustPref {
			val.Weight = val.Weight / gcdV
			clustPref[key] = val
		}
	}
	return clustPref

}
func (frsc *ReplicaSetController) isSynced() bool {
	if !frsc.fedReplicaSetInformer.ClustersSynced() {
		glog.V(2).Infof("Cluster list not synced")
		return false
	}
	clusters, err := frsc.fedReplicaSetInformer.GetReadyClusters()
	if err != nil {
		glog.Errorf("Failed to get ready clusters: %v", err)
		return false
	}
	if !frsc.fedReplicaSetInformer.GetTargetStore().ClustersSynced(clusters) {
		return false
	}

	if !frsc.fedPodInformer.ClustersSynced() {
		glog.V(2).Infof("Cluster list not synced")
		return false
	}
	clusters2, err := frsc.fedPodInformer.GetReadyClusters()
	if err != nil {
		glog.Errorf("Failed to get ready clusters: %v", err)
		return false
	}

	// This also checks whether podInformer and replicaSetInformer have the
	// same cluster lists.
	if !frsc.fedPodInformer.GetTargetStore().ClustersSynced(clusters) {
		return false
	}
	if !frsc.fedPodInformer.GetTargetStore().ClustersSynced(clusters2) {
		return false
	}

	if !frsc.replicaSetController.HasSynced() {
		glog.V(2).Infof("federation replicaset list not synced")
		return false
	}
	return true
}

func (frsc *ReplicaSetController) deliverLocalReplicaSet(obj interface{}, duration time.Duration) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		glog.Errorf("Couldn't get key for object %v: %v", obj, err)
		return
	}
	_, exists, err := frsc.replicaSetStore.Indexer.GetByKey(key)
	if err != nil {
		glog.Errorf("Couldn't get federation replicaset %v: %v", key, err)
		return
	}
	if exists { // ignore replicasets exists only in local k8s
		frsc.deliverReplicaSetByKey(key, duration, false)
	}
}

func (frsc *ReplicaSetController) deliverFedReplicaSetObj(obj interface{}, delay time.Duration) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		glog.Errorf("Couldn't get key for object %+v: %v", obj, err)
		return
	}
	frsc.deliverReplicaSetByKey(key, delay, false)
}

func (frsc *ReplicaSetController) deliverReplicaSetByKey(key string, delay time.Duration, failed bool) {
	if failed {
		frsc.replicaSetBackoff.Next(key, time.Now())
		delay = delay + frsc.replicaSetBackoff.Get(key)
	} else {
		frsc.replicaSetBackoff.Reset(key)
	}
	frsc.replicasetDeliverer.DeliverAfter(key, nil, delay)
}

func (frsc *ReplicaSetController) worker() {
	for {
		item, quit := frsc.replicasetWorkQueue.Get()
		if quit {
			return
		}
		key := item.(string)
		status, err := frsc.reconcileReplicaSet(key)
		frsc.replicasetWorkQueue.Done(item)
		if err != nil {
			glog.Errorf("Error syncing cluster controller: %v", err)
			frsc.deliverReplicaSetByKey(key, 0, true)
		} else {
			switch status {
			case statusAllOk:
				break
			case statusError:
				frsc.deliverReplicaSetByKey(key, 0, true)
			case statusNeedRecheck:
				frsc.deliverReplicaSetByKey(key, replicaSetReviewDelay, false)
			case statusNotSynced:
				frsc.deliverReplicaSetByKey(key, clusterAvailableDelay, false)
			default:
				glog.Errorf("Unhandled reconciliation status: %s", status)
				frsc.deliverReplicaSetByKey(key, replicaSetReviewDelay, false)
			}
		}
	}
}

func (frsc *ReplicaSetController) schedule(frs *extensionsv1.ReplicaSet, clusters []*fedv1.Cluster,
	current map[string]int64, estimatedCapacity map[string]int64) map[string]int64 {
	// TODO: integrate real scheduler

	plnr := frsc.defaultPlanner
	frsPref, err := parseFederationReplicaSetReference(frs)
	if err != nil {
		glog.Info("Invalid ReplicaSet specific preference, use default. rs: %v, err: %v", frs, err)
	}
	if frsPref != nil { // create a new planner if user specified a preference
		plnr = planner.NewPlanner(frsPref)
	} else if frsc.enableResourceMetricBasedScheduling {
		clustPref := frsc.calculatePrefBasedOnResources(frs.Spec.Template.Spec)
		glog.V(2).Infof("Cluster preferences based on scheduler %v", clustPref)
		plnr = planner.NewPlanner(&fed.FederatedReplicaSetPreferences{Clusters: clustPref})
	}

	replicas := int64(*frs.Spec.Replicas)
	var clusterNames []string
	for _, cluster := range clusters {
		clusterNames = append(clusterNames, cluster.Name)
	}
	scheduleResult, overflow := plnr.Plan(replicas, clusterNames, current, estimatedCapacity,
		frs.Namespace+"/"+frs.Name)
	// make sure the return contains clusters need to zero the replicas
	result := make(map[string]int64)
	for clusterName := range current {
		result[clusterName] = 0
	}
	for clusterName, replicas := range scheduleResult {
		result[clusterName] = replicas
	}
	for clusterName, replicas := range overflow {
		result[clusterName] += replicas
	}
	if glog.V(4) {
		buf := bytes.NewBufferString(fmt.Sprintf("Schedule - ReplicaSet: %s/%s\n", frs.Namespace, frs.Name))
		sort.Strings(clusterNames)
		for _, clusterName := range clusterNames {
			cur := current[clusterName]
			target := scheduleResult[clusterName]
			fmt.Fprintf(buf, "%s: current: %d target: %d", clusterName, cur, target)
			if over, found := overflow[clusterName]; found {
				fmt.Fprintf(buf, " overflow: %d", over)
			}
			if capacity, found := estimatedCapacity[clusterName]; found {
				fmt.Fprintf(buf, " capacity: %d", capacity)
			}
			fmt.Fprintf(buf, "\n")
		}
		glog.V(4).Infof(buf.String())
	}
	return result
}

type reconciliationStatus string

const (
	statusAllOk       = reconciliationStatus("ALL_OK")
	statusNeedRecheck = reconciliationStatus("RECHECK")
	statusError       = reconciliationStatus("ERROR")
	statusNotSynced   = reconciliationStatus("NOSYNC")
)

func (frsc *ReplicaSetController) reconcileReplicaSet(key string) (reconciliationStatus, error) {
	if !frsc.isSynced() {
		return statusNotSynced, nil
	}

	glog.V(4).Infof("Start reconcile replicaset %q", key)
	startTime := time.Now()
	defer glog.V(4).Infof("Finished reconcile replicaset %q (%v)", key, time.Now().Sub(startTime))

	objFromStore, exists, err := frsc.replicaSetStore.Indexer.GetByKey(key)
	if err != nil {
		return statusError, err
	}
	if !exists {
		// don't delete local replicasets for now. Do not reconcile it anymore.
		return statusAllOk, nil
	}
	obj, err := conversion.NewCloner().DeepCopy(objFromStore)
	frs, ok := obj.(*extensionsv1.ReplicaSet)
	if err != nil || !ok {
		glog.Errorf("Error in retrieving obj from store: %v, %v", ok, err)
		frsc.deliverReplicaSetByKey(key, 0, true)
		return statusError, err
	}
	if frs.DeletionTimestamp != nil {
		if err := frsc.delete(frs); err != nil {
			glog.Errorf("Failed to delete %s: %v", frs, err)
			frsc.eventRecorder.Eventf(frs, api.EventTypeNormal, "DeleteFailed",
				"ReplicaSet delete failed: %v", err)
			frsc.deliverReplicaSetByKey(key, 0, true)
			return statusError, err
		}
		return statusAllOk, nil
	}

	glog.V(3).Infof("Ensuring delete object from underlying clusters finalizer for replicaset: %s",
		frs.Name)
	// Add the required finalizers before creating a replicaset in underlying clusters.
	updatedRsObj, err := frsc.deletionHelper.EnsureFinalizers(frs)
	if err != nil {
		glog.Errorf("Failed to ensure delete object from underlying clusters finalizer in replicaset %s: %v",
			frs.Name, err)
		frsc.deliverReplicaSetByKey(key, 0, false)
		return statusError, err
	}
	frs = updatedRsObj.(*extensionsv1.ReplicaSet)

	glog.V(3).Infof("Syncing replicaset %s in underlying clusters", frs.Name)

	clusters, err := frsc.fedReplicaSetInformer.GetReadyClusters()
	if err != nil {
		return statusError, err
	}

	// collect current status and do schedule
	allPods, err := frsc.fedPodInformer.GetTargetStore().List()
	if err != nil {
		return statusError, err
	}
	podStatus, err := podanalyzer.AnalysePods(frs.Spec.Selector, allPods, time.Now())
	current := make(map[string]int64)
	estimatedCapacity := make(map[string]int64)
	for _, cluster := range clusters {
		lrsObj, exists, err := frsc.fedReplicaSetInformer.GetTargetStore().GetByKey(cluster.Name, key)
		if err != nil {
			return statusError, err
		}
		if exists {
			lrs := lrsObj.(*extensionsv1.ReplicaSet)
			current[cluster.Name] = int64(podStatus[cluster.Name].RunningAndReady) // include pending as well?
			unschedulable := int64(podStatus[cluster.Name].Unschedulable)
			if unschedulable > 0 {
				estimatedCapacity[cluster.Name] = int64(*lrs.Spec.Replicas) - unschedulable
			}
		}
	}

	scheduleResult := frsc.schedule(frs, clusters, current, estimatedCapacity)

	glog.V(4).Infof("Start syncing local replicaset %s: %v", key, scheduleResult)

	fedStatus := extensionsv1.ReplicaSetStatus{ObservedGeneration: frs.Generation}
	operations := make([]fedutil.FederatedOperation, 0)
	for clusterName, replicas := range scheduleResult {

		lrsObj, exists, err := frsc.fedReplicaSetInformer.GetTargetStore().GetByKey(clusterName, key)
		if err != nil {
			return statusError, err
		}

		// The object can be modified.
		lrs := &extensionsv1.ReplicaSet{
			ObjectMeta: fedutil.DeepCopyRelevantObjectMeta(frs.ObjectMeta),
			Spec:       fedutil.DeepCopyApiTypeOrPanic(frs.Spec).(extensionsv1.ReplicaSetSpec),
		}
		specReplicas := int32(replicas)
		lrs.Spec.Replicas = &specReplicas

		if !exists {
			if replicas > 0 {
				frsc.eventRecorder.Eventf(frs, api.EventTypeNormal, "CreateInCluster",
					"Creating replicaset in cluster %s", clusterName)

				operations = append(operations, fedutil.FederatedOperation{
					Type:        fedutil.OperationTypeAdd,
					Obj:         lrs,
					ClusterName: clusterName,
				})
			}
		} else {
			currentLrs := lrsObj.(*extensionsv1.ReplicaSet)
			// Update existing replica set, if needed.
			if !fedutil.ObjectMetaAndSpecEquivalent(lrs, currentLrs) {
				frsc.eventRecorder.Eventf(frs, api.EventTypeNormal, "UpdateInCluster",
					"Updating replicaset in cluster %s", clusterName)

				operations = append(operations, fedutil.FederatedOperation{
					Type:        fedutil.OperationTypeUpdate,
					Obj:         lrs,
					ClusterName: clusterName,
				})
			}
			fedStatus.Replicas += currentLrs.Status.Replicas
			fedStatus.FullyLabeledReplicas += currentLrs.Status.FullyLabeledReplicas
			// leave the replicaset even the replicas dropped to 0
		}
	}
	if fedStatus.Replicas != frs.Status.Replicas || fedStatus.FullyLabeledReplicas != frs.Status.FullyLabeledReplicas {
		frs.Status = fedStatus
		_, err = frsc.fedClient.Extensions().ReplicaSets(frs.Namespace).UpdateStatus(frs)
		if err != nil {
			return statusError, err
		}
	}

	if len(operations) == 0 {
		// Everything is in order
		return statusAllOk, nil
	}
	err = frsc.fedUpdater.UpdateWithOnError(operations, updateTimeout, func(op fedutil.FederatedOperation, operror error) {
		frsc.eventRecorder.Eventf(frs, api.EventTypeNormal, "FailedUpdateInCluster",
			"Replicaset update in cluster %s failed: %v", op.ClusterName, operror)
	})
	if err != nil {
		glog.Errorf("Failed to execute updates for %s: %v", key, err)
		return statusError, err
	}

	// Some operations were made, reconcile after a while.
	return statusNeedRecheck, nil
}

func (frsc *ReplicaSetController) reconcileReplicaSetsOnClusterChange() {
	if !frsc.isSynced() {
		frsc.clusterDeliverer.DeliverAfter(allClustersKey, nil, clusterAvailableDelay)
	}
	rss := frsc.replicaSetStore.Indexer.List()
	for _, rs := range rss {
		key, _ := controller.KeyFunc(rs)
		frsc.deliverReplicaSetByKey(key, 0, false)
	}
}

// delete deletes the given replicaset or returns error if the deletion was not complete.
func (frsc *ReplicaSetController) delete(replicaset *extensionsv1.ReplicaSet) error {
	glog.V(3).Infof("Handling deletion of replicaset: %v", *replicaset)
	_, err := frsc.deletionHelper.HandleObjectInUnderlyingClusters(replicaset)
	if err != nil {
		return err
	}

	err = frsc.fedClient.Extensions().ReplicaSets(replicaset.Namespace).Delete(replicaset.Name, nil)
	if err != nil {
		// Its all good if the error is not found error. That means it is deleted already and we do not have to do anything.
		// This is expected when we are processing an update as a result of replicaset finalizer deletion.
		// The process that deleted the last finalizer is also going to delete the replicaset and we do not have to do anything.
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete replicaset: %v", err)
		}
	}
	return nil
}
