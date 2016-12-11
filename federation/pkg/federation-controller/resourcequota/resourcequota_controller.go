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

	"encoding/json"

	"fmt"
	"reflect"
	"time"

	fed "k8s.io/kubernetes/federation/apis/federation"
	federation_api "k8s.io/kubernetes/federation/apis/federation/v1beta1"
	federationclientset "k8s.io/kubernetes/federation/client/clientset_generated/federation_release_1_5"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util/deletionhelper"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util/eventsink"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/errors"
	api_v1 "k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/client/cache"
	kubeclientset "k8s.io/kubernetes/pkg/client/clientset_generated/release_1_5"
	"k8s.io/kubernetes/pkg/client/record"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/conversion"
	pkg_runtime "k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/util/flowcontrol"
	"k8s.io/kubernetes/pkg/watch"

	"github.com/golang/glog"
)

const (
	FedResourceQuotaPreferencesAnnotation = "federation.kubernetes.io/resource-quota-preferences"
	allClustersKey                        = "ALL_CLUSTERS"
	UserAgentName                         = "Federation-resourcequota-Controller"
)

func parseFederationResourceQuotaPreference(fedResourceQuota *api_v1.ResourceQuota) (*fed.FederatedResourceQuotaPreferences, error) {
	if fedResourceQuota.Annotations == nil {
		return nil, nil
	}
	fedResourceQuotaPrefString, found := fedResourceQuota.Annotations[FedResourceQuotaPreferencesAnnotation]
	if !found {
		return nil, nil
	}
	var fedResourceQuotaPref fed.FederatedResourceQuotaPreferences
	if err := json.Unmarshal([]byte(fedResourceQuotaPrefString), &fedResourceQuotaPref); err != nil {
		return nil, err
	}
	return &fedResourceQuotaPref, nil
}

type ResourceQuotaController struct {
	// For triggering single resourcequota reconciliation. This is used when there is an
	// add/update/delete operation on a resourcequota in either federated API server or
	// in some member of the federation.
	resourceQuotaDeliverer *util.DelayingDeliverer

	// For triggering all resourcequotas reconciliation. This is used when
	// a new cluster becomes available.
	clusterDeliverer *util.DelayingDeliverer

	// Contains resourcequotas present in members of federation.
	resourceQuotaFederatedInformer util.FederatedInformer

	// Contains namespaces present in members of federation.
	namespaceFederatedInformer util.FederatedInformer

	// For updating members of federation.
	federatedUpdater util.FederatedUpdater
	// Definitions of resourcequotas that should be federated.
	resourceQuotaInformerStore cache.Store
	// Informer controller for resourcequotas that should be federated.
	resourceQuotaInformerController cache.ControllerInterface

	// Client to federated api server.
	federatedApiClient federationclientset.Interface

	// clusterMonitorPeriod is the period for updating status of cluster
	resourceQuotaMonitorPeriod time.Duration
	// namespaceResourceQuotaStatusMap is a cluster map of mapping of namespaceName and resource quota status of last sampling
	resourceQuotaStatusMap map[string]api_v1.ResourceQuotaStatus

	// Backoff manager for resourcequotas
	resourceQuotaBackoff *flowcontrol.Backoff

	// For events
	eventRecorder record.EventRecorder

	deletionHelper *deletionhelper.DeletionHelper

	resourceQuotaReviewDelay time.Duration
	clusterAvailableDelay    time.Duration
	smallDelay               time.Duration
	updateTimeout            time.Duration
}

// NewResourceQuotaController returns a new resourcequota controller
func NewResourceQuotaController(client federationclientset.Interface) *ResourceQuotaController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(eventsink.NewFederatedEventSink(client))
	recorder := broadcaster.NewRecorder(api.EventSource{Component: "federated-resourcequotas-controller"})

	resourcequotacontroller := &ResourceQuotaController {
		federatedApiClient:       client,
		resourceQuotaReviewDelay: time.Second * 10,
		clusterAvailableDelay:    time.Second * 20,
		smallDelay:               time.Second * 3,
		updateTimeout:            time.Second * 30,
		resourceQuotaBackoff:     flowcontrol.NewBackOff(5*time.Second, time.Minute),
		resourceQuotaStatusMap:   make(map[string]api_v1.ResourceQuotaStatus),
		eventRecorder:            recorder,
	}

	// Build delivereres for triggering reconciliations.
	resourcequotacontroller.resourceQuotaDeliverer = util.NewDelayingDeliverer()
	resourcequotacontroller.clusterDeliverer = util.NewDelayingDeliverer()

	// Start informer in federated API servers on resourcequotas that should be federated.
	resourcequotacontroller.resourceQuotaInformerStore, resourcequotacontroller.resourceQuotaInformerController = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options api.ListOptions) (pkg_runtime.Object, error) {
				versionedOptions := util.VersionizeV1ListOptions(options)
				return client.Core().ResourceQuotas(api_v1.NamespaceAll).List(versionedOptions)
			},
			WatchFunc: func(options api.ListOptions) (watch.Interface, error) {
				versionedOptions := util.VersionizeV1ListOptions(options)
				return client.Core().ResourceQuotas(api_v1.NamespaceAll).Watch(versionedOptions)
			},
		},
		&api_v1.ResourceQuota{},
		controller.NoResyncPeriodFunc(),
		util.NewTriggerOnAllChanges(func(obj pkg_runtime.Object) { resourcequotacontroller.deliverResourceQuotaObj(obj, 0, false) }))

	// Federated informer on resourcequotas in members of federation.
	resourcequotacontroller.resourceQuotaFederatedInformer = util.NewFederatedInformer(
		client,
		func(cluster *federation_api.Cluster, targetClient kubeclientset.Interface) (cache.Store, cache.ControllerInterface) {
			return cache.NewInformer(
				&cache.ListWatch{
					ListFunc: func(options api.ListOptions) (pkg_runtime.Object, error) {
						versionedOptions := util.VersionizeV1ListOptions(options)
						return targetClient.Core().ResourceQuotas(api_v1.NamespaceAll).List(versionedOptions)
					},
					WatchFunc: func(options api.ListOptions) (watch.Interface, error) {
						versionedOptions := util.VersionizeV1ListOptions(options)
						return targetClient.Core().ResourceQuotas(api_v1.NamespaceAll).Watch(versionedOptions)
					},
				},
				&api_v1.ResourceQuota{},
				controller.NoResyncPeriodFunc(),
				// Trigger reconciliation whenever something in federated cluster is changed. In most cases it
				// would be just confirmation that some resourcequota opration succeeded.
				util.NewTriggerOnAllChanges(
					func(obj pkg_runtime.Object) {
						resourcequotacontroller.deliverResourceQuotaObj(obj, resourcequotacontroller.resourceQuotaReviewDelay, false)
					},
				))
		},

		&util.ClusterLifecycleHandlerFuncs{
			ClusterAvailable: func(cluster *federation_api.Cluster) {
				// When new cluster becomes available process all the resourcequotas again.
				resourcequotacontroller.clusterDeliverer.DeliverAt(allClustersKey, nil, time.Now().Add(resourcequotacontroller.clusterAvailableDelay))
			},
		},
	)

	// Federated updater along with Create/Update/Delete operations.
	resourcequotacontroller.federatedUpdater = util.NewFederatedUpdater(resourcequotacontroller.resourceQuotaFederatedInformer,
		func(client kubeclientset.Interface, obj pkg_runtime.Object) error {
			resourcequota := obj.(*api_v1.ResourceQuota)
			_, err := client.Core().ResourceQuotas(resourcequota.Namespace).Create(resourcequota)
			return err
		},
		func(client kubeclientset.Interface, obj pkg_runtime.Object) error {
			resourcequota := obj.(*api_v1.ResourceQuota)
			_, err := client.Core().ResourceQuotas(resourcequota.Namespace).Update(resourcequota)
			return err
		},
		func(client kubeclientset.Interface, obj pkg_runtime.Object) error {
			resourcequota := obj.(*api_v1.ResourceQuota)
			err := client.Core().ResourceQuotas(resourcequota.Namespace).Delete(resourcequota.Name, &api_v1.DeleteOptions{})
			return err
		})
	resourcequotacontroller.deletionHelper = deletionhelper.NewDeletionHelper(
		resourcequotacontroller.hasFinalizerFunc,
		resourcequotacontroller.removeFinalizerFunc,
		resourcequotacontroller.addFinalizerFunc,
		// objNameFunc
		func(obj pkg_runtime.Object) string {
			resourceQuota := obj.(*api_v1.ResourceQuota)
			return resourceQuota.Name
		},
		resourcequotacontroller.updateTimeout,
		resourcequotacontroller.eventRecorder,
		resourcequotacontroller.resourceQuotaFederatedInformer,
		resourcequotacontroller.federatedUpdater,
	)
	return resourcequotacontroller
}

// Returns true if the given object has the given finalizer in its ObjectMeta.
func (resourcequotacontroller *ResourceQuotaController) hasFinalizerFunc(obj pkg_runtime.Object, finalizer string) bool {
	resourceQuota := obj.(*api_v1.ResourceQuota)
	for i := range resourceQuota.ObjectMeta.Finalizers {
		if string(resourceQuota.ObjectMeta.Finalizers[i]) == finalizer {
			return true
		}
	}
	return false
}

// Removes the finalizer from the given objects ObjectMeta.
// Assumes that the given object is a namespace.
func (resourcequotacontroller *ResourceQuotaController) removeFinalizerFunc(obj pkg_runtime.Object, finalizer string) (pkg_runtime.Object, error) {
	resourceQuota := obj.(*api_v1.ResourceQuota)
	newFinalizers := []string{}
	hasFinalizer := false
	for i := range resourceQuota.ObjectMeta.Finalizers {
		if string(resourceQuota.ObjectMeta.Finalizers[i]) != finalizer {
			newFinalizers = append(newFinalizers, resourceQuota.ObjectMeta.Finalizers[i])
		} else {
			hasFinalizer = true
		}
	}
	if !hasFinalizer {
		// Nothing to do.
		return obj, nil
	}
	resourceQuota.ObjectMeta.Finalizers = newFinalizers
	resourceQuota, err := resourcequotacontroller.federatedApiClient.Core().ResourceQuotas(resourceQuota.Namespace).Update(resourceQuota)
	if err != nil {
		return nil, fmt.Errorf("failed to remove finalizer %s from resource quota %s: %v", finalizer, resourceQuota.Name, err)
	}
	return resourceQuota, nil
}

// Adds the given finalizer to the given objects ObjectMeta.
// Assumes that the given object is a namespace.
func (resourcequotacontroller *ResourceQuotaController) addFinalizerFunc(obj pkg_runtime.Object, finalizer string) (pkg_runtime.Object, error) {
	resourceQuota := obj.(*api_v1.ResourceQuota)
	resourceQuota.ObjectMeta.Finalizers = append(resourceQuota.ObjectMeta.Finalizers, finalizer)
	resourceQuota, err := resourcequotacontroller.federatedApiClient.Core().ResourceQuotas(resourceQuota.Namespace).Update(resourceQuota)
	if err != nil {
		return nil, fmt.Errorf("failed to add finalizer %s to resource quota %s: %v", finalizer, resourceQuota.Name, err)
	}
	return resourceQuota, nil
}

func (resourcequotacontroller *ResourceQuotaController) Run(stopChan <-chan struct{}) {

	go resourcequotacontroller.resourceQuotaInformerController.Run(stopChan)
	resourcequotacontroller.resourceQuotaFederatedInformer.Start()
	go func() {
		<-stopChan
		resourcequotacontroller.resourceQuotaFederatedInformer.Stop()
	}()
	resourcequotacontroller.resourceQuotaDeliverer.StartWithHandler(func(item *util.DelayingDelivererItem) {
		resourcequota := item.Value.(*resourcequotaItem)
		resourcequotacontroller.reconcileResourceQuota(resourcequota.namespace, resourcequota.name)
	})
	resourcequotacontroller.clusterDeliverer.StartWithHandler(func(_ *util.DelayingDelivererItem) {
		resourcequotacontroller.reconcileResourceQuotasOnClusterChange()
	})
	util.StartBackoffGC(resourcequotacontroller.resourceQuotaBackoff, stopChan)
}

func getResourceQuotaKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

// Internal structure for data in delaying deliverer.
type resourcequotaItem struct {
	namespace string
	name      string
}

func (resourcequotacontroller *ResourceQuotaController) deliverResourceQuotaObj(obj interface{}, delay time.Duration, failed bool) {
	resourceQuota := obj.(*api_v1.ResourceQuota)
	resourcequotacontroller.deliverResourceQuota(resourceQuota.Namespace, resourceQuota.Name, delay, failed)
}

// Adds backoff to delay if this delivery is related to some failure. Resets backoff if there was no failure.
func (resourcequotacontroller *ResourceQuotaController) deliverResourceQuota(namespace string, name string, delay time.Duration, failed bool) {
	key := getResourceQuotaKey(namespace, name)
	if failed {
		resourcequotacontroller.resourceQuotaBackoff.Next(key, time.Now())
		delay = delay + resourcequotacontroller.resourceQuotaBackoff.Get(key)
	} else {
		resourcequotacontroller.resourceQuotaBackoff.Reset(key)
	}
	resourcequotacontroller.resourceQuotaDeliverer.DeliverAfter(key,
		&resourcequotaItem{namespace: namespace, name: name}, delay)
}

// Check whether all data stores are in sync. False is returned if any of the informer/stores is not yet
// synced with the corresponding api server.
func (resourcequotacontroller *ResourceQuotaController) isSynced() bool {
	if !resourcequotacontroller.resourceQuotaFederatedInformer.ClustersSynced() {
		glog.V(2).Infof("Cluster list not synced")
		return false
	}
	clusters, err := resourcequotacontroller.resourceQuotaFederatedInformer.GetReadyClusters()
	if err != nil {
		glog.Errorf("Failed to get ready clusters: %v", err)
		return false
	}
	if !resourcequotacontroller.resourceQuotaFederatedInformer.GetTargetStore().ClustersSynced(clusters) {
		return false
	}
	return true
}

// The function triggers reconciliation of all federated resourcequotas.
func (resourcequotacontroller *ResourceQuotaController) reconcileResourceQuotasOnClusterChange() {
	if !resourcequotacontroller.isSynced() {
		resourcequotacontroller.clusterDeliverer.DeliverAt(allClustersKey, nil, time.Now().Add(resourcequotacontroller.clusterAvailableDelay))
	}
	for _, obj := range resourcequotacontroller.resourceQuotaInformerStore.List() {
		resourceQuota := obj.(*api_v1.ResourceQuota)
		resourcequotacontroller.deliverResourceQuota(resourceQuota.Namespace, resourceQuota.Name, resourcequotacontroller.smallDelay, false)
	}
}

func (resourcequotacontroller *ResourceQuotaController) reconcileResourceQuota(namespace string, resourcequotaName string) {

	if !resourcequotacontroller.isSynced() {
		resourcequotacontroller.deliverResourceQuota(namespace, resourcequotaName, resourcequotacontroller.clusterAvailableDelay, false)
		return
	}

	key := getResourceQuotaKey(namespace, resourcequotaName)
	baseResourceQuotaObjFromStore, exist, err := resourcequotacontroller.resourceQuotaInformerStore.GetByKey(key)
	if err != nil {
		glog.Errorf("Failed to query main resource quota store for %v: %v", key, err)
		resourcequotacontroller.deliverResourceQuota(namespace, resourcequotaName, 0, true)
		return
	}

	if !exist {
		// Not federated resource quota, ignoring.
		return
	}
	// Create a copy before modifying the resource quota to prevent race condition with
	// other readers of resource quota from store.
	baseResourceQuotaObj, err := conversion.NewCloner().DeepCopy(baseResourceQuotaObjFromStore)
	baseResourceQuota, ok := baseResourceQuotaObj.(*api_v1.ResourceQuota)
	if err != nil || !ok {
		glog.Errorf("Error in retrieving obj from store: %v, %v", ok, err)
		resourcequotacontroller.deliverResourceQuota(namespace, baseResourceQuota.Name, resourcequotacontroller.smallDelay, true)
		return
	}
	if baseResourceQuota.DeletionTimestamp != nil {
		if err := resourcequotacontroller.delete(baseResourceQuota); err != nil {
			glog.Errorf("Failed to delete %s: %v", resourcequotaName, err)
			resourcequotacontroller.eventRecorder.Eventf(baseResourceQuota, api.EventTypeNormal, "DeleteFailed",
				"Resource quota delete failed: %v", err)
			resourcequotacontroller.deliverResourceQuota(namespace, resourcequotaName, resourcequotacontroller.smallDelay, true)
		}
		return
	}

	glog.V(3).Infof("Ensuring delete object from underlying clusters finalizer for namespace: %s",
		baseResourceQuota.Name)
	// Add the required finalizers before creating a resource quota in
	// underlying clusters.
	// This ensures that the dependent resource quotas are deleted in underlying
	// clusters when the federated resource quota is deleted.
	updatedResourceQuotaObj, err := resourcequotacontroller.deletionHelper.EnsureFinalizers(baseResourceQuota)
	if err != nil {
		glog.Errorf("Failed to ensure delete object from underlying clusters finalizer in namespace %s: %v",
			baseResourceQuota.Name, err)
		resourcequotacontroller.deliverResourceQuota(namespace, resourcequotaName, resourcequotacontroller.smallDelay, true)
		return
	}
	updatedResourceQuota := updatedResourceQuotaObj.(*api_v1.ResourceQuota)

	glog.V(3).Infof("Syncing resource quota %s in underlying clusters", updatedResourceQuota.Name)

	clusters, err := resourcequotacontroller.resourceQuotaFederatedInformer.GetReadyClusters()
	if err != nil {
		glog.Errorf("Failed to get cluster list: %v", err)
		resourcequotacontroller.deliverResourceQuota(namespace, resourcequotaName, resourcequotacontroller.clusterAvailableDelay, true)
		return
	}

	operations := make([]util.FederatedOperation, 0)
	for _, cluster := range clusters {

		clusterResourceQuotaObj, found, err := resourcequotacontroller.resourceQuotaFederatedInformer.GetTargetStore().GetByKey(cluster.Name, key)
		if err != nil {
			glog.Errorf("Failed to get %s from %s: %v", key, cluster.Name, err)
			resourcequotacontroller.deliverResourceQuota(namespace, resourcequotaName, 0, true)
			return
		}

		desiredResourceQuota := &api_v1.ResourceQuota{
			ObjectMeta: util.DeepCopyRelevantObjectMeta(updatedResourceQuota.ObjectMeta),
			Spec:       updatedResourceQuota.Spec,
			Status:     updatedResourceQuota.Status,
		}

		if !found {
			resourcequotacontroller.eventRecorder.Eventf(updatedResourceQuota, api.EventTypeNormal, "CreateInCluster",
				"Creating resourcequota in cluster %s", cluster.Name)

			operations = append(operations, util.FederatedOperation{
				Type:        util.OperationTypeAdd,
				Obj:         desiredResourceQuota,
				ClusterName: cluster.Name,
			})
		} else {
			clusterResourceQuota := clusterResourceQuotaObj.(*api_v1.ResourceQuota)

			// Update existing resourcequota, if needed.
			// TODO: Current logic only support pass through, need split CPU/Memory values in future
			if !util.ObjectMetaEquivalent(desiredResourceQuota.ObjectMeta, clusterResourceQuota.ObjectMeta) ||
				!reflect.DeepEqual(desiredResourceQuota.Spec, clusterResourceQuota.Spec) ||
				!reflect.DeepEqual(desiredResourceQuota.Status.Hard, clusterResourceQuota.Status.Hard) {

				resourcequotacontroller.eventRecorder.Eventf(updatedResourceQuota, api.EventTypeNormal, "UpdateInCluster",
					"Updating resourcequota in cluster %s", cluster.Name)
				operations = append(operations, util.FederatedOperation{
					Type:        util.OperationTypeUpdate,
					Obj:         desiredResourceQuota,
					ClusterName: cluster.Name,
				})
			}
			// TODO: Summarizing Status values for the fields being split
			resourcequotacontroller.resourceQuotaStatusMap[key] = clusterResourceQuota.Status
		}
	}

	baseResourceQuota.Status = resourcequotacontroller.resourceQuotaStatusMap[key]
	baseResourceQuota, err = resourcequotacontroller.federatedApiClient.Core().ResourceQuotas(namespace).Update(baseResourceQuota)
	if err != nil {
		glog.Errorf("failed to update resource quota status %s: %v", resourcequotaName, err)
		return
	}

	if len(operations) == 0 {
		// Everything is in order
		return
	}
	err = resourcequotacontroller.federatedUpdater.UpdateWithOnError(operations, resourcequotacontroller.updateTimeout,
		func(op util.FederatedOperation, operror error) {
			resourcequotacontroller.eventRecorder.Eventf(updatedResourceQuota, api.EventTypeNormal, "UpdateInClusterFailed",
				"ResourceQuota update in cluster %s failed: %v", op.ClusterName, operror)
		})

	if err != nil {
		glog.Errorf("Failed to execute updates for %s: %v", key, err)
		resourcequotacontroller.deliverResourceQuota(namespace, resourcequotaName, 0, true)
		return
	}

	// Evertyhing is in order but lets be double sure
	resourcequotacontroller.deliverResourceQuota(namespace, resourcequotaName, resourcequotacontroller.resourceQuotaReviewDelay, false)
}

// delete  deletes the given resource quota or returns error if the deletion was not complete.
func (resourcequotacontroller *ResourceQuotaController) delete(resourceQuota *api_v1.ResourceQuota) error {
	var err error
	glog.V(3).Infof("Deleting resource quota %s ...", resourceQuota.Name)
	// Delete the resource quota from all underlying clusters.
	_, err = resourcequotacontroller.deletionHelper.HandleObjectInUnderlyingClusters(resourceQuota)
	if err != nil {
		return err
	}

	/*resourcequotacontroller.eventRecorder.Event(resourceQuota, api.EventTypeNormal, "DeleteResourceQuota", fmt.Sprintf("Marking for deletion"))
	err = resourcequotacontroller.federatedApiClient.Core().ResourceQuotas(resourceQuota.Namespace).Delete(resourceQuota.Name, &api_v1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete resource quota: %v", err)
	}*/

	err = resourcequotacontroller.federatedApiClient.Core().ResourceQuotas(resourceQuota.Namespace).Delete(resourceQuota.Name, nil)
	if err != nil {
		// Its all good if the error is not found error. That means it is deleted already and we do not have to do anything.
		// This is expected when we are processing an update as a result of resource quota finalizer deletion.
		// The process that deleted the last finalizer is also going to delete the resource quota and we do not have to do anything.
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete resource quota: %v", err)
		}
	}
	return nil
}


