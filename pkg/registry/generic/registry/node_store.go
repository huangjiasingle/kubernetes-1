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

package registry

import (
	"reflect"

	"golang.org/x/net/context"
	"k8s.io/kubernetes/pkg/conversion"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/watch"

	//kubeclientset "k8s.io/kubernetes/pkg/client/clientset_generated/release_1_5"
	etcd "github.com/coreos/etcd/client"
	api_v1 "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/meta"
	kubeclientset "k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"
	"k8s.io/kubernetes/pkg/client/restclient"
	"k8s.io/kubernetes/pkg/client/unversioned/clientcmd"
	"strings"
)

const (
	ClusterNameAnnotation = "federation.kubernetes.io/cluster-name"
)

type nodeStore struct {
	store       storage.Interface
	etcdKeysAPI etcd.KeysAPI
}

func NewNodeStore(client etcd.Client, c storage.Interface) *nodeStore {
	return &nodeStore{
		store:       c,
		etcdKeysAPI: etcd.NewKeysAPI(client),
	}
}

// Versioner implements storage.Interface.Versioner.
func (s *nodeStore) Versioner() storage.Versioner {
	return s.store.Versioner()
}

// Get implements storage.Interface.Get.
func (s *nodeStore) Get(ctx context.Context, key string, out runtime.Object, ignoreNotFound bool) error {
	return s.store.Get(ctx, key, out, ignoreNotFound)
}

// Create implements storage.Interface.Create.
func (s *nodeStore) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	return s.store.Create(ctx, key, obj, out, ttl)
}

// Delete implements storage.Interface.Delete.
func (s *nodeStore) Delete(ctx context.Context, key string, out runtime.Object, precondtions *storage.Preconditions) error {
	return s.store.Delete(ctx, key, out, precondtions)
}

// GuaranteedUpdate implements storage.Interface.GuaranteedUpdate.
func (s *nodeStore) GuaranteedUpdate(ctx context.Context, key string, out runtime.Object, ignoreNotFound bool, precondtions *storage.Preconditions, tryUpdate storage.UpdateFunc, suggestion ...runtime.Object) error {
	return s.store.GuaranteedUpdate(ctx, key, out, ignoreNotFound, precondtions, tryUpdate, suggestion...)
}

// GetToList implements storage.Interface.GetToList.
func (s *nodeStore) GetToList(ctx context.Context, key string, resourceVersion string, pred storage.SelectionPredicate, listObj runtime.Object) error {
	return s.store.GetToList(ctx, key, resourceVersion, pred, listObj)
}

// List implements storage.Interface.List.
func (s *nodeStore) List(ctx context.Context, key, resourceVersion string, pred storage.SelectionPredicate, listObj runtime.Object) error {
	if resourceVersion == "0" {
		// return cache only
		return s.store.List(ctx, key, "", pred, listObj)

	} else {
		listPtr, err := meta.GetItemsPtr(listObj)
		if err != nil {
			return err
		}
		vPtr, err := conversion.EnforcePtr(listPtr)
		if err != nil || vPtr.Kind() != reflect.Slice {
			// This should not happen at runtime.
			panic("need ptr to slice")
		}
		// get clusters from etcd
		ClusterPrefix := "/registry/clusters"
		clusters, _, _ := listEtcdNode(s.etcdKeysAPI, ctx, ClusterPrefix)

		//TODO: change this to parallel
		for _, cluster := range clusters {
			if cluster.Value != "" {
				v := cluster.Value
				ip := strings.Split(strings.Split(v, "\"serverAddress\":\"")[1], "\"}]}")[0]
				// get the list of nodes from the clusters
				clusterConfig, err := clientcmd.BuildConfigFromFlags(ip, "")
				if err == nil && clusterConfig != nil {
					clientset := kubeclientset.NewForConfigOrDie(restclient.AddUserAgent(clusterConfig, userAgentName))
					tmpNodeList, _ := clientset.Core().Nodes().List(api_v1.ListOptions{})
					for i := range tmpNodeList.Items {
						p := tmpNodeList.Items[i]
						if p.Annotations == nil {
							p.Annotations = make(map[string]string)
						}
						p.Annotations[ClusterNameAnnotation] = strings.Split(cluster.Key, ClusterPrefix)[1]
						vPtr.Set(reflect.Append(vPtr, reflect.ValueOf(p)))
					}
				}
			}
		}
		return nil
	}
}

// Watch implements storage.Interface.Watch.
func (s *nodeStore) Watch(ctx context.Context, key string, resourceVersion string, pred storage.SelectionPredicate) (watch.Interface, error) {
	watchInterface, err := s.store.Watch(ctx, key, resourceVersion, pred)
	return watchInterface, err
}

// WatchList implements storage.Interface.WatchList.
func (s *nodeStore) WatchList(ctx context.Context, key string, resourceVersion string, pred storage.SelectionPredicate) (watch.Interface, error) {
	return s.store.WatchList(ctx, key, resourceVersion, pred)
}