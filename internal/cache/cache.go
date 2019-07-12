package cache

/*
 *import (
 *  demandapi "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha1"
 *  "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
 *  demandclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/scaler/v1alpha1"
 *  sparkschedulerclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/sparkscheduler/v1beta1"
 *  "k8s.io/apimachinery/pkg/api/errors"
 *  metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
 *  "k8s.io/apimachinery/pkg/types"
 *  clientcache "k8s.io/client-go/tools/cache"
 *)
 *
 *type cache struct {
 *  client *asyncClient
 *  store  *objectStore
 *}
 *
 *func NewCache(
 *  client *asyncClient,
 *  informer clientcache.SharedIndexInformer) *cache {
 *  c := &cache{
 *    client: client,
 *    store:  NewStore(),
 *  }
 *  informer.AddEventHandler(
 *    clientcache.ResourceEventHandlerFuncs{
 *      AddFunc:    c.onObjAdd,
 *      UpdateFunc: c.onObjUpdate,
 *      DeleteFunc: c.onObjDelete,
 *    },
 *  )
 *  return c
 *}
 *
 *func (c *cache) onObjAdd(obj interface{}) {
 *  typedObject, ok := obj.(metav1.Object)
 *  if !ok {
 *    // TODO log
 *    return
 *  }
 *  c.store.PutIfAbsent(typedObject)
 *}
 *
 *func (c *cache) onObjUpdate(oldObj interface{}, newObj interface{}) {
 *
 *}
 *
 *func (c *cache) onObjDelete(obj interface{}) {
 *  typedObject, ok := obj.(metav1.Object)
 *  if !ok {
 *    // TODO log
 *    return
 *  }
 *  c.store.Delete(typedObject.GetUID())
 *}
 *
 *func (c *cache) Create(obj metav1.Object) error {
 *  // TODO: return err already exists
 *  c.client.Create(obj)
 *  c.store.PutIfAbsent(obj)
 *}
 *
 *func (c *cache) Get(namespace, name string) (metav1.Object, error) {
 *  //obj, err := c.getFunc(namespace, name)
 *  //if err == nil {
 *  //return obj, nil
 *  //}
 *  //if !errors.IsNotFound(err) {
 *  //return nil, err
 *  //}
 *
 *  // call getFunc get internal cache, or return
 *}
 *
 *func (c *cache) Update(obj metav1.Object) error {
 *}
 *
 *func (c *cache) Delete(obj metav1.Object) error {
 *  c.client.Delete(obj)
 *  c.deletedObjectStore.Put(obj)
 *}
 *
 *func (c *cache) List() []metav1.Object {
 *  return c.store.List()
 *}
 *
 *type ResourceReservationCache struct {
 *  cache                       cache
 *  resourceReservationClient   sparkschedulerclient.SparkschedulerV1beta1Interface
 *  resourceReservationInformer clientcache.SharedIndexInformer
 *}
 *
 *func (rrc *ResourceReservationCache) Create(rr *v1beta1.ResourceReservation) {
 *  rrc.cache.Create(rr)
 *}
 *
 *func (rrc *ResourceReservationCache) Get(namespace, name string) *v1beta1.ResourceReservation {
 *  return rrc.cache.Get(namespace, name).(*v1beta1.ResourceReservation)
 *}
 *
 *func (rrc *ResourceReservationCache) List() []*v1beta1.ResourceReservation {
 *  objects := rrc.cache.List()
 *  res := make([]*v1beta1.ResourceReservation, 0, len(objects))
 *  for _, o := range objects {
 *    res = append(res, o.(*v1beta1.ResourceReservation))
 *  }
 *  return res
 *}
 *
 *type DemandCache struct {
 *  cache          cache
 *  demandClient   demandclient.ScalerV1alpha1Interface
 *  demandInformer clientcache.SharedIndexInformer
 *}
 *
 *func (dc *DemandCache) Create(d *demandapi.Demand) {
 *  dc.cache.Create(d)
 *}
 *
 *func (dc *DemandCache) Get(namespace, name string) *demandapi.Demand {
 *  return dc.cache.Get(namespace, name).(*demandapi.Demand)
 *}
 */
