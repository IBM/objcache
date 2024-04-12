/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package controllers

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"gopkg.in/yaml.v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	appsv1apply "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1apply "k8s.io/client-go/applyconfigurations/core/v1"
	metav1apply "k8s.io/client-go/applyconfigurations/meta/v1"
	storagev1apply "k8s.io/client-go/applyconfigurations/storage/v1"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	trlv1alpha1 "github.com/IBM/objcache/operator/api/v1alpha1"
)

// ObjcacheClusterReconciler reconciles a ObjcacheCluster object
type ObjcacheClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=trl.ibm.com,resources=objcacheclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=trl.ibm.com,resources=objcacheclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=trl.ibm.com,resources=objcacheclusters/finalizers,verbs=update

//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete;deletecollection
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete;deletecollection
//+kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch;delete;create;update;patch
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;delete;create;update;patch
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete;deletecollection

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ObjcacheCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *ObjcacheClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	l := logger.WithValues("Objcache", req.NamespacedName)
	objcache := &trlv1alpha1.ObjcacheCluster{}
	err := r.Get(ctx, req.NamespacedName, objcache)
	var requeue = false
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		l.Error(err, "Failed: Reconcile, Get", "namespace", req.Namespace, "name", req.Name)
		return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, err
	}

	// Add finalizer to instance
	if !controllerutil.ContainsFinalizer(objcache, objcacheFinalizer) {
		controllerutil.AddFinalizer(objcache, objcacheFinalizer)
		err = r.Update(ctx, objcache)
		if err != nil {
			return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, err
		}
	}
	if objcache.GetDeletionTimestamp() != nil {
		if controllerutil.ContainsFinalizer(objcache, objcacheFinalizer) {
			requeue, err = r.DeleteCluster(ctx, objcache, l)
		}
	} else {
		requeue, err = r.UpdateCluster(ctx, objcache, l)
	}
	if requeue || err != nil {
		return ctrl.Result{Requeue: requeue, RequeueAfter: 100 * time.Millisecond}, err
	}
	return ctrl.Result{Requeue: requeue}, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *ObjcacheClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&trlv1alpha1.ObjcacheCluster{}).
		Owns(&corev1.Service{}).Owns(&appsv1.StatefulSet{}).Owns(&storagev1.StorageClass{}).
		WithEventFilter(predicate.Funcs{
			//CreateFunc: func(e event.CreateEvent) bool { return ObjcacheEventFilter(e.Object) },
			UpdateFunc: func(e event.UpdateEvent) bool {
				return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration()
			},
			DeleteFunc: func(e event.DeleteEvent) bool { return !e.DeleteStateUnknown },
			//GenericFunc: func(e event.GenericEvent) bool { return ObjcacheEventFilter(e.Object) },
		}).
		Complete(r)
}

func (r *ObjcacheClusterReconciler) DeleteCluster(ctx context.Context, objcache *trlv1alpha1.ObjcacheCluster, l logr.Logger) (requeue bool, err error) {
	if requeue, err = r.DeleteStorageClass(ctx, objcache, l); err != nil || requeue {
		return
	}
	for i := objcache.Status.StartedShards - 1; i >= 0; i-- {
		if err = r.DeleteStatefulSet(ctx, objcache, l, i); err != nil {
			return
		}
		for j := objcache.Status.RaftFollowers; j >= 0; j-- {
			if err = r.DeleteStatefulSetVolume(ctx, objcache, l, i, j); err != nil {
				return
			}
		}
		objcache.Status.StartedShards = i
	}
	if requeue, err = r.DeleteService(ctx, objcache, l); err != nil || requeue {
		return
	}
	if err = r.DeleteConfigMap(ctx, objcache, l); err != nil {
		return
	}

	if updated := controllerutil.RemoveFinalizer(objcache, objcacheFinalizer); updated {
		err = r.Update(ctx, objcache)
		if err != nil {
			l.Error(err, "Failed: DeleteCluster, Update")
		}
	}
	return
}

func (r *ObjcacheClusterReconciler) UpdateCluster(ctx context.Context, objcache *trlv1alpha1.ObjcacheCluster, l logr.Logger) (requeue bool, err error) {
	requeue, err = r.UpdateConfigMap(ctx, objcache, l)
	if err != nil || requeue {
		return
	}
	if requeue, err = r.UpdateService(ctx, objcache, l); err != nil || requeue {
		return
	}
	var update = false
	if objcache.Status.Seed == 0 {
		if objcache.Spec.Seed > 0 {
			objcache.Status.Seed = objcache.Spec.Seed
		} else {
			objcache.Status.Seed = int64(time.Now().Unix())
			if objcache.Status.Seed < 0 {
				objcache.Status.Seed *= -1
			}
		}
		update = true
	}
	currentStartedReplicas := objcache.Status.StartedShards
	if objcache.Spec.RaftFollowers != objcache.Status.RaftFollowers {
		// update existing statefulsets with new raft followers
		for i := 0; i < currentStartedReplicas; i++ {
			if err = r.UpdateStatefulSet(ctx, objcache, l, i); err != nil {
				goto tryUpdate
			}
		}
		objcache.Status.RaftFollowers = objcache.Spec.RaftFollowers
		update = true
	}
	for i := currentStartedReplicas; i < *objcache.Spec.Shards; i++ { // scaling up
		if err = r.UpdateStatefulSet(ctx, objcache, l, i); err != nil {
			goto tryUpdate
		}
		objcache.Status.StartedShards = i + 1
		objcache.Status.RaftFollowers = objcache.Spec.RaftFollowers
		update = true
	}
	for i := currentStartedReplicas - 1; i >= 0 && i > *objcache.Spec.Shards-1; i-- { // scaling down
		if err = r.DeleteStatefulSet(ctx, objcache, l, i); err != nil {
			goto tryUpdate
		}
		for j := objcache.Status.RaftFollowers; j >= 0; j-- {
			if err = r.DeleteStatefulSetVolume(ctx, objcache, l, i, j); err != nil {
				return
			}
		}
		objcache.Status.StartedShards = i
		update = true
	}
	if requeue, err = r.UpdateStorageClass(ctx, objcache, l); err != nil || requeue {
		return
	}
tryUpdate:
	if update {
		if err2 := r.Status().Update(ctx, objcache); err2 != nil {
			return
		}
	}
	return false, err
}

func (r *ObjcacheClusterReconciler) UpdateConfigMap(ctx context.Context, objcache *trlv1alpha1.ObjcacheCluster, l logr.Logger) (bool, error) {
	yamlData, err := yaml.Marshal(objcache.Spec.ServerConfig)
	if err != nil {
		l.Error(err, "Failed: Marshal")
		return false, err
	}
	configMap := corev1apply.ConfigMap(objcache.Name, objcache.Namespace).
		WithLabels(map[string]string{"app": "objcache"}).WithAnnotations(GetAnnotationMap()).
		WithData(map[string]string{"config.yaml": string(yamlData)})
	gvk, err := apiutil.GVKForObject(objcache, r.Scheme)
	if err != nil {
		l.Error(err, "Failed: UpdateConfigMap, GVKForObject")
		return false, err
	}
	configMap.WithOwnerReferences(metav1apply.OwnerReference().
		WithAPIVersion(gvk.GroupVersion().String()).
		WithKind(gvk.Kind).
		WithName(objcache.Name).
		WithUID(objcache.GetUID()).
		WithBlockOwnerDeletion(true).
		WithController(true))
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(configMap)
	if err != nil {
		return false, err
	}
	patch := &unstructured.Unstructured{Object: obj}

	orig := &corev1.ConfigMap{}
	err = r.Get(ctx, client.ObjectKey{Name: *configMap.Name, Namespace: *configMap.Namespace}, orig)
	if err != nil && !errors.IsNotFound(err) {
		l.Error(err, "Failed: UpdateConfigMap, Get")
		return false, err
	}
	if err == nil {
		origApplyConfig, err := corev1apply.ExtractConfigMap(orig, fieldManager)
		if err != nil {
			l.Error(err, "Failed: UpdateConfigMap, ExtractConfigMap")
			return false, err
		}
		if equality.Semantic.DeepEqual(configMap, origApplyConfig) {
			return false, nil
		}
		diff := cmp.Diff(*origApplyConfig, *configMap)
		if len(diff) > 0 {
			l.Info("UpdateConfigMap, Patch", "diff", diff)
		}
	}
	if err = r.Patch(ctx, patch, client.Apply, &client.PatchOptions{FieldManager: fieldManager, Force: pointer.Bool(true)}); err != nil {
		l.Error(err, "Failed: UpdateConfigMap, Patch", "namespace", objcache.Namespace, "name", objcache.Name)
		return false, err
	}
	l.Info("Success: UpdateConfigMap, Patch", "namespace", objcache.Namespace, "name", objcache.Name)
	return false, nil
}

func (r *ObjcacheClusterReconciler) DeleteConfigMap(ctx context.Context, objcache *trlv1alpha1.ObjcacheCluster, l logr.Logger) error {
	found := &corev1.ConfigMap{}
	err := r.Get(ctx, client.ObjectKey{Name: objcache.Name, Namespace: objcache.Namespace}, found)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		l.Error(err, "Failed: DeleteConfigMap, Get")
		return err
	}
	if err = r.Delete(ctx, found); err != nil {
		l.Error(err, "Failed: DeleteConfigMap, Delete", "namespace", found.Namespace, "name", found.Name)
		return err
	}
	l.Info("Success: DeleteConfigMap, Delete", "namespace", found.Namespace, "name", found.Name)
	return nil
}

func (r *ObjcacheClusterReconciler) DeleteStorageClass(ctx context.Context, objcache *trlv1alpha1.ObjcacheCluster, l logr.Logger) (bool, error) {
	found := &storagev1.StorageClass{}
	key := client.ObjectKey{Name: objcache.Spec.GeneratedStorageClassName}
	err := r.Get(ctx, key, found)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		l.Error(err, "Failed: DeleteStorageClass, Get")
		return false, err
	}
	if err = r.Delete(ctx, found); err != nil {
		l.Error(err, "Failed: DeleteStorageClass, Delete", "name", found.Name)
		return false, err
	}
	err = r.Get(ctx, key, found)
	if err == nil {
		return true, nil
	}
	l.Info("Success: DeleteStorageClass, Delete", "name", found.Name)
	return false, nil
}

func GetStorageClassParameters(objcache *trlv1alpha1.ObjcacheCluster) map[string]string {
	ret := map[string]string{
		"objcache.io/headWorkerIp":   fmt.Sprintf("%v-0-0.%v-0.%v.svc.cluster.local", objcache.Name, objcache.Name, objcache.Namespace),
		"objcache.io/headWorkerPort": fmt.Sprintf("%d", objcache.Spec.ApiPort),
		"objcache.io/unstageDelay":   objcache.Spec.UnstageDelayPeriod,
	}
	if objcache.Spec.GoDebugEnvVar != nil {
		ret["objcache.io/godebug"] = *objcache.Spec.GoDebugEnvVar
	}
	if objcache.Spec.GoMemLimit != nil {
		ret["objcache.io/gomemlimit"] = *objcache.Spec.GoMemLimit
	}
	return ret
}

func (r *ObjcacheClusterReconciler) UpdateStorageClass(ctx context.Context, objcache *trlv1alpha1.ObjcacheCluster, l logr.Logger) (bool, error) {
	storageClass := storagev1apply.StorageClass(objcache.Spec.GeneratedStorageClassName).
		WithLabels(map[string]string{"app": "objcache"}).WithAnnotations(GetAnnotationMap()).
		WithProvisioner(driverName).WithParameters(GetStorageClassParameters(objcache))
	gvk, err := apiutil.GVKForObject(objcache, r.Scheme)
	if err != nil {
		l.Error(err, "Failed: UpdateStorageClass, GVKForObject")
		return false, err
	}
	storageClass.WithOwnerReferences(metav1apply.OwnerReference().
		WithAPIVersion(gvk.GroupVersion().String()).
		WithKind(gvk.Kind).
		WithName(objcache.Name).
		WithUID(objcache.GetUID()).
		WithBlockOwnerDeletion(true).
		WithController(true))

	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(storageClass)
	if err != nil {
		return false, err
	}
	patch := &unstructured.Unstructured{Object: obj}

	orig := &storagev1.StorageClass{}
	err = r.Get(ctx, client.ObjectKey{Name: *storageClass.Name}, orig)
	if err != nil && !errors.IsNotFound(err) {
		l.Error(err, "Failed: UpdateStorageClass, Get")
		return false, err
	}
	if err == nil {
		origApplyConfig, err := storagev1apply.ExtractStorageClass(orig, fieldManager)
		if err != nil {
			l.Error(err, "Failed: UpdateStorageClass, ExtractStorageClass")
			return false, err
		}
		return !equality.Semantic.DeepEqual(storageClass, origApplyConfig), nil
	}
	if err = r.Patch(ctx, patch, client.Apply, &client.PatchOptions{FieldManager: fieldManager, Force: pointer.Bool(true)}); err != nil {
		l.Error(err, "Failed: UpdateStorageClass, Patch", "namespace", objcache.Namespace, "name", storageClass.Name)
		return false, err
	}
	l.Info("Success: UpdateStorageClass, Patch", "name", storageClass.Name)
	return false, nil
}

func GetWorkerLabels(objcache *trlv1alpha1.ObjcacheCluster, groupId string) map[string]string {
	return map[string]string{
		"app": "objcache", "cluster": objcache.Name, "component": "cluster", "raft-groupId": groupId,
	}
}

func (r *ObjcacheClusterReconciler) DeleteService(ctx context.Context, objcache *trlv1alpha1.ObjcacheCluster, l logr.Logger) (bool, error) {
	groupName := fmt.Sprintf("%s-0", objcache.Name)
	found := &corev1.Service{}
	key := client.ObjectKey{Name: groupName, Namespace: objcache.Namespace}
	err := r.Get(ctx, key, found)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		l.Error(err, "Failed: DeleteService, Get")
		return false, err
	}
	if err = r.Delete(ctx, found); err != nil {
		l.Error(err, "Failed: DeleteService, Delete", "name", found.Name)
		return false, err
	}
	err = r.Get(ctx, key, found)
	if err == nil {
		return true, nil
	}
	l.Info("Success: DeleteService, Delete", "name", found.Name)
	return false, nil
}

func (r *ObjcacheClusterReconciler) UpdateService(ctx context.Context, objcache *trlv1alpha1.ObjcacheCluster, l logr.Logger) (requeue bool, err error) {
	groupName := fmt.Sprintf("%s-0", objcache.Name)
	service := corev1apply.Service(groupName, objcache.Namespace)
	spec := &corev1apply.ServiceSpecApplyConfiguration{}
	ports := &corev1apply.ServicePortApplyConfiguration{}
	labels := GetWorkerLabels(objcache, groupName)

	service.WithLabels(labels).
		WithSpec(spec.WithClusterIP("None").WithPorts(ports.WithName("raft").WithPort(int32(objcache.Spec.Port))).WithSelector(labels))

	gvk, err := apiutil.GVKForObject(objcache, r.Scheme)
	if err != nil {
		l.Error(err, "Failed: UpdateService, GVKForObject")
		return false, err
	}
	service.WithOwnerReferences(metav1apply.OwnerReference().
		WithAPIVersion(gvk.GroupVersion().String()).
		WithKind(gvk.Kind).
		WithName(objcache.Name).
		WithUID(objcache.GetUID()).
		WithBlockOwnerDeletion(true).
		WithController(true))

	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(service)
	if err != nil {
		return false, err
	}
	patch := &unstructured.Unstructured{Object: obj}

	orig := &corev1.Service{}
	err = r.Get(ctx, client.ObjectKey{Name: *service.Name, Namespace: *service.Namespace}, orig)
	if err != nil && !errors.IsNotFound(err) {
		l.Error(err, "Failed: UpdateService, Get")
		return false, err
	}
	if err == nil {
		origApplyConfig, err := corev1apply.ExtractService(orig, fieldManager)
		if err != nil {
			l.Error(err, "Failed: UpdateService, ExtractService")
			return false, err
		}
		return !equality.Semantic.DeepEqual(service, origApplyConfig), nil
	}
	if err = r.Patch(ctx, patch, client.Apply, &client.PatchOptions{FieldManager: fieldManager, Force: pointer.Bool(true)}); err != nil {
		l.Error(err, "Failed: UpdateService, Patch", "namespace", objcache.Namespace, "name", service.Name)
		return false, err
	}
	l.Info("Success: UpdateService, Patch", "name", service.Name)
	return false, nil
}

type MyRandString struct {
	xorState uint64
}

// https://en.wikipedia.org/wiki/Xorshift
func (r *MyRandString) xorShift() uint64 {
	for {
		old := atomic.LoadUint64(&r.xorState)
		var x = old
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		if atomic.CompareAndSwapUint64(&r.xorState, old, x) {
			return x
		}
	}
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func (r *MyRandString) Get(digit int64) string {
	b := make([]byte, digit)
	for i := int64(0); i < digit; i++ {
		b[i] = letters[r.xorShift()%uint64(len(letters))]
	}
	return string(b)
}

func (r *ObjcacheClusterReconciler) GetGroupId(seed int64, groupIndex int) (str string) {
	RandString := MyRandString{xorState: uint64(seed)}
	for i := 0; i <= groupIndex; i++ {
		str = RandString.Get(64)
	}
	return
}

func (r *ObjcacheClusterReconciler) UpdateStatefulSet(ctx context.Context, objcache *trlv1alpha1.ObjcacheCluster, l logr.Logger, groupIndex int) (err error) {
	groupName := fmt.Sprintf("%s-%d", objcache.Name, groupIndex)

	var sf, origApplyConfig *appsv1apply.StatefulSetApplyConfiguration
	orig := &appsv1.StatefulSet{}
	err = r.Get(ctx, client.ObjectKey{Name: groupName, Namespace: objcache.Namespace}, orig)
	if err != nil {
		if !errors.IsNotFound(err) {
			l.Error(err, "Failed: UpdateStatefulSet, Get", "groupName", groupName, "namespace", objcache.Namespace)
			return
		}
		sf = appsv1apply.StatefulSet(groupName, objcache.Namespace)
	} else {
		origApplyConfig, err = appsv1apply.ExtractStatefulSet(orig, fieldManager)
		if err != nil {
			l.Error(err, "Failed: UpdateStatefulSet, ExtractDeployment")
			return err
		}
		copied := *origApplyConfig
		sf = &copied
		sf.OwnerReferences = nil
		sf.Spec = nil
	}
	labels := GetWorkerLabels(objcache, groupName)
	sf.WithLabels(labels).
		WithSpec(appsv1apply.StatefulSetSpec().WithSelector(metav1apply.LabelSelector().WithMatchLabels(labels)).
			WithReplicas(int32(objcache.Spec.RaftFollowers + 1)).
			WithTemplate(corev1apply.PodTemplateSpec().
				WithLabels(labels)).WithServiceName(groupName))
	pod := sf.Spec.Template

	pod.WithAnnotations(GetAnnotationMapForPod(orig.GetAnnotations(), objcache.Spec.MultiNicName))
	if objcache.Spec.OverridePodSpec != nil {
		pod.WithSpec((*corev1apply.PodSpecApplyConfiguration)(objcache.Spec.OverridePodSpec.DeepCopy()))
	} else {
		pod.WithSpec(corev1apply.PodSpec())
	}
	if pod.Spec.TopologySpreadConstraints == nil {
		spreadLabels := GetWorkerLabels(objcache, groupName)
		delete(spreadLabels, "raft-groupId")
		pod.Spec.WithTopologySpreadConstraints(
			corev1apply.TopologySpreadConstraint().WithMaxSkew(1).
				WithTopologyKey("topology.kubernetes.io/zone").WithWhenUnsatisfiable(corev1.ScheduleAnyway).
				WithLabelSelector(metav1apply.LabelSelector().WithMatchLabels(spreadLabels)),
			corev1apply.TopologySpreadConstraint().WithMaxSkew(1).
				WithTopologyKey("kubernetes.io/hostname").WithWhenUnsatisfiable(corev1.ScheduleAnyway).
				WithLabelSelector(metav1apply.LabelSelector().WithMatchLabels(spreadLabels)))
	}
	var objcacheIndex = -1
	var hasObjcache = false
	for i, c := range pod.Spec.Containers {
		if *c.Name == "objcache" {
			objcacheIndex = i
			hasObjcache = true
			break
		}
	}
	if !hasObjcache {
		ratio := objcache.Spec.LocalVolume.CacheReplacementThreashold
		cacheCapacity := int64(float64(objcache.Spec.LocalVolume.Capacity.Value()) / 100 * float64(ratio))
		cacheCapacityInQuantity := objcache.Spec.LocalVolume.Capacity
		if cacheCapacity > 0 {
			cacheCapacityInQuantity.Set(cacheCapacity)
		}
		container := corev1apply.Container().WithName("objcache").
			WithImage(objcache.Spec.ObjcacheImage).WithImagePullPolicy(corev1.PullAlways).
			WithSecurityContext(corev1apply.SecurityContext().WithPrivileged(true).WithCapabilities(corev1apply.Capabilities().WithAdd("SYS_ADMIN")).WithAllowPrivilegeEscalation(true)).
			WithCommand("/objcache", "--listenIp=0.0.0.0", "--externalIp=$(POD_IP)", "--serverId=k8s", "--passiveRaftInit=k8s",
				fmt.Sprintf("--raftGroupId=%v", r.GetGroupId(objcache.Status.Seed, groupIndex)),
				fmt.Sprintf("--raftPort=%v", objcache.Spec.Port),
				fmt.Sprintf("--apiPort=%v", objcache.Spec.ApiPort),
				fmt.Sprintf("--headWorkerIp=%v-0-0.%v-0.%v.svc.cluster.local", objcache.Name, objcache.Name, objcache.Namespace),
				fmt.Sprintf("--headWorkerPort=%v", objcache.Spec.ApiPort),
				fmt.Sprintf("--rootDir=%v", objcache.Spec.LocalVolume.MountPath),
				fmt.Sprintf("--cacheCapacity=%v", cacheCapacityInQuantity.String()),
				fmt.Sprintf("--secretFile=%v/secret.yaml", objcache.Spec.LocalVolume.MountPath),
				fmt.Sprintf("--configFile=%v/config.yaml", objcache.Spec.LocalVolume.MountPath)).
			WithWorkingDir(objcache.Spec.LocalVolume.MountPath).
			WithLifecycle(corev1apply.Lifecycle().WithPreStop(corev1apply.LifecycleHandler().WithExec(corev1apply.ExecAction().
				WithCommand("/objcache-client", "--cmd=quit", "--targetIp=0.0.0.0", //NOTE: cannot pass env values
					fmt.Sprintf("--targetPort=%v", objcache.Spec.ApiPort),
					fmt.Sprintf("--stateFile=%v/state", objcache.Spec.LocalVolume.MountPath))))).
			WithEnv(corev1apply.EnvVar().WithName("NODE_ID").WithValueFrom(corev1apply.EnvVarSource().WithFieldRef(corev1apply.ObjectFieldSelector().WithAPIVersion("v1").WithFieldPath("spec.nodeName")))).
			WithEnv(corev1apply.EnvVar().WithName("POD_IP").WithValueFrom(corev1apply.EnvVarSource().WithFieldRef(corev1apply.ObjectFieldSelector().WithAPIVersion("v1").WithFieldPath("status.podIP")))).
			WithEnv(corev1apply.EnvVar().WithName("GOTRACEBACK").WithValue("crash")).
			WithVolumeMounts().
			WithVolumeMounts(corev1apply.VolumeMount().WithName("data-dir").WithMountPath(objcache.Spec.LocalVolume.MountPath)).
			WithVolumeMounts(corev1apply.VolumeMount().WithName("secret-file").WithMountPath(fmt.Sprintf("%v/secret.yaml", objcache.Spec.LocalVolume.MountPath)).WithReadOnly(true).WithSubPath("secret.yaml")).
			WithVolumeMounts(corev1apply.VolumeMount().WithName("config-file").WithMountPath(fmt.Sprintf("%v/config.yaml", objcache.Spec.LocalVolume.MountPath)).WithReadOnly(true).WithSubPath("config.yaml"))
		if objcache.Spec.Resources != nil {
			container.WithResources((*corev1apply.ResourceRequirementsApplyConfiguration)(objcache.Spec.Resources))
		}
		if objcache.Spec.GoDebugEnvVar != nil {
			if *objcache.Spec.GoDebugEnvVar == "" {
				container.WithEnv(corev1apply.EnvVar().WithName("GODEBUG"))
			} else {
				container.WithEnv(corev1apply.EnvVar().WithName("GODEBUG").WithValue(*objcache.Spec.GoDebugEnvVar))
			}
		}
		if objcache.Spec.GoMemLimit != nil {
			if *objcache.Spec.GoMemLimit == "" {
				container.WithEnv(corev1apply.EnvVar().WithName("GOMEMLIMIT"))
			} else {
				container.WithEnv(corev1apply.EnvVar().WithName("GOMEMLIMIT").WithValue(*objcache.Spec.GoMemLimit))
			}
		}
		pod.Spec.WithContainers(container)
		objcacheIndex = len(pod.Spec.Containers) - 1
		pvc := &corev1apply.PersistentVolumeClaimApplyConfiguration{}
		if objcache.Spec.LocalVolume.StorageClass != "" {
			pvc.WithName("data-dir").WithSpec(corev1apply.PersistentVolumeClaimSpec().
				WithStorageClassName(objcache.Spec.LocalVolume.StorageClass).WithAccessModes(corev1.ReadWriteOnce).
				WithVolumeMode(corev1.PersistentVolumeFilesystem).
				WithResources(corev1apply.ResourceRequirements().WithRequests(corev1.ResourceList{"storage": objcache.Spec.LocalVolume.Capacity}))).
				WithStatus(corev1apply.PersistentVolumeClaimStatus().WithPhase(corev1.ClaimPending)) // prevent from unnecessary patch
			//ret := &appsv1apply.StatefulSetPersistentVolumeClaimRetentionPolicyApplyConfiguration{}
			//ret.WithWhenDeleted(appsv1.DeletePersistentVolumeClaimRetentionPolicyType).WithWhenScaled(appsv1.DeletePersistentVolumeClaimRetentionPolicyType)
		}
		sf.Spec.WithVolumeClaimTemplates(pvc) //.WithPersistentVolumeClaimRetentionPolicy(ret) // NOTE: this is enabled at alpha versions
	}
	hasLogDir := false
	for _, v := range pod.Spec.Volumes {
		if *v.Name == "log-dir" {
			hasLogDir = true
		}
	}
	pod.Spec.WithVolumes(corev1apply.Volume().WithName("secret-file").WithSecret(corev1apply.SecretVolumeSource().WithSecretName(objcache.Spec.Secret)))
	pod.Spec.WithVolumes(corev1apply.Volume().WithName("config-file").WithConfigMap(corev1apply.ConfigMapVolumeSource().WithName(objcache.Name)))
	if objcache.Spec.LocalVolume.DebugLogPvc != nil && *objcache.Spec.LocalVolume.DebugLogPvc != "" {
		if !hasObjcache {
			pod.Spec.Containers[objcacheIndex].Command = append(pod.Spec.Containers[objcacheIndex].Command, fmt.Sprintf("--logFile=%v/log/$(HOSTNAME).log", objcache.Spec.LocalVolume.MountPath))
			pod.Spec.Containers[objcacheIndex].WithVolumeMounts(corev1apply.VolumeMount().WithName("log-dir").WithMountPath(fmt.Sprintf("%v/log", objcache.Spec.LocalVolume.MountPath)))
		}
		if !hasLogDir {
			pod.Spec.WithVolumes(corev1apply.Volume().WithName("log-dir").WithPersistentVolumeClaim(corev1apply.PersistentVolumeClaimVolumeSource().WithClaimName(*objcache.Spec.LocalVolume.DebugLogPvc)))
		}
	}
	gvk, err := apiutil.GVKForObject(objcache, r.Scheme)
	if err != nil {
		l.Error(err, "Failed: UpdateStatefulSet, GVKForObject")
		return err
	}
	sf.WithOwnerReferences(metav1apply.OwnerReference().
		WithAPIVersion(gvk.GroupVersion().String()).
		WithKind(gvk.Kind).
		WithName(objcache.Name).
		WithUID(objcache.GetUID()).
		WithBlockOwnerDeletion(true).
		WithController(true))

	firstApply := origApplyConfig == nil
	if !firstApply {
		if equality.Semantic.DeepEqual(sf, origApplyConfig) {
			return nil
		}
		diff := cmp.Diff(*origApplyConfig, *sf)
		if len(diff) > 0 {
			l.Info("UpdateStatefulSet, Patch", "diff", diff)
		}
	}

	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(sf)
	if err != nil {
		l.Error(err, "Failed: UpdateStatefulSet, ToUnstructured")
		return err
	}
	patch := &unstructured.Unstructured{Object: obj}
	if err = r.Patch(ctx, patch, client.Apply, &client.PatchOptions{FieldManager: fieldManager, Force: pointer.Bool(true)}); err != nil {
		l.Error(err, "Failed: UpdateStatefulSet, Patch", "namespace", *sf.Namespace, "name", *sf.Name)
		return err
	}
	if firstApply {
		l.Info("Success: UpdateStatefulSet, Patch (first)", "namespace", objcache.Namespace, "name", groupName, "shards", objcache.Spec.Shards, "raftFollowers", objcache.Spec.RaftFollowers)
		time.Sleep(time.Second)
	}
	return nil
}

func (r *ObjcacheClusterReconciler) DeleteStatefulSet(ctx context.Context, objcache *trlv1alpha1.ObjcacheCluster, l logr.Logger, groupIndex int) error {
	groupName := fmt.Sprintf("%s-%d", objcache.Name, groupIndex)
	found := &appsv1.StatefulSet{}
	err := r.Get(ctx, client.ObjectKey{Name: groupName, Namespace: objcache.Namespace}, found)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		l.Error(err, "Failed: DeleteStatefulSet, Get")
		return err
	}
	if err = r.Delete(ctx, found); err != nil {
		l.Error(err, "Failed: DeleteStatefulSet, Delete", "namespace", found.Namespace, "name", found.Name)
		return err
	}
	l.Info("Success: DeleteStatefulSet, Delete", "namespace", found.Namespace, "name", found.Name)
	return nil
}

func (r *ObjcacheClusterReconciler) DeleteStatefulSetVolume(ctx context.Context, objcache *trlv1alpha1.ObjcacheCluster, l logr.Logger, groupIndex int, raftIndex int) error {
	pvcName := fmt.Sprintf("data-dir-%s-%d-%d", objcache.Name, groupIndex, raftIndex)
	found := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, client.ObjectKey{Name: pvcName, Namespace: objcache.Namespace}, found)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		l.Error(err, "Failed: DeleteStatefulSetVolume, Get")
		return err
	}
	if err = r.Delete(ctx, found); err != nil {
		l.Error(err, "Failed: DeleteStatefulSetVolume, Delete", "namespace", found.Namespace, "name", found.Name)
		return err
	}
	l.Info("Success: DeleteStatefulSetVolume, Delete", "namespace", found.Namespace, "name", found.Name)
	return nil
}
