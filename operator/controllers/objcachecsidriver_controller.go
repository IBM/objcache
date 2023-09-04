/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

	trlv1alpha1 "github.ibm.com/TYOS/objcache/operator/api/v1alpha1"
)

// ObjcacheCsiDriverReconciler reconciles a ObjcacheCsiDriver object
type ObjcacheCsiDriverReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const objcacheFinalizer = "trl.ibm.com/finalizer"
const fieldManager = "objcache-operator"
const driverName = "objcache.csi.trl.ibm.com"
const objcacheVersion = "v0.1.0"
const monolithicDeployment = "monolithic"
const isolatedDeployment = "isolated"

var instanceName = ""
var instanceNamespace = ""

func GetAnnotationMap() map[string]string {
	return map[string]string{"objcacheVersion": objcacheVersion}
}

func GetAnnotationMapForPod(orig map[string]string, multinicName *string) map[string]string {
	ret := GetAnnotationMap()
	if orig != nil {
		for key, value := range orig {
			ret[key] = value
		}
	}
	ret["cluster-autoscaler.kubernetes.io/safe-to-evict"] = "true"
	if multinicName != nil && *multinicName != "" {
		ret["k8s.v1.cni.cncf.io/networks"] = *multinicName
	}
	return ret
}

//+kubebuilder:rbac:groups=trl.ibm.com,resources=objcachecsidrivers,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=trl.ibm.com,resources=objcachecsidrivers/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=trl.ibm.com,resources=objcachecsidrivers/finalizers,verbs=update

//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete;deletecollection
//+kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch;create;update;patch;delete;deletecollection
//+kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
//+kubebuilder:rbac:groups=storage.k8s.io,resources=csidrivers,verbs=get;list;watch;delete;create;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ObjcacheCsiDriver object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *ObjcacheCsiDriverReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	l := logger.WithValues("Objcache", req.NamespacedName)
	objcache := &trlv1alpha1.ObjcacheCsiDriver{}
	err := r.Get(ctx, req.NamespacedName, objcache)
	var requeue = false
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		l.Error(err, "Failed: Reconcile, Get", "namespace", req.Namespace, "name", req.Name)
		return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, err
	}
	if instanceName != "" && instanceNamespace != "" {
		if instanceName != objcache.Name || instanceNamespace != objcache.Namespace {
			err = unix.EINVAL
			l.Error(err, "Failed: Reconcile, an active instance is already running, ignore", "request", req)
			return ctrl.Result{Requeue: false, RequeueAfter: 100 * time.Millisecond}, err
		}
	}

	// Add finalizer to instance
	if !controllerutil.ContainsFinalizer(objcache, objcacheFinalizer) {
		controllerutil.AddFinalizer(objcache, objcacheFinalizer)
		err = r.Update(ctx, objcache)
		if err != nil {
			l.Error(err, "Failed: Reconcile, Update")
			return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, err
		}
	}
	if objcache.GetDeletionTimestamp() != nil {
		if controllerutil.ContainsFinalizer(objcache, objcacheFinalizer) {
			requeue, err = r.DeleteCluster(ctx, objcache, l)
			if err == nil && !requeue {
				instanceName = ""
				instanceNamespace = ""
			}
		}
	} else {
		requeue, err = r.UpdateCluster(ctx, objcache, l)
		if err == nil && !requeue {
			instanceName = objcache.Name
			instanceNamespace = objcache.Namespace
		}
	}
	if requeue || err != nil {
		return ctrl.Result{Requeue: requeue, RequeueAfter: 100 * time.Millisecond}, err
	}
	return ctrl.Result{Requeue: requeue}, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *ObjcacheCsiDriverReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&trlv1alpha1.ObjcacheCsiDriver{}).
		Owns(&storagev1.CSIDriver{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.DaemonSet{}).
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

func (r *ObjcacheCsiDriverReconciler) DeleteCluster(ctx context.Context, objcache *trlv1alpha1.ObjcacheCsiDriver, l logr.Logger) (requeue bool, err error) {
	if requeue, err = r.DeleteCsiDriver(ctx, objcache, l); err != nil {
		return
	}
	if err = r.DeleteControllerDeployment(ctx, objcache, l); err != nil {
		return
	}
	if requeue, err = r.DeleteNodes(ctx, objcache, l); err != nil {
		return
	}
	if err = r.DeleteConfigMap(ctx, objcache, l); err != nil {
		return
	}
	controllerutil.RemoveFinalizer(objcache, objcacheFinalizer)
	err = r.Update(ctx, objcache)
	if err != nil {
		l.Error(err, "DeleteCluster, Update")
	}
	return
}

func (r *ObjcacheCsiDriverReconciler) UpdateCluster(ctx context.Context, objcache *trlv1alpha1.ObjcacheCsiDriver, l logr.Logger) (requeue bool, err error) {
	if requeue, err = r.UpdateConfigMap(ctx, objcache, l); err != nil || requeue {
		return
	}
	if requeue, err = r.UpdateNodes(ctx, objcache, l); err != nil || requeue {
		return
	}
	if requeue, err = r.UpdateControllerDeployment(ctx, objcache, l); err != nil || requeue {
		return
	}
	if requeue, err = r.UpdateCsiDriver(ctx, objcache, l); err != nil || requeue {
		return
	}
	return false, nil
}

func (r *ObjcacheCsiDriverReconciler) UpdateConfigMap(ctx context.Context, objcache *trlv1alpha1.ObjcacheCsiDriver, l logr.Logger) (bool, error) {
	yamlData, err := yaml.Marshal(objcache.Spec.FuseConfig)
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

func (r *ObjcacheCsiDriverReconciler) DeleteConfigMap(ctx context.Context, objcache *trlv1alpha1.ObjcacheCsiDriver, l logr.Logger) error {
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

func GetControllerName(objcache *trlv1alpha1.ObjcacheCsiDriver) string {
	return fmt.Sprintf("%v-controller", objcache.Name)
}

func GetControllerLabels(objcache *trlv1alpha1.ObjcacheCsiDriver) map[string]string {
	return map[string]string{
		"app": "objcache", "cluster": objcache.Name, "component": "csi-controller",
	}
}

func GetNodeName(name string) string {
	return fmt.Sprintf("%v-node", name)
}

func GetNodeLabels(objcache *trlv1alpha1.ObjcacheCsiDriver) map[string]string {
	return map[string]string{
		"app": "objcache", "cluster": objcache.Name, "component": "csi-node",
	}
}

func (r *ObjcacheCsiDriverReconciler) DeleteControllerDeployment(ctx context.Context, objcache *trlv1alpha1.ObjcacheCsiDriver, l logr.Logger) error {
	controllerName := GetControllerName(objcache)
	found := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: controllerName, Namespace: objcache.Namespace}, found)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		l.Error(err, "Failed: DeleteControllerDeployment, Get")
		return err
	}
	if err = r.Delete(ctx, found); err != nil {
		l.Error(err, "Failed: DeleteControllerDeployment, Delete", "namespace", found.Namespace, "name", found.Name)
		return err
	}
	l.Info("Success: DeleteControllerDeployment, Delete", "namespace", found.Namespace, "name", found.Name)
	return nil
}

func (r *ObjcacheCsiDriverReconciler) UpdateControllerDeployment(ctx context.Context, objcache *trlv1alpha1.ObjcacheCsiDriver, l logr.Logger) (bool, error) {
	controllerName := GetControllerName(objcache)
	var dep, origApplyConfig *appsv1apply.DeploymentApplyConfiguration
	var orig appsv1.Deployment
	err := r.Get(ctx, client.ObjectKey{Name: controllerName, Namespace: objcache.Namespace}, &orig)
	if err != nil && !errors.IsNotFound(err) {
		l.Error(err, "Failed: UpdateControllerDeployment, Get")
		return false, err
	} else if err == nil {
		origApplyConfig, err = appsv1apply.ExtractDeployment(&orig, fieldManager)
		if err != nil {
			l.Error(err, "Failed: UpdateReplicaWorker, ExtractDeployment")
			return false, err
		}
		copied := *origApplyConfig
		dep = &copied
		dep.OwnerReferences = nil
		dep.WithSpec(appsv1apply.DeploymentSpec())
	} else {
		dep = appsv1apply.Deployment(controllerName, objcache.Namespace)
	}

	labels := GetControllerLabels(objcache)
	dep.WithLabels(labels).WithAnnotations(GetAnnotationMap()).
		WithSpec(appsv1apply.DeploymentSpec().WithReplicas(1).
			WithSelector(metav1apply.LabelSelector().WithMatchLabels(labels)))

	if objcache.Spec.OverridePodTemplateSpec != nil {
		dep.Spec.WithTemplate((*corev1apply.PodTemplateSpecApplyConfiguration)(objcache.Spec.OverridePodTemplateSpec.DeepCopy()))
	} else {
		dep.Spec.WithTemplate(corev1apply.PodTemplateSpec().WithSpec(&corev1apply.PodSpecApplyConfiguration{}))
	}
	dep.Spec.Template.WithAnnotations(map[string]string{"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"})
	dep.Spec.Template.WithLabels(labels)
	dep.Spec.Template.Spec.WithServiceAccountName(objcache.Spec.ServiceAccount)
	hasCsiController := false
	hasAttacher := false
	hasProvisioner := false
	for _, c := range dep.Spec.Template.Spec.Containers {
		if *c.Name == "csi-controller" {
			hasCsiController = true
		}
		if *c.Name == "csi-attacher" {
			hasAttacher = true
		}
		if *c.Name == "csi-provisioner" {
			hasProvisioner = true
		}
	}
	hasSocketDir := false
	for _, v := range dep.Spec.Template.Spec.Volumes {
		if *v.Name == "socket-dir" {
			hasSocketDir = true
		}
	}
	if !hasCsiController {
		dep.Spec.Template.Spec.WithContainers(corev1apply.Container().WithName("csi-controller").
			WithImage(objcache.Spec.Images.ObjcacheCsiController).WithImagePullPolicy(corev1.PullAlways).
			WithCommand("/objcache-csi-controller", "--endpoint=unix:///csi/csi.sock",
				fmt.Sprintf("--driverName=%v", driverName),
				fmt.Sprintf("--driverVersion=%v", objcacheVersion)).
			WithVolumeMounts(&corev1apply.VolumeMountApplyConfiguration{
				Name: pointer.String("socket-dir"), MountPath: pointer.String("/csi"),
			}))
	}
	if !hasAttacher {
		dep.Spec.Template.Spec.WithContainers(corev1apply.Container().WithName("csi-attacher").
			WithImage(objcache.Spec.Images.Attacher).WithImagePullPolicy(corev1.PullIfNotPresent).
			WithArgs("--csi-address=/csi/csi.sock", "--v=5").
			WithVolumeMounts(&corev1apply.VolumeMountApplyConfiguration{
				Name: pointer.String("socket-dir"), MountPath: pointer.String("/csi"),
			}))
	}
	if !hasProvisioner {
		dep.Spec.Template.Spec.WithContainers(corev1apply.Container().WithName("csi-provisioner").
			WithImage(objcache.Spec.Images.Provisioner).WithImagePullPolicy(corev1.PullIfNotPresent).
			WithArgs("--csi-address=/csi/csi.sock", "--v=5", "--feature-gates=Topology=true").
			WithVolumeMounts(&corev1apply.VolumeMountApplyConfiguration{
				Name: pointer.String("socket-dir"), MountPath: pointer.String("/csi"),
			}))
	}
	if !hasSocketDir {
		dep.Spec.Template.Spec.WithVolumes(corev1apply.Volume().WithName("socket-dir"))
	}

	gvk, err := apiutil.GVKForObject(objcache, r.Scheme)
	if err != nil {
		l.Error(err, "Failed: UpdateControllerDeployment, GVKForObject")
		return false, err
	}
	dep.WithOwnerReferences(metav1apply.OwnerReference().
		WithAPIVersion(gvk.GroupVersion().String()).
		WithKind(gvk.Kind).
		WithName(objcache.Name).
		WithUID(objcache.GetUID()).
		WithBlockOwnerDeletion(true).
		WithController(true))
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(dep)
	if err != nil {
		return false, err
	}
	patch := &unstructured.Unstructured{Object: obj}

	firstApply := origApplyConfig == nil
	if !firstApply {
		if equality.Semantic.DeepEqual(dep, origApplyConfig) {
			return false, nil
		}
		diff := cmp.Diff(*origApplyConfig, *dep)
		if len(diff) > 0 {
			l.Info("UpdateControllerDeployment, Patch", "diff", diff)
		}
	}
	if err = r.Patch(ctx, patch, client.Apply, &client.PatchOptions{FieldManager: fieldManager, Force: pointer.Bool(true)}); err != nil {
		l.Error(err, "Failed: UpdateControllerDeployment, Patch", "namespace", *dep.Namespace, "name", *dep.Name)
		return false, err
	}
	if firstApply {
		l.Info("Success: UpdateControllerDeployment, Patch (first)", "namespace", *dep.Namespace, "name", *dep.Name)
	}
	return false, nil
}

func (r *ObjcacheCsiDriverReconciler) DeleteCsiDriver(ctx context.Context, objcache *trlv1alpha1.ObjcacheCsiDriver, l logr.Logger) (bool, error) {
	found := &storagev1.CSIDriver{}
	key := client.ObjectKey{Name: driverName}
	err := r.Get(ctx, key, found)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		l.Error(err, "Failed: DeleteCsiDriver, Get")
		return false, err
	}
	if err = r.Delete(ctx, found); err != nil {
		l.Error(err, "Failed: DeleteCsiDriver, Delete", "name", found.Name)
		return false, err
	}
	err = r.Get(ctx, key, found)
	if err == nil {
		return true, nil
	}
	l.Info("Success: DeleteCsiDriver, Delete", "name", found.Name)
	return false, nil
}

func (r *ObjcacheCsiDriverReconciler) UpdateCsiDriver(ctx context.Context, objcache *trlv1alpha1.ObjcacheCsiDriver, l logr.Logger) (bool, error) {
	csiDriver := storagev1apply.CSIDriver(driverName).WithLabels(map[string]string{"app": "objcache"}).WithAnnotations(GetAnnotationMap()).
		WithSpec(storagev1apply.CSIDriverSpec().WithAttachRequired(false))
	gvk, err := apiutil.GVKForObject(objcache, r.Scheme)
	if err != nil {
		l.Error(err, "Failed: UpdateCsiDriver, GVKForObject")
		return false, err
	}
	csiDriver.WithOwnerReferences(metav1apply.OwnerReference().
		WithAPIVersion(gvk.GroupVersion().String()).
		WithKind(gvk.Kind).
		WithName(objcache.Name).
		WithUID(objcache.GetUID()).
		WithBlockOwnerDeletion(true).
		WithController(true))
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(csiDriver)
	if err != nil {
		return false, err
	}
	patch := &unstructured.Unstructured{Object: obj}

	orig := &storagev1.CSIDriver{}
	err = r.Get(ctx, client.ObjectKey{Name: *csiDriver.Name}, orig)
	if err != nil && !errors.IsNotFound(err) {
		l.Error(err, "Failed: UpdateCsiDriver, Get")
		return false, err
	}
	if err == nil {
		origApplyConfig, err := storagev1apply.ExtractCSIDriver(orig, fieldManager)
		if err != nil {
			l.Error(err, "Failed: UpdateCsiDriver, ExtractCSIDriver")
			return false, err
		}
		if equality.Semantic.DeepEqual(csiDriver, origApplyConfig) {
			return false, nil
		}
	}
	if err = r.Patch(ctx, patch, client.Apply, &client.PatchOptions{FieldManager: fieldManager, Force: pointer.Bool(true)}); err != nil {
		l.Error(err, "Failed: UpdateCsiDriver, Patch", "namespace", objcache.Namespace, "name", driverName)
		return false, err
	}
	l.Info("Success: UpdateCsiDriver, Patch", "namespace", objcache.Namespace, "name", driverName)
	return false, nil
}

func (r *ObjcacheCsiDriverReconciler) UpdateNodes(ctx context.Context, objcache *trlv1alpha1.ObjcacheCsiDriver, l logr.Logger) (bool, error) {
	nodeName := GetNodeName(objcache.Name)
	var ds, origApplyConfig *appsv1apply.DaemonSetApplyConfiguration
	var orig appsv1.DaemonSet
	var origAnnotation map[string]string
	err := r.Get(ctx, client.ObjectKey{Name: nodeName, Namespace: objcache.Namespace}, &orig)
	if err != nil && !errors.IsNotFound(err) {
		l.Error(err, "Failed: UpdateNodes, Get")
		return false, err
	} else if err == nil {
		origApplyConfig, err = appsv1apply.ExtractDaemonSet(&orig, fieldManager)
		if err != nil {
			l.Error(err, "Failed: UpdateNodes, ExtractDaemonSet")
			return false, err
		}
		origAnnotation = orig.Spec.Template.GetAnnotations()
		copied := *origApplyConfig
		ds = &copied
		ds.OwnerReferences = nil
	} else {
		ds = appsv1apply.DaemonSet(nodeName, objcache.Namespace)
	}
	labels := GetNodeLabels(objcache)
	ds.WithLabels(labels).
		WithSpec(appsv1apply.DaemonSetSpec().
			WithSelector(metav1apply.LabelSelector().WithMatchLabels(labels)))

	rootDir := fmt.Sprintf("%v/%v", objcache.Spec.LocalVolume.MountPath, nodeName)
	var kubeletDir = "/var/data/kubelet"
	var pluginsDir = "csi-plugins"
	if objcache.Spec.EnvStr == "ocp-4.10.20" {
		kubeletDir = "/var/lib/kubelet"
		pluginsDir = "plugins"
	}
	pluginsDir = fmt.Sprintf("%v/%v/%v", kubeletDir, pluginsDir, driverName)

	var pod *corev1apply.PodTemplateSpecApplyConfiguration
	if objcache.Spec.OverridePodTemplateSpec != nil {
		pod = (*corev1apply.PodTemplateSpecApplyConfiguration)(objcache.Spec.OverridePodTemplateSpec.DeepCopy())
	} else {
		pod = corev1apply.PodTemplateSpec().WithSpec(&corev1apply.PodSpecApplyConfiguration{})
	}
	ds.Spec.WithTemplate(pod.WithLabels(labels).
		WithAnnotations(GetAnnotationMapForPod(origAnnotation, objcache.Spec.MultiNicName)))
	if pod.Spec.ServiceAccountName == nil {
		pod.Spec.WithServiceAccountName(objcache.Spec.ServiceAccount)
	}
	hasDriverReigstrar := false
	var objcacheIndex = -1
	var hasObjcache = false
	for i, c := range pod.Spec.Containers {
		if *c.Name == "driver-registrar" {
			hasDriverReigstrar = true
		}
		if *c.Name == "objcache" {
			objcacheIndex = i
			hasObjcache = true
		}
	}
	if !hasDriverReigstrar {
		pod.Spec.WithContainers(corev1apply.Container().WithName("driver-registrar").
			WithImage(objcache.Spec.Images.DriverRegistar).WithImagePullPolicy(corev1.PullIfNotPresent).
			WithArgs(fmt.Sprintf("--kubelet-registration-path=%v/csi.sock", pluginsDir), "--v=5", "--csi-address=/csi/csi.sock").
			WithVolumeMounts(corev1apply.VolumeMount().WithName("plugin-dir").WithMountPath("/csi")).
			WithVolumeMounts(corev1apply.VolumeMount().WithName("registration-dir").WithMountPath("/registration")))
	}
	if !hasObjcache {
		command := []string{
			"/objcache-csi-node", "--endpoint=unix:///csi/csi.sock", "--nodeId=$(NODE_ID)", "--externalIp=$(POD_IP)",
			"--deploymentModel=isolated",
			fmt.Sprintf("--rootDir=%v", objcache.Spec.LocalVolume.MountPath),
			fmt.Sprintf("--driverName=%v", driverName),
			fmt.Sprintf("--driverVersion=%v", objcacheVersion),
			"--fuse.binPath=/objcache-fuse",
			fmt.Sprintf("--fuse.configFile=%v/config.yaml", rootDir),
			fmt.Sprintf("--fuse.portBegin=%v", objcache.Spec.FusePortBegin),
			fmt.Sprintf("--fuse.portEnd=%v", objcache.Spec.FusePortEnd),
		}
		if objcache.Spec.FuseGoDebugEnvVar != nil {
			command = append(command, fmt.Sprintf("--fuse.goDebugEnvVar=\"%v\"", *objcache.Spec.FuseGoDebugEnvVar))
		}
		if objcache.Spec.FuseGoMemLimit != nil {
			command = append(command, fmt.Sprintf("--fuse.goMemLimit=%v", *objcache.Spec.FuseGoMemLimit))
		}
		container := corev1apply.Container().WithName("objcache").
			WithImage(objcache.Spec.Images.ObjcacheCsiNode).WithImagePullPolicy(corev1.PullAlways).
			WithSecurityContext(corev1apply.SecurityContext().WithPrivileged(true).WithCapabilities(corev1apply.Capabilities().WithAdd("SYS_ADMIN")).WithAllowPrivilegeEscalation(true)).
			WithCommand(command...).
			WithWorkingDir(rootDir).
			WithEnv(corev1apply.EnvVar().WithName("NODE_ID").WithValueFrom(corev1apply.EnvVarSource().WithFieldRef(corev1apply.ObjectFieldSelector().WithAPIVersion("v1").WithFieldPath("spec.nodeName")))).
			WithEnv(corev1apply.EnvVar().WithName("POD_IP").WithValueFrom(corev1apply.EnvVarSource().WithFieldRef(corev1apply.ObjectFieldSelector().WithAPIVersion("v1").WithFieldPath("status.podIP")))).
			WithEnv(corev1apply.EnvVar().WithName("GOTRACEBACK").WithValue("crash")).
			WithVolumeMounts(corev1apply.VolumeMount().WithName("plugin-dir").WithMountPath("/csi")).
			WithVolumeMounts(corev1apply.VolumeMount().WithName("kubelet-dir").WithMountPath(kubeletDir).WithMountPropagation(corev1.MountPropagationBidirectional)).
			WithVolumeMounts(corev1apply.VolumeMount().WithName("fuse-device").WithMountPath("/dev/fuse")).
			WithVolumeMounts(corev1apply.VolumeMount().WithName("data-dir").WithMountPath(objcache.Spec.LocalVolume.MountPath)).
			WithVolumeMounts(corev1apply.VolumeMount().WithName("config-file").WithMountPath(fmt.Sprintf("%v/config.yaml", rootDir)).WithReadOnly(true).WithSubPath("config.yaml"))
		pod.Spec.WithContainers(container)
		objcacheIndex = len(pod.Spec.Containers) - 1
	}
	hasRegistrationDir := false
	hasPluginDir := false
	hasKubeletDir := false
	hasFuseDevice := false
	hasDataDir := false
	hasLogDir := false
	for _, v := range pod.Spec.Volumes {
		if *v.Name == "registration-dir" {
			hasRegistrationDir = true
		}
		if *v.Name == "plugin-dir" {
			hasPluginDir = true
		}
		if *v.Name == "kubelet-dir" {
			hasKubeletDir = true
		}
		if *v.Name == "fuse-device" {
			hasFuseDevice = true
		}
		if *v.Name == "data-dir" {
			hasDataDir = true
		}
		if *v.Name == "log-dir" {
			hasLogDir = true
		}
	}
	if !hasRegistrationDir {
		pod.Spec.WithVolumes(corev1apply.Volume().WithName("registration-dir").WithHostPath(corev1apply.HostPathVolumeSource().WithPath(fmt.Sprintf("%v/plugins_registry/", kubeletDir)).WithType(corev1.HostPathDirectoryOrCreate)))
	}
	if !hasPluginDir {
		pod.Spec.WithVolumes(corev1apply.Volume().WithName("plugin-dir").WithHostPath(corev1apply.HostPathVolumeSource().WithPath(pluginsDir).WithType(corev1.HostPathDirectoryOrCreate)))
	}
	if !hasKubeletDir {
		pod.Spec.WithVolumes(corev1apply.Volume().WithName("kubelet-dir").WithHostPath(corev1apply.HostPathVolumeSource().WithPath(kubeletDir).WithType(corev1.HostPathDirectory)))
	}
	if !hasFuseDevice {
		pod.Spec.WithVolumes(corev1apply.Volume().WithName("fuse-device").WithHostPath(corev1apply.HostPathVolumeSource().WithPath("/dev/fuse")))
	}
	if !hasDataDir {
		if objcache.Spec.LocalVolume.StorageClass != "" {
			pod.Spec.WithVolumes(corev1apply.Volume().WithName("data-dir").WithEphemeral(corev1apply.EphemeralVolumeSource().
				WithVolumeClaimTemplate(corev1apply.PersistentVolumeClaimTemplate().WithSpec(corev1apply.PersistentVolumeClaimSpec().
					WithStorageClassName(objcache.Spec.LocalVolume.StorageClass).WithAccessModes(corev1.ReadWriteOnce).WithVolumeMode(corev1.PersistentVolumeFilesystem).
					WithResources(corev1apply.ResourceRequirements().WithRequests(corev1.ResourceList{"storage": objcache.Spec.LocalVolume.Capacity}))))))
		} else {
			pod.Spec.WithVolumes(corev1apply.Volume().WithName("data-dir").WithEmptyDir(corev1apply.EmptyDirVolumeSource().WithSizeLimit(objcache.Spec.LocalVolume.Capacity)))
		}
	}
	pod.Spec.WithVolumes(corev1apply.Volume().WithName("config-file").WithConfigMap(corev1apply.ConfigMapVolumeSource().WithName(objcache.Name)))
	if objcache.Spec.LocalVolume.DebugLogPvc != nil {
		if !hasObjcache {
			pod.Spec.Containers[objcacheIndex].Command = append(pod.Spec.Containers[objcacheIndex].Command, fmt.Sprintf("--logFile=%v/log/%v.log", objcache.Spec.LocalVolume.MountPath, nodeName))
			pod.Spec.Containers[objcacheIndex].WithVolumeMounts(corev1apply.VolumeMount().WithName("log-dir").WithMountPath(fmt.Sprintf("%v/log", objcache.Spec.LocalVolume.MountPath)))
		}
		if !hasLogDir {
			pod.Spec.WithVolumes(corev1apply.Volume().WithName("log-dir").WithPersistentVolumeClaim(corev1apply.PersistentVolumeClaimVolumeSource().WithClaimName(*objcache.Spec.LocalVolume.DebugLogPvc)))
		}
	}
	gvk, err := apiutil.GVKForObject(objcache, r.Scheme)
	if err != nil {
		l.Error(err, "Failed: UpdateNodes, GVKForObject")
		return false, err
	}
	ds.WithOwnerReferences(metav1apply.OwnerReference().
		WithAPIVersion(gvk.GroupVersion().String()).
		WithKind(gvk.Kind).
		WithName(objcache.Name).
		WithUID(objcache.GetUID()).
		WithBlockOwnerDeletion(true).
		WithController(true))

	firstApply := origApplyConfig == nil
	if !firstApply {
		if equality.Semantic.DeepEqual(ds, origApplyConfig) {
			return false, nil
		}
		diff := cmp.Diff(*origApplyConfig, *ds)
		if len(diff) > 0 {
			l.Info("UpdateNodes, Patch", "diff", diff)
		}
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(ds)
	if err != nil {
		l.Error(err, "Failed: UpdateNodes, ToUnstructured")
		return false, err
	}
	patch := &unstructured.Unstructured{Object: obj}
	if err = r.Patch(ctx, patch, client.Apply, &client.PatchOptions{FieldManager: fieldManager, Force: pointer.Bool(true)}); err != nil {
		l.Error(err, "Failed: UpdateNodes, Patch", "namespace", *ds.Namespace, "name", *ds.Name)
		return false, err
	}
	if firstApply {
		l.Info("Success: UpdateNodes, Patch (first)", "namespace", *ds.Namespace, "name", *ds.Name)
	}
	return false, nil
}

type NetworkStatus struct {
	Name   string   `json:"name"`
	IFName string   `json:"interface"`
	IPs    []string `json:"ips"`
}

const NetworkStatusAnnotation = "k8s.v1.cni.cncf.io/network-status"

func (r *ObjcacheCsiDriverReconciler) WaitAndGetPodIp(ctx context.Context, namespace string, podName string, iFName string, l logr.Logger) (externalIp string, notReady bool, err error) {
	notReady = true
	pod := &corev1.Pod{}
	err = r.Get(ctx, client.ObjectKey{Name: podName, Namespace: namespace}, pod)
	if err != nil {
		return
	}
	podIp := pod.Status.PodIP
	if iFName != "" {
		var re *regexp.Regexp
		re, err = regexp.Compile("net1-([0-9]+)")
		if err != nil {
			l.Error(err, "Failed: WaitAndGetPodIp, cannot compile regex")
			return
		}
		result := re.FindAllStringSubmatch(iFName, -1)
		if len(result) == 0 || len(result[0]) < 2 {
			l.Error(err, "Failed: WaitAndGetPodIp, cannot parse iFName (Note: currently we only support net1-*)", "IFName", iFName)
			return
		}
		var index int64
		index, err = strconv.ParseInt(result[0][1], 10, 64)
		if err != nil {
			l.Error(err, "Failed: WaitAndGetPodIp, cannot parse index", "interface name", result[0])
			return
		}

		stateStr, ok := pod.GetObjectMeta().GetAnnotations()[NetworkStatusAnnotation]
		if !ok {
			l.Error(err, "Failed: WaitAndGetPodIp, pod is not annotated", "key", NetworkStatusAnnotation, "annotations", pod.GetObjectMeta().GetAnnotations())
			time.Sleep(time.Second * 5)
			return
		}
		var statusObj []NetworkStatus
		err = json.Unmarshal([]byte(stateStr), &statusObj)
		if err != nil {
			l.Info("WaitAndGetPodIp, cannot unmarshal json", "str", stateStr)
			return
		}
		for _, status := range statusObj {
			if re.MatchString(status.IFName) {
				if int64(len(status.IPs)) <= index {
					err = unix.EOVERFLOW
					l.Error(err, "WaitAndGetPodIp, pod does not have enough IPs", "ips", status.IPs, "target index", index)
					return
				}
				externalIp = status.IPs[index]
				notReady = false
				break
			}
		}
	} else {
		externalIp = podIp
		notReady = podIp == ""
	}
	return
}

func (r *ObjcacheCsiDriverReconciler) DeleteNodes(ctx context.Context, objcache *trlv1alpha1.ObjcacheCsiDriver, l logr.Logger) (requeue bool, err error) {
	namespace := client.InNamespace(objcache.Namespace)
	labels := client.MatchingLabels(GetNodeLabels(objcache))
	dsList := appsv1.DaemonSetList{}
	if err := r.DeleteAllOf(ctx, &appsv1.DaemonSet{}, namespace, labels); err != nil {
		l.Error(err, "Failed: DeleteGroup, DeleteAllOf (Head)", "namespace", objcache.Namespace, "labels", labels)
		return false, err
	}
	dsList = appsv1.DaemonSetList{}
	if err := r.List(ctx, &dsList, namespace, labels); err == nil && len(dsList.Items) > 0 {
		return true, nil
	}
	l.Info("Success: DeleteGroup", "namespace", objcache.Namespace, "labels", labels)
	return false, nil
}
