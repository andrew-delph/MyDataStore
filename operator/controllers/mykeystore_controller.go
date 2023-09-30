/*
Copyright 2023.

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

package controllers

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/andrew-delph/my-key-store/rpc"

	cachev1alpha1 "github.com/andrew-delph/my-key-store/operator/api/v1alpha1"
)

const mykeystoreFinalizer = "cache.andrewdelph.com/finalizer"

// Definitions to manage status conditions
const (
	// typeAvailableMyKeyStore represents the status of the Deployment reconciliation
	typeAvailableMyKeyStore = "Available"
	typeRolloutMyKeyStore   = "Rollout"
	// typeDegradedMyKeyStore represents the status used when the custom resource is deleted and the finalizer operations are must to occur.
	typeDegradedMyKeyStore = "Degraded"
)

// MyKeyStoreReconciler reconciles a MyKeyStore object
type MyKeyStoreReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// The following markers are used to generate the rules permissions (RBAC) on config/rbac using controller-gen
// when the command <make manifests> is executed.
// To know more about markers see: https://book.kubebuilder.io/reference/markers.html

//+kubebuilder:rbac:groups=cache.andrewdelph.com,resources=mykeystores,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=cache.andrewdelph.com,resources=mykeystores/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=cache.andrewdelph.com,resources=mykeystores/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.

// It is essential for the controller's reconciliation loop to be idempotent. By following the Operator
// pattern you will create Controllers which provide a reconcile function
// responsible for synchronizing resources until the desired state is reached on the cluster.
// Breaking this recommendation goes against the design principles of controller-runtime.
// and may lead to unforeseen consequences such as resources becoming stuck and requiring manual intervention.
// For further info:
// - About Operator Pattern: https://kubernetes.io/docs/concepts/extend-kubernetes/operator/
// - About Controllers: https://kubernetes.io/docs/concepts/architecture/controller/
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *MyKeyStoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logrus.Warn("req ", req)
	log := log.FromContext(ctx)

	// Fetch the MyKeyStore instance
	// The purpose is check if the Custom Resource for the Kind MyKeyStore
	// is applied on the cluster if not we return nil to stop the reconciliation
	mykeystore := &cachev1alpha1.MyKeyStore{}
	err := r.Get(ctx, req.NamespacedName, mykeystore)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// If the custom resource is not found then, it usually means that it was deleted or not created
			// In this way, we will stop the reconciliation
			log.Info("mykeystore resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "Failed to get mykeystore")
		return ctrl.Result{}, err
	}

	// Let's just set the status as Unknown when no status are available
	if mykeystore.Status.Conditions == nil || len(mykeystore.Status.Conditions) == 0 {
		meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{Type: typeAvailableMyKeyStore, Status: metav1.ConditionUnknown, Reason: "Reconciling", Message: "Starting reconciliation"})
		if err = r.Status().Update(ctx, mykeystore); err != nil {
			log.Error(err, "Failed to update MyKeyStore status")
			return ctrl.Result{}, err
		}

		// Let's re-fetch the mykeystore Custom Resource after update the status
		// so that we have the latest state of the resource on the cluster and we will avoid
		// raise the issue "the object has been modified, please apply
		// your changes to the latest version and try again" which would re-trigger the reconciliation
		// if we try to update it again in the following operations
		if err := r.Get(ctx, req.NamespacedName, mykeystore); err != nil {
			log.Error(err, "Failed to re-fetch mykeystore")
			return ctrl.Result{}, err
		}
	}

	// Let's add a finalizer. Then, we can define some operations which should
	// occurs before the custom resource to be deleted.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers
	if !controllerutil.ContainsFinalizer(mykeystore, mykeystoreFinalizer) {
		log.Info("Adding Finalizer for MyKeyStore")
		if ok := controllerutil.AddFinalizer(mykeystore, mykeystoreFinalizer); !ok {
			log.Error(err, "Failed to add finalizer into the custom resource")
			return ctrl.Result{Requeue: true}, nil
		}

		if err = r.Update(ctx, mykeystore); err != nil {
			log.Error(err, "Failed to update custom resource to add finalizer")
			return ctrl.Result{}, err
		}
	}

	// Check if the MyKeyStore instance is marked to be deleted, which is
	// indicated by the deletion timestamp being set.
	isMyKeyStoreMarkedToBeDeleted := mykeystore.GetDeletionTimestamp() != nil
	if isMyKeyStoreMarkedToBeDeleted {

		if controllerutil.ContainsFinalizer(mykeystore, mykeystoreFinalizer) {

			log.Info("Performing Finalizer Operations for MyKeyStore before delete CR")

			// Let's add here an status "Downgrade" to define that this resource begin its process to be terminated.
			meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{
				Type:   typeDegradedMyKeyStore,
				Status: metav1.ConditionUnknown, Reason: "Finalizing",
				Message: fmt.Sprintf("Performing finalizer operations for the custom resource: %s ", mykeystore.Name),
			})

			if err := r.Status().Update(ctx, mykeystore); err != nil {
				log.Error(err, "Failed to update MyKeyStore status")
				return ctrl.Result{}, err
			}

			// Perform all operations required before remove the finalizer and allow
			// the Kubernetes API to remove the custom resource.
			r.doFinalizerOperationsForMyKeyStore(mykeystore)

			// TODO(user): If you add operations to the doFinalizerOperationsForMyKeyStore method
			// then you need to ensure that all worked fine before deleting and updating the Downgrade status
			// otherwise, you should requeue here.

			// Re-fetch the mykeystore Custom Resource before update the status
			// so that we have the latest state of the resource on the cluster and we will avoid
			// raise the issue "the object has been modified, please apply
			// your changes to the latest version and try again" which would re-trigger the reconciliation
			if err := r.Get(ctx, req.NamespacedName, mykeystore); err != nil {
				log.Error(err, "Failed to re-fetch mykeystore")
				return ctrl.Result{}, err
			}

			meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{
				Type:   typeDegradedMyKeyStore,
				Status: metav1.ConditionTrue, Reason: "Finalizing",
				Message: fmt.Sprintf("Finalizer operations for custom resource %s name were successfully accomplished", mykeystore.Name),
			})

			if err := r.Status().Update(ctx, mykeystore); err != nil {
				log.Error(err, "Failed to update MyKeyStore status")
				return ctrl.Result{}, err
			}

			log.Info("Removing Finalizer for MyKeyStore after successfully perform the operations")
			if ok := controllerutil.RemoveFinalizer(mykeystore, mykeystoreFinalizer); !ok {
				log.Error(err, "Failed to remove finalizer for MyKeyStore")
				return ctrl.Result{Requeue: true}, nil
			}

			if err := r.Update(ctx, mykeystore); err != nil {
				log.Error(err, "Failed to remove finalizer for MyKeyStore")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	result, err := ProcessService(r, ctx, req, log, mykeystore)
	if result != nil {
		return *result, err
	}

	result, err = ProcessNodePort(r, ctx, req, log, mykeystore)
	if result != nil {
		return *result, err
	}

	result, err = ProcessStatefulSet(r, ctx, req, log, mykeystore)
	if result != nil {
		return *result, err
	}

	if err := r.Status().Update(ctx, mykeystore); err != nil {
		log.Error(err, "Failed to update MyKeyStore status")
		return ctrl.Result{}, err
	}

	pods := &corev1.PodList{}
	err = r.List(ctx, pods, client.MatchingLabels{"app": "store"})
	logrus.Warnf("list= %v err = %v", len(pods.Items), err)
	for _, pod := range pods.Items {
		addr := fmt.Sprintf("%s.%s.%s", pod.Name, mykeystore.Name, pod.Namespace)
		logrus.Warnf("pod addr= %v", addr)
		conn, client, err := rpc.CreateRawRpcClient(addr, 7070)
		if err != nil {
			logrus.Errorf("Client err = %v", err)
			continue
		}
		defer conn.Close()
		req := &rpc.RpcStandardObject{}
		res, err := client.HealthCheck(ctx, req)
		if err != nil {
			logrus.Errorf("res err = %v", err)
			continue
		}
		logrus.Warnf("res %v", res.Message)
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
	// return ctrl.Result{}, nil
}

// finalizeMyKeyStore will perform the required operations before delete the CR.
func (r *MyKeyStoreReconciler) doFinalizerOperationsForMyKeyStore(cr *cachev1alpha1.MyKeyStore) {
	// TODO(user): Add the cleanup steps that the operator
	// needs to do before the CR can be deleted. Examples
	// of finalizers include performing backups and deleting
	// resources that are not owned by this CR, like a PVC.

	// Note: It is not recommended to use finalizers with the purpose of delete resources which are
	// created and managed in the reconciliation. These ones, such as the Deployment created on this reconcile,
	// are defined as depended of the custom resource. See that we use the method ctrl.SetControllerReference.
	// to set the ownerRef which means that the Deployment will be deleted by the Kubernetes API.
	// More info: https://kubernetes.io/docs/tasks/administer-cluster/use-cascading-deletion/

	// The following implementation will raise an event
	r.Recorder.Event(cr, "Warning", "Deleting",
		fmt.Sprintf("Custom Resource %s is being deleted from the namespace %s",
			cr.Name,
			cr.Namespace))
}

// labelsForMyKeyStore returns the labels for selecting the resources
// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
func labelsForMyKeyStore(name string) map[string]string {
	return map[string]string{
		"app": name,
	}
}

// imageForMyKeyStore gets the Operand image which is managed by this controller
// from the MYKEYSTORE_IMAGE environment variable defined in the config/manager/manager.yaml
func imageForMyKeyStore() (string, error) {
	// metricsv "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	// _ = &metricsv.PodMetrics{}
	return "ghcr.io/andrew-delph/my-key-store:latest", nil
	// imageEnvVar := "MYKEYSTORE_IMAGE"
	// image, found := os.LookupEnv(imageEnvVar)
	// if !found {
	// 	return "", fmt.Errorf("Unable to find %s environment variable with the image", imageEnvVar)
	// }
	// return image, nil
}

// SetupWithManager sets up the controller with the Manager.
// Note that the Deployment will be also watched in order to ensure its
// desirable state on the cluster
func (r *MyKeyStoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// corev1 "k8s.io/api/core/v1"
	// networkingv1 "k8s.io/api/networking/v1"
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.MyKeyStore{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
