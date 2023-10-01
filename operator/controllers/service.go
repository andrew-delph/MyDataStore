package controllers

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/go-logr/logr"

	cachev1alpha1 "github.com/andrew-delph/my-key-store/operator/api/v1alpha1"
)

type MyKeyStoreService struct{}

func ProcessService(r *MyKeyStoreReconciler, ctx context.Context, req ctrl.Request, log logr.Logger, mykeystore *cachev1alpha1.MyKeyStore) (*ctrl.Result, error) {
	found := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: mykeystore.Name, Namespace: mykeystore.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		// Define a new Service
		dep := getService(mykeystore)

		err = ctrl.SetControllerReference(mykeystore, dep, r.Scheme)
		if err != nil {

			log.Error(err, "Failed to define new Service resource for MyKeyStore")

			// The following implementation will update the status
			meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{
				Type:   typeAvailableMyKeyStore,
				Status: metav1.ConditionFalse, Reason: "Reconciling",
				Message: fmt.Sprintf("Failed to create Service for the custom resource (%s): (%s)", mykeystore.Name, err),
			})

			if err := r.Status().Update(ctx, mykeystore); err != nil {
				log.Error(err, "Failed to update MyKeyStore status")
				return requeueIfError(err)
			}

			return requeueIfError(err)
		}

		log.Info("Creating a new Service",
			"Service.Namespace", dep.Namespace, "Service.Name", dep.Name)
		if err = r.Create(ctx, dep); err != nil {
			log.Error(err, "Failed to create new Service",
				"Service.Namespace", dep.Namespace, "Service.Name", dep.Name)
			return requeueIfError(err)
		}

		// Service created successfully
		// We will requeue the reconciliation so that we can ensure the state
		// and move forward for the next operations
		return requeueImmediately()
	} else if err != nil {
		log.Error(err, "Failed to get Service")
		// Let's return the error for the reconciliation be re-trigged again
		return requeueIfError(err)
	}

	// The following implementation will update the status
	meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{
		Type:   typeAvailableMyKeyStore,
		Status: metav1.ConditionTrue, Reason: "Reconciling",
		Message: fmt.Sprintf("Service for custom resource (%s) created successfully", mykeystore.Name),
	})
	return nil, nil
}

func getService(mykeystore *cachev1alpha1.MyKeyStore) *corev1.Service {
	selector := make(map[string]string)
	selector["app"] = mykeystore.Name
	dep := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mykeystore.Name,
			Namespace: mykeystore.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Ports: []corev1.ServicePort{
				{Name: "grpc", Port: 7070},
				{Name: "http", Port: 8080},
				{Name: "memberlist", Port: 8081},
			},
			Selector: selector,
		},
	}
	return dep
}
