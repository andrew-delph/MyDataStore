package controllers

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"

	cachev1alpha1 "github.com/andrew-delph/my-key-store/operator/api/v1alpha1"
)

type MyKeyStoreNodePort struct{}

func ProcessNodePort(r *MyKeyStoreReconciler, ctx context.Context, req ctrl.Request, log logr.Logger, mykeystore *cachev1alpha1.MyKeyStore) (*ctrl.Result, error) {
	name := fmt.Sprintf("%s-nodeport", mykeystore.Name)
	found := &corev1.Service{}
	dep := getNodePort(mykeystore, name)
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: mykeystore.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		// Define a new NodePort

		logrus.Warnf("NODEPORT %+v", dep)

		err = ctrl.SetControllerReference(mykeystore, dep, r.Scheme)
		if err != nil {

			log.Error(err, "Failed to define new NodePort resource for MyKeyStore")

			// The following implementation will update the status
			meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{
				Type:   typeAvailableMyKeyStore,
				Status: metav1.ConditionFalse, Reason: "Reconciling",
				Message: fmt.Sprintf("Failed to create NodePort for the custom resource (%s): (%s)", name, err),
			})

			if err := r.Status().Update(ctx, mykeystore); err != nil {
				log.Error(err, "Failed to update MyKeyStore status")
				return requeueIfError(err)
			}

			return requeueIfError(err)
		}

		log.Info("Creating a new NodePort",
			"NodePort.Namespace", dep.Namespace, "NodePort.Name", dep.Name)
		if err = r.Create(ctx, dep); err != nil {
			log.Error(err, "Failed to create new NodePort",
				"NodePort.Namespace", dep.Namespace, "NodePort.Name", dep.Name)
			return requeueIfError(err)
		}

		// NodePort created successfully
		// We will requeue the reconciliation so that we can ensure the state
		// and move forward for the next operations
		return requeueAfter(time.Second*10, nil)
	} else if err != nil {
		log.Error(err, "Failed to get NodePort")
		// Let's return the error for the reconciliation be re-trigged again
		return requeueIfError(err)
	}
	err = validateNodePort(found, dep)
	if err != nil {
		logrus.Warn("failed to validate node port")
		dep := getNodePort(mykeystore, name)

		err = ctrl.SetControllerReference(mykeystore, dep, r.Scheme)
		if err != nil {

			log.Error(err, "Failed to define new NodePort resource for MyKeyStore")

			// The following implementation will update the status
			meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{
				Type:   typeAvailableMyKeyStore,
				Status: metav1.ConditionFalse, Reason: "Reconciling",
				Message: fmt.Sprintf("Failed to create NodePort for the custom resource (%s): (%s)", name, err),
			})

			if err := r.Status().Update(ctx, mykeystore); err != nil {
				log.Error(err, "Failed to update MyKeyStore status")
				return requeueIfError(err)
			}

			return requeueIfError(err)
		}

		log.Info("Updating exiting NodePort",
			"NodePort.Namespace", dep.Namespace, "NodePort.Name", dep.Name)
		if err = r.Update(ctx, dep); err != nil {
			log.Error(err, "Failed to create new NodePort",
				"NodePort.Namespace", dep.Namespace, "NodePort.Name", dep.Name)
			return requeueIfError(err)
		}
		return requeueAfter(time.Second*10, nil)
	}

	// The following implementation will update the status
	meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{
		Type:   typeAvailableMyKeyStore,
		Status: metav1.ConditionTrue, Reason: "Reconciling",
		Message: fmt.Sprintf("NodePort for custom resource (%s) created successfully", name),
	})
	return nil, nil
}

func getNodePort(mykeystore *cachev1alpha1.MyKeyStore, name string) *corev1.Service {
	selector := make(map[string]string)
	selector["app"] = mykeystore.Name
	dep := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: mykeystore.Namespace,
		},
		Spec: getNodePortSpec(mykeystore, selector),
	}
	return dep
}

func getNodePortSpec(mykeystore *cachev1alpha1.MyKeyStore, selector map[string]string) corev1.ServiceSpec {
	serviceType := corev1.ServiceTypeNodePort
	return corev1.ServiceSpec{
		Type: serviceType,
		Ports: []corev1.ServicePort{
			{Name: "http", Port: 8080, NodePort: 30000, TargetPort: intstr.FromInt(8080), Protocol: corev1.ProtocolTCP},
		},
		Selector: selector,
	}
}

func validateNodePort(found *corev1.Service, expected *corev1.Service) error {
	if !reflect.DeepEqual(found.Spec.Ports, expected.Spec.Ports) {
		return errors.New("NodePort Ports failed validation")
	}
	if !reflect.DeepEqual(found.Spec.Type, expected.Spec.Type) {
		return errors.New("NodePort Type failed validation")
	}
	if !reflect.DeepEqual(found.Spec.Selector, expected.Spec.Selector) {
		return errors.New("NodePort Selector failed validation")
	}
	return nil
}
