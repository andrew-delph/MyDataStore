package controllers

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"

	cachev1alpha1 "github.com/andrew-delph/my-key-store/operator/api/v1alpha1"
)

type MyKeyStoreIngress struct{}

func ProcessIngress(r *MyKeyStoreReconciler, ctx context.Context, req ctrl.Request, log logr.Logger, mykeystore *cachev1alpha1.MyKeyStore) (*ctrl.Result, error) {
	logrus.Info("ProcessIngress")
	found := &v1.Ingress{}
	err := r.Get(ctx, types.NamespacedName{Name: mykeystore.Name, Namespace: mykeystore.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		// Define a new Ingress
		dep := getIngress(mykeystore)

		err = ctrl.SetControllerReference(mykeystore, dep, r.Scheme)
		if err != nil {

			log.Error(err, "Failed to define new Ingress resource for MyKeyStore")

			// The following implementation will update the status
			meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{
				Type:   typeAvailableMyKeyStore,
				Status: metav1.ConditionFalse, Reason: "Reconciling",
				Message: fmt.Sprintf("Failed to create Ingress for the custom resource (%s): (%s)", mykeystore.Name, err),
			})

			if err := r.Status().Update(ctx, mykeystore); err != nil {
				log.Error(err, "Failed to update MyKeyStore status")
				return requeueIfError(err)
			}

			return requeueIfError(err)
		}

		log.Info("Creating a new Ingress",
			"Ingress.Namespace", dep.Namespace, "Ingress.Name", dep.Name)
		if err = r.Create(ctx, dep); err != nil {
			log.Error(err, "Failed to create new Ingress",
				"Ingress.Namespace", dep.Namespace, "Ingress.Name", dep.Name)
			return requeueIfError(err)
		}

		// Ingress created successfully
		// We will requeue the reconciliation so that we can ensure the state
		// and move forward for the next operations
		return requeueAfter(time.Minute, nil)
	} else if err != nil {
		log.Error(err, "Failed to get Ingress")
		// Let's return the error for the reconciliation be re-trigged again
		return requeueIfError(err)
	}

	// The following implementation will update the status
	meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{
		Type:   typeAvailableMyKeyStore,
		Status: metav1.ConditionTrue, Reason: "Reconciling",
		Message: fmt.Sprintf("Ingress for custom resource (%s) created successfully", mykeystore.Name),
	})
	return nil, nil
}

func getIngress(mykeystore *cachev1alpha1.MyKeyStore) *v1.Ingress {
	pathType := v1.PathTypePrefix
	dep := &v1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mykeystore.Name,
			Namespace: mykeystore.Namespace,
		},
		Spec: v1.IngressSpec{
			Rules: []v1.IngressRule{
				{
					IngressRuleValue: v1.IngressRuleValue{
						HTTP: &v1.HTTPIngressRuleValue{
							Paths: []v1.HTTPIngressPath{
								{
									Path:     "/",
									PathType: &pathType,
									Backend: v1.IngressBackend{
										Service: &v1.IngressServiceBackend{
											Name: mykeystore.Name,
											Port: v1.ServiceBackendPort{
												Number: 8080,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	return dep
}
