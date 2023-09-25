package controllers

import (
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

func requeueIfError(err error) (ctrl.Result, error) {
	return ctrl.Result{}, err
}

func noRequeue() (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

func requeueAfter(interval time.Duration, err error) (ctrl.Result, error) {
	return ctrl.Result{RequeueAfter: interval}, err
}

func requeueImmediately() (ctrl.Result, error) {
	return ctrl.Result{Requeue: true}, nil
}
