package controllers

import (
	"time"

	"github.com/sirupsen/logrus"
	ctrl "sigs.k8s.io/controller-runtime"
)

func requeueIfError(err error) (*ctrl.Result, error) {
	logrus.Warnf("requeueIfError err %v", err)
	return &ctrl.Result{}, err
}

func noRequeue() (*ctrl.Result, error) {
	logrus.Warnf("noRequeue")
	return &ctrl.Result{}, nil
}

func requeueAfter(interval time.Duration, err error) (*ctrl.Result, error) {
	logrus.Warnf("requeueAfter interval %v err %v", interval, err)
	return &ctrl.Result{RequeueAfter: interval}, err
}

func requeueImmediately() (*ctrl.Result, error) {
	logrus.Warnf("requeueImmediately")
	return &ctrl.Result{Requeue: true}, nil
}
