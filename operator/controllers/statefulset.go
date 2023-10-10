package controllers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/andrew-delph/my-key-store/rpc"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"

	cachev1alpha1 "github.com/andrew-delph/my-key-store/operator/api/v1alpha1"
)

type MyKeyStoreStatefulSet struct{}

func ProcessStatefulSet(r *MyKeyStoreReconciler, ctx context.Context, req ctrl.Request, log logr.Logger, mykeystore *cachev1alpha1.MyKeyStore) (*ctrl.Result, error) {
	found := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: mykeystore.Name, Namespace: mykeystore.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		// Define a new deployment
		dep := getStatefulSet(mykeystore)

		err = ctrl.SetControllerReference(mykeystore, dep, r.Scheme)
		if err != nil {

			log.Error(err, "Failed to define new Deployment resource for MyKeyStore")

			// The following implementation will update the status
			meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{
				Type:   typeAvailableMyKeyStore,
				Status: metav1.ConditionFalse, Reason: "Reconciling",
				Message: fmt.Sprintf("Failed to create Deployment for the custom resource (%s): (%s)", mykeystore.Name, err),
			})

			if err := r.Status().Update(ctx, mykeystore); err != nil {
				log.Error(err, "Failed to update MyKeyStore status")
				return requeueIfError(err)
			}

			return requeueIfError(err)
		}

		log.Info("Creating a new Deployment",
			"Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
		if err = r.Create(ctx, dep); err != nil {
			log.Error(err, "Failed to create new Deployment",
				"Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
			return requeueIfError(err)
		}

		// Deployment created successfully
		// We will requeue the reconciliation so that we can ensure the state
		// and move forward for the next operations
		return requeueAfter(time.Minute, nil)
	} else if err != nil {
		log.Error(err, "Failed to get Deployment")
		// Let's return the error for the reconciliation be re-trigged again
		return requeueIfError(err)
	}

	// The CRD API is defining that the MyKeyStore type, have a MyKeyStoreSpec.Size field
	// to set the quantity of Deployment instances is the desired state on the cluster.
	// Therefore, the following code will ensure the Deployment size is the same as defined
	// via the Size spec of the Custom Resource which we are reconciling.

	rolloutStatus := meta.FindStatusCondition(mykeystore.Status.Conditions, typeRolloutMyKeyStore)
	if rolloutStatus != nil && rolloutStatus.Status == metav1.ConditionTrue {
		incrementReady := found.Status.ReadyReplicas == found.Status.Replicas
		updatedReplicas := found.Status.UpdatedReplicas
		finalUpdate := found.Status.UpdatedReplicas == found.Status.Replicas
		if time.Since(rolloutStatus.LastTransitionTime.Time) < time.Second*10 {
			return requeueAfter(time.Second*5, nil)
		}

		// logrus.Warnf("IN ROLLOUT %v %v incrementReady = %v updatedReplicas = %v finalUpdate = %v", utils.Min(found.Status.ReadyReplicas, updatedReplicas), found.Status.Replicas, incrementReady, updatedReplicas, finalUpdate)
		// logrus.Warnf("time %v", time.Since(rolloutStatus.LastTransitionTime.Time))
		if !finalUpdate && incrementReady {
			nextPartition := found.Status.Replicas - updatedReplicas - 1
			logrus.Warnf("nextPartition ready! nextPartition %d", nextPartition)
			err = manualRollout(r, ctx, req, log, mykeystore, found, nextPartition)
			if err != nil {
				logrus.Warnf("manualRollout err = %v", err)
			}

		} else if finalUpdate && incrementReady {
			logrus.Warnf("FINAL UPDATE! time: %v", time.Since(rolloutStatus.LastTransitionTime.Time))
			err = setRolloutStatus(r, ctx, log, mykeystore, found, false, "rollout finished!")
			if err != nil {
				logrus.Errorf("setRolloutStatus err = %v", err)
			}
		}
		return requeueAfter(time.Second*5, nil)

	}

	image := mykeystore.Spec.Image
	if found.Spec.Template.Spec.Containers[0].Image != image {
		logrus.Warn("WRONG IMAGE image=", image)
		found.Status.UpdateRevision = image
		found.Spec.Template.Spec.Containers[0].Image = image
		err = manualRollout(r, ctx, req, log, mykeystore, found, found.Status.Replicas)
		if err != nil {
			logrus.Errorf("new size: %v", err)
		}
		return requeueAfter(time.Second*5, nil)
	}

	currSize := *found.Spec.Replicas
	reqSize := mykeystore.Spec.Size
	sizeDiff := reqSize - currSize
	if sizeDiff > 0 {
		members := generateMembers(mykeystore, currSize)
		temp_members := generateMembers(mykeystore, currSize+1)
		err := notifyMembers(r, ctx, req, log, mykeystore, members, temp_members)
		if err != nil {
			// logrus.Error(err)
			return requeueAfter(time.Second*5, nil)
		}

		err = waitForPodsHealthy(r, ctx, req, log, mykeystore)
		if err != nil {
			logrus.Error(err)
			return requeueAfter(time.Second*5, nil)
		}

		newSize := *found.Spec.Replicas + 1
		logrus.Warnf("ALL PARTITIONS HEALTHY. READY TO SCALE UP. newSize %d", newSize)

		found.Spec.Replicas = &newSize
		err = manualRollout(r, ctx, req, log, mykeystore, found, found.Status.CurrentReplicas)
		if err != nil {
			logrus.Errorf("new size: err %v", err)
		}
		return requeueImmediately()

	} else if sizeDiff < 0 {
		members := generateMembers(mykeystore, currSize)
		temp_members := generateMembers(mykeystore, currSize-1)
		err := notifyMembers(r, ctx, req, log, mykeystore, members, temp_members)
		if err != nil {
			logrus.Error(err)
			return requeueAfter(time.Second*5, nil)
		}

		err = waitForPodsHealthy(r, ctx, req, log, mykeystore)
		if err != nil {
			logrus.Error(err)
			return requeueAfter(time.Second*5, nil)
		}

		newSize := *found.Spec.Replicas - 1
		logrus.Warnf("ALL PARTITIONS HEALTHY. READY TO SCALE DOWN. newSize %d", newSize)
		found.Spec.Replicas = &newSize
		err = manualRollout(r, ctx, req, log, mykeystore, found, found.Status.CurrentReplicas)
		if err != nil {
			logrus.Errorf("new size: err %v", err)
		}
	}
	members := generateMembers(mykeystore, currSize)
	err = notifyMembers(r, ctx, req, log, mykeystore, members, members)
	if err != nil {
		return requeueAfter(time.Second*5, nil)
	}

	// health check pods

	meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{
		Type:   typeAvailableMyKeyStore,
		Status: metav1.ConditionTrue, Reason: "Reconciling",
		Message: fmt.Sprintf("Deployment for custom resource (%s) with %d replicas created successfully", mykeystore.Name, reqSize),
	})

	if err := r.Status().Update(ctx, mykeystore); err != nil {
		log.Error(err, "Failed to update MyKeyStore status")
		return requeueIfError(err)
	}

	return nil, nil
}

func setRolloutStatus(r *MyKeyStoreReconciler, ctx context.Context, log logr.Logger, mykeystore *cachev1alpha1.MyKeyStore, found *appsv1.StatefulSet, status bool, message string) error {
	var conditionStatus metav1.ConditionStatus
	if status {
		conditionStatus = metav1.ConditionTrue
	} else {
		conditionStatus = metav1.ConditionFalse
	}
	meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{
		Type:   typeRolloutMyKeyStore,
		Status: conditionStatus, Reason: "RollingUpdate",
		Message: message,
	})

	if err := r.Status().Update(ctx, mykeystore); err != nil {
		log.Error(err, "Failed to update MyKeyStore status")
		return err
	}
	return nil
}

func manualRollout(r *MyKeyStoreReconciler, ctx context.Context, req ctrl.Request, log logr.Logger, mykeystore *cachev1alpha1.MyKeyStore, found *appsv1.StatefulSet, ordinal int32) error {
	var err error
	updateId := fmt.Sprintf("%s-%d-%d", found.Status.CurrentRevision, ordinal, found.Status.Replicas)
	logrus.Warnf("updateId %v", updateId)
	err = verifyEpochUpdate(r, ctx, req, log, mykeystore, updateId)
	if err != nil {
		return err
	}
	err = waitForPodsHealthy(r, ctx, req, log, mykeystore)
	if err != nil {
		return err
	}
	found.Spec.UpdateStrategy.RollingUpdate.Partition = &ordinal
	if err = r.Update(ctx, found); err != nil {
		log.Error(err, "Failed to update Deployment",
			"Deployment.Namespace", found.Namespace, "Deployment.Name", found.Name)

		// Re-fetch the mykeystore Custom Resource before update the status
		// so that we have the latest state of the resource on the cluster and we will avoid
		// raise the issue "the object has been modified, please apply
		// your changes to the latest version and try again" which would re-trigger the reconciliation
		if err := r.Get(ctx, req.NamespacedName, mykeystore); err != nil {
			log.Error(err, "Failed to re-fetch mykeystore")
			return err
		}

		// The following implementation will update the status
		meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{
			Type:   typeAvailableMyKeyStore,
			Status: metav1.ConditionFalse, Reason: "Rollout",
			Message: "rollout message is not implemented.",
		})

		if err := r.Status().Update(ctx, mykeystore); err != nil {
			log.Error(err, "Failed to update MyKeyStore status")
			return err
		}

		return err
	}

	meta.SetStatusCondition(&mykeystore.Status.Conditions, metav1.Condition{
		Type:   typeRolloutMyKeyStore,
		Status: metav1.ConditionTrue, Reason: "RollingUpdate",
		Message: "rolling out message",
	})

	if err := r.Status().Update(ctx, mykeystore); err != nil {
		log.Error(err, "Failed to update MyKeyStore status")
		return err
	}
	return nil
}

func waitForPodsHealthy(r *MyKeyStoreReconciler, ctx context.Context, req ctrl.Request, log logr.Logger, mykeystore *cachev1alpha1.MyKeyStore) error {
	pods := &corev1.PodList{}
	err := r.List(ctx, pods, client.MatchingLabels{"app": mykeystore.Name})
	if err != nil {
		return err
	}
	errorCount := 0
	// logrus.Warnf("list= %v err = %v", len(pods.Items), err)
	for _, pod := range pods.Items {
		addr := fmt.Sprintf("%s.%s.%s", pod.Name, mykeystore.Name, pod.Namespace)
		conn, client, err := rpc.CreateRawRpcClient(addr, 7070)
		if err != nil {
			logrus.Errorf("Client %s err = %v", addr, err)
			errorCount++
		}
		defer conn.Close()
		req := &rpc.RpcStandardObject{}
		res, err := client.PartitionsHealthCheck(ctx, req)
		if err != nil {
			err = rpc.ExtractError(err)
			logrus.Errorf("Client %s err = %v", pod.Name, err)
			errorCount++
		} else if res.Error {
			err = errors.New(res.Message)
			logrus.Errorf("Client %s err msg = %v", pod.Name, err)
			errorCount++
		}
		// logrus.Warnf("Client %s res= %v", pod.Name, res.Message)
	}
	if errorCount > 0 {
		return fmt.Errorf("waitForPodsHealthy had %d errors", errorCount)
	}
	logrus.Warnf("all pods health. # = %v", len(pods.Items))
	return nil
}

func verifyEpochUpdate(r *MyKeyStoreReconciler, ctx context.Context, req ctrl.Request, log logr.Logger, mykeystore *cachev1alpha1.MyKeyStore, updateId string) error {
	pods := &corev1.PodList{}
	err := r.List(ctx, pods, client.MatchingLabels{"app": mykeystore.Name})
	if err != nil {
		return err
	}
	errorCount := 0
	// logrus.Warnf("list= %v err = %v", len(pods.Items), err)
	for _, pod := range pods.Items {
		addr := fmt.Sprintf("%s.%s.%s", pod.Name, mykeystore.Name, pod.Namespace)
		conn, client, err := rpc.CreateRawRpcClient(addr, 7070)
		if err != nil {
			logrus.Errorf("verifyEpochUpdate Client %s err = %v", addr, err)
			errorCount++
		}
		defer conn.Close()
		req := &rpc.RpcStandardObject{Message: updateId}
		res, err := client.UpdateEpoch(ctx, req)
		if err != nil {
			err = rpc.ExtractError(err)
			logrus.Errorf("verifyEpochUpdate Client %s err = %v", pod.Name, err)
			errorCount++
		} else if res.Error {
			err = errors.New(res.Message)
			logrus.Errorf("verifyEpochUpdate Client %s err msg = %v", pod.Name, err)
			errorCount++
		}
		// logrus.Warnf("Client %s res= %v", pod.Name, res.Message)
	}
	if errorCount > 0 {
		return fmt.Errorf("verifyEpochUpdate had %d errors", errorCount)
	}
	logrus.Warnf("all pods verifyEpochUpdate. # = %v", len(pods.Items))
	return nil
}

func generateMembers(mykeystore *cachev1alpha1.MyKeyStore, size int32) []string {
	var members []string
	for i := int32(0); i < size; i++ {
		members = append(members, fmt.Sprintf("%s-%d", mykeystore.Name, i))
	}
	return members
}

func notifyMembers(r *MyKeyStoreReconciler, ctx context.Context, req ctrl.Request, log logr.Logger, mykeystore *cachev1alpha1.MyKeyStore, members, temp_members []string) error {
	// logrus.Warnf("notifyMembers members = %v temp_members = %v", members, temp_members)
	pods := &corev1.PodList{}
	err := r.List(ctx, pods, client.MatchingLabels{"app": mykeystore.Name})
	if err != nil {
		return err
	}
	errorCount := 0
	// logrus.Warnf("AddTempNode list= %v err = %v", len(pods.Items), err)
	for _, pod := range pods.Items {
		addr := fmt.Sprintf("%s.%s.%s", pod.Name, mykeystore.Name, pod.Namespace)
		conn, client, err := rpc.CreateRawRpcClient(addr, 7070)
		if err != nil {
			// logrus.Errorf("Client %s err = %v", addr, err)
			errorCount++
			continue
		}
		defer conn.Close()
		req := &rpc.RpcMembers{}
		req.Members = members
		req.TempMembers = temp_members
		res, err := client.UpdateMembers(ctx, req)
		if err != nil {
			// logrus.Errorf("ResetTempNode Client %s err = %v", pod.Name, err)
			errorCount++
			continue
		} else if res.Error {
			err = errors.New(res.Message)
			// logrus.Errorf("ResetTempNode Client %s err msg = %v", pod.Name, err)
			errorCount++
			continue
		}
		// logrus.Warnf("ResetTempNode Client %s res= %v", pod.Name, res.Message)
	}

	if errorCount > 0 {
		return fmt.Errorf("notifyMembers had %d errors", errorCount)
	}
	logrus.Warnf("all pods notifyMembers. # = %v", len(pods.Items))
	return nil
}

func getStatefulSet(mykeystore *cachev1alpha1.MyKeyStore) *appsv1.StatefulSet {
	// found.Spec.UpdateStrategy.RollingUpdate.Partition
	ls := labelsForMyKeyStore(mykeystore.Name)
	replicas := mykeystore.Spec.Size

	// Get the Operand image
	image, err := imageForMyKeyStore()
	if err != nil {
		logrus.Panic(err)
	}

	pvQuantity, err := resource.ParseQuantity("10Gi")
	if err != nil {
		logrus.Panic(err)
	}

	dep := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mykeystore.Name,
			Namespace: mykeystore.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: mykeystore.Name,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{},
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
				},
				Spec: corev1.PodSpec{
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: &[]bool{false}[0],
						// IMPORTANT: seccomProfile was introduced with Kubernetes 1.19
						// If you are looking for to produce solutions to be supported
						// on lower versions you must remove this option.
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Image:           image,
						Name:            "mykeystore",
						ImagePullPolicy: corev1.PullIfNotPresent,
						// Ensure restrictive context for the container
						// More info: https://kubernetes.io/docs/concepts/security/pod-security-standards/#restricted
						Env: []corev1.EnvVar{
							{
								Name:  "OPERATOR",
								Value: "true",
							},
						},
						SecurityContext: &corev1.SecurityContext{
							RunAsNonRoot:             &[]bool{false}[0],
							AllowPrivilegeEscalation: &[]bool{false}[0],
							Capabilities: &corev1.Capabilities{
								Drop: []corev1.Capability{
									"ALL",
								},
							},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/ready",
									Port: intstr.FromInt(8080),
								},
							},
							InitialDelaySeconds: 5,
							PeriodSeconds:       5,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/health",
									Port: intstr.FromInt(8080),
								},
							},
							InitialDelaySeconds: 30,
							PeriodSeconds:       5,
							FailureThreshold:    2,
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "store-data-volume", MountPath: "/data/"},
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "store-data-volume"},
					Spec:       corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{"storage": pvQuantity}}},
				},
			},
		},
	}

	return dep
}
