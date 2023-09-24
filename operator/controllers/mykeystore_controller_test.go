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
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cachev1alpha1 "github.com/andrew-delph/my-key-store/operator/api/v1alpha1"
)

var _ = Describe("MyKeyStore controller", func() {
	Context("MyKeyStore controller test", func() {

		const MyKeyStoreName = "test-mykeystore"

		ctx := context.Background()

		namespace := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      MyKeyStoreName,
				Namespace: MyKeyStoreName,
			},
		}

		typeNamespaceName := types.NamespacedName{Name: MyKeyStoreName, Namespace: MyKeyStoreName}

		BeforeEach(func() {
			By("Creating the Namespace to perform the tests")
			err := k8sClient.Create(ctx, namespace)
			Expect(err).To(Not(HaveOccurred()))

			By("Setting the Image ENV VAR which stores the Operand image")
			err = os.Setenv("MYKEYSTORE_IMAGE", "example.com/image:test")
			Expect(err).To(Not(HaveOccurred()))
		})

		AfterEach(func() {
			// TODO(user): Attention if you improve this code by adding other context test you MUST
			// be aware of the current delete namespace limitations. More info: https://book.kubebuilder.io/reference/envtest.html#testing-considerations
			By("Deleting the Namespace to perform the tests")
			_ = k8sClient.Delete(ctx, namespace)

			By("Removing the Image ENV VAR which stores the Operand image")
			_ = os.Unsetenv("MYKEYSTORE_IMAGE")
		})

		It("should successfully reconcile a custom resource for MyKeyStore", func() {
			By("Creating the custom resource for the Kind MyKeyStore")
			mykeystore := &cachev1alpha1.MyKeyStore{}
			err := k8sClient.Get(ctx, typeNamespaceName, mykeystore)
			if err != nil && errors.IsNotFound(err) {
				// Let's mock our custom resource at the same way that we would
				// apply on the cluster the manifest under config/samples
				mykeystore := &cachev1alpha1.MyKeyStore{
					ObjectMeta: metav1.ObjectMeta{
						Name:      MyKeyStoreName,
						Namespace: namespace.Name,
					},
					Spec: cachev1alpha1.MyKeyStoreSpec{
						Size: 1,
					},
				}

				err = k8sClient.Create(ctx, mykeystore)
				Expect(err).To(Not(HaveOccurred()))
			}

			By("Checking if the custom resource was successfully created")
			Eventually(func() error {
				found := &cachev1alpha1.MyKeyStore{}
				return k8sClient.Get(ctx, typeNamespaceName, found)
			}, time.Minute, time.Second).Should(Succeed())

			By("Reconciling the custom resource created")
			mykeystoreReconciler := &MyKeyStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err = mykeystoreReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespaceName,
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Checking if Deployment was successfully created in the reconciliation")
			Eventually(func() error {
				found := &appsv1.Deployment{}
				return k8sClient.Get(ctx, typeNamespaceName, found)
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking the latest Status Condition added to the MyKeyStore instance")
			Eventually(func() error {
				if mykeystore.Status.Conditions != nil && len(mykeystore.Status.Conditions) != 0 {
					latestStatusCondition := mykeystore.Status.Conditions[len(mykeystore.Status.Conditions)-1]
					expectedLatestStatusCondition := metav1.Condition{Type: typeAvailableMyKeyStore,
						Status: metav1.ConditionTrue, Reason: "Reconciling",
						Message: fmt.Sprintf("Deployment for custom resource (%s) with %d replicas created successfully", mykeystore.Name, mykeystore.Spec.Size)}
					if latestStatusCondition != expectedLatestStatusCondition {
						return fmt.Errorf("The latest status condition added to the mykeystore instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())
		})
	})
})
