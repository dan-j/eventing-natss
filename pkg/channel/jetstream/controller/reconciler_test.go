/*
Copyright 2019 The Knative Authors.

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

package controller

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgotesting "k8s.io/client-go/testing"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/network"

	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"

	"knative.dev/eventing-natss/pkg/apis/messaging/v1alpha1"
	"knative.dev/eventing-natss/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-natss/pkg/channel/jetstream/controller/resources"
	"knative.dev/eventing-natss/pkg/client/clientset/versioned/scheme"
	fakeclientset "knative.dev/eventing-natss/pkg/client/injection/client/fake"
	"knative.dev/eventing-natss/pkg/client/injection/reconciler/messaging/v1alpha1/natsjetstreamchannel"
	reconciletesting "knative.dev/eventing-natss/pkg/reconciler/testing"
)

func init() {
	// Add types to scheme
	_ = v1beta1.AddToScheme(scheme.Scheme)
	_ = duckv1.AddToScheme(scheme.Scheme)
}

const (
	testNS                   = "test-namespace"
	ncName                   = "test-nc"
	dispatcherImage          = "test-image"
	dispatcherDeploymentName = "test-deployment"
	dispatcherServiceName    = "test-service"
	dispatcherServiceAccount = "test-service-account"
	channelServiceAddress    = "test-nc-kn-channel.test-namespace.svc.cluster.local"
)

func TestAllCases(t *testing.T) {
	ncKey := testNS + "/" + ncName
	table := TableTest{
		{
			Name: "bad workqueue key",
			// Make sure Reconcile handles bad keys.
			Key: "too/many/parts",
		}, {
			Name: "key not found",
			// Make sure Reconcile handles good keys that don't exist.
			Key: "foo/not-found",
		}, {
			Name: "deleting",
			Key:  ncKey,
			Objects: []runtime.Object{
				reconciletesting.NewNatsJetStreamChannel(ncName, testNS,
					reconciletesting.WithNatsJetStreamInitChannelConditions,
					reconciletesting.WithNatsJetStreamChannelDeleted),
			},
			WantErr: false,
		}, {
			Name: "deployment does not exist",
			Key:  ncKey,
			Objects: []runtime.Object{
				reconciletesting.NewNatsJetStreamChannel(ncName, testNS),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
				Object: reconciletesting.NewNatsJetStreamChannel(ncName, testNS,
          reconciletesting.WithNatsJetStreamChannelServiceNotReady()
					// reconciletesting.WithNatsJetStreamInitChannelConditions,
					// reconciletesting.WithNatsJetStreamChannelDeploymentNotReady(dispatcherDeploymentNotFound, "Dispatcher Deployment does not exist"),
					// reconciletesting.WithNatsJetStreamChannelChannelServiceReady(),
					// reconciletesting.WithNatsJetStreamChannelAddress(channelServiceAddress),
					// reconciletesting.JetStreamAddressable(),
					// reconciletesting.WithNatsJetStreamChannelServiceNotReady(dispatcherServiceNotFound, "Dispatcher Service does not exist"),
					// reconciletesting.WithNatsJetStreamChannelEndpointsNotReady(dispatcherEndpointsNotFound, "Dispatcher Endpoints does not exist"),
				),
			}},
			WantCreates: []runtime.Object{
				// makeChannelService(reconciletesting.NewNatsJetStreamChannel(ncName, testNS)),
			},
		},
	}

	table.Test(t, reconciletesting.MakeFactory(func(ctx context.Context, listers *reconciletesting.Listers) controller.Reconciler {
		r := &Reconciler{
			kubeClientSet:            kubeclient.Get(ctx),
			systemNamespace:          testNS,
			dispatcherImage:          dispatcherImage,
			dispatcherServiceAccount: dispatcherServiceAccount,
			deploymentLister:         listers.GetDeploymentLister(),
			serviceLister:            listers.GetServiceLister(),
			endpointsLister:          listers.GetEndpointsLister(),
			serviceAccountLister:     listers.GetServiceAccountLister(),
			roleBindingLister:        listers.GetRoleBindingLister(),
			//TODO: Figure out controllerRef
			// controllerRef:            v1.OwnerReference{},
		}
		return natsjetstreamchannel.NewReconciler(ctx, logging.FromContext(ctx),
			fakeclientset.Get(ctx), listers.GetNatsJetstreamChannelLister(),
			controller.GetEventRecorder(ctx),
			r)
	}))
}

func makeChannelService(nc *v1alpha1.NatsJetStreamChannel) *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNS,
			Name:      fmt.Sprintf("%s-kn-channel", ncName),
			Labels: map[string]string{
				resources.MessagingRoleLabel: resources.MessagingRole,
			},
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(nc),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: network.GetServiceHostname(dispatcherServiceName, testNS),
		},
	}
}
