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
	"testing"

	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"

	"knative.dev/eventing-natss/pkg/apis/messaging/v1beta1"
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
	dispatcherDeploymentName = "test-deployment"
	dispatcherServiceName    = "test-service"
	channelServiceAddress    = "test-nc-kn-channel.test-namespace.svc.cluster.local"
)

func TestAllCases(t *testing.T) {
	// ncKey := testNS + "/" + ncName
	table := TableTest{
		{
			Name: "bad workqueue key",
			// Make sure Reconcile handles bad keys.
			Key: "too/many/parts",
		},
	}

	table.Test(t, reconciletesting.MakeFactory(func(ctx context.Context, listers *reconciletesting.Listers) controller.Reconciler {
		r := &Reconciler{
			kubeClientSet:            nil,
			systemNamespace:          "",
			dispatcherImage:          "",
			dispatcherServiceAccount: "",
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
