package controller

import (
	"context"
	"testing"

	"knative.dev/eventing-natss/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-natss/pkg/client/clientset/versioned/scheme"
	"knative.dev/eventing-natss/pkg/client/injection/reconciler/messaging/v1beta1/natsschannel"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	. "knative.dev/pkg/reconciler/testing"
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
	ncKey := testNS + "/" + ncName
	table := TableTest{
		{
			Name: "bad workqueue key",
			// Make sure Reconcile handles bad keys.
			Key: "too/many/parts",
		},
	}

	table.Test(t, reconciletesting.MakeFactory(func(ctx context.Context, listers *reconciletesting.Listers) controller.Reconciler {
		r := &Reconciler{
			dispatcherNamespace:      testNS,
			dispatcherDeploymentName: dispatcherDeploymentName,
			dispatcherServiceName:    dispatcherServiceName,
			kubeClientSet:            fakekubeclient.Get(ctx),
			deploymentLister:         listers.GetDeploymentLister(),
			serviceLister:            listers.GetServiceLister(),
			endpointsLister:          listers.GetEndpointsLister(),
		}
		return natsschannel.NewReconciler(ctx, logging.FromContext(ctx),
			fakeclientset.Get(ctx), listers.GetNatssChannelLister(),
			controller.GetEventRecorder(ctx),
			r)
	}))
}
