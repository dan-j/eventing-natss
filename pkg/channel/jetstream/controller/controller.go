/*
Copyright 2021 The Knative Authors

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
	"knative.dev/eventing-natss/pkg/channel/jetstream"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/reconciler"
	"time"

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/pointer"

	kubeclient "knative.dev/pkg/client/injection/kube/client"
	deploymentinformer "knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment"
	endpointsinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/endpoints"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	serviceinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/service"
	serviceaccountinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/serviceaccount"
	rolebindinginformer "knative.dev/pkg/client/injection/kube/informers/rbac/v1/rolebinding"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/system"

	"knative.dev/eventing-natss/pkg/client/injection/informers/messaging/v1alpha1/natsjetstreamchannel"
	jsmreconciler "knative.dev/eventing-natss/pkg/client/injection/reconciler/messaging/v1alpha1/natsjetstreamchannel"
)

const (
	channelLabelKey          = "messaging.knative.dev/channel"
	channelLabelValue        = "nats-jetstream-channel"
	roleLabelKey             = "messaging.knative.dev/role"
	dispatcherRoleLabelValue = "dispatcher"
	controllerRoleLabelValue = "controller"
	interval                 = 100 * time.Millisecond
	timeout                  = 5 * time.Minute
)

// NewController initializes the controller and is called by the generated code.
// Registers event handlers to enqueue events.
func NewController(ctx context.Context, _ configmap.Watcher) *controller.Impl {
	logger := logging.FromContext(ctx)

	env := &envConfig{}
	if err := envconfig.Process("", env); err != nil {
		logger.Panicf("unable to process required environment variables: %v", err)
	}

	// get the ref of the controller deployment
	ownerRef, err := getControllerOwnerRef(ctx)
	if err != nil {
		logger.Panic("Could not determine the proper owner reference for the dispatcher deployment.", zap.Error(err))
	}

	jsmInformer := natsjetstreamchannel.Get(ctx)
	deploymentInformer := deploymentinformer.Get(ctx)
	serviceInformer := serviceinformer.Get(ctx)
	endpointsInformer := endpointsinformer.Get(ctx)
	serviceAccountInformer := serviceaccountinformer.Get(ctx)
	roleBindingInformer := rolebindinginformer.Get(ctx)
	podInformer := podinformer.Get(ctx)

	kubeClient := kubeclient.Get(ctx)

	r := &Reconciler{
		kubeClientSet:            kubeClient,
		systemNamespace:          system.Namespace(),
		dispatcherImage:          env.Image,
		dispatcherServiceAccount: env.DispatcherServiceAccount,
		deploymentLister:         deploymentInformer.Lister(),
		serviceLister:            serviceInformer.Lister(),
		endpointsLister:          endpointsInformer.Lister(),
		serviceAccountLister:     serviceAccountInformer.Lister(),
		roleBindingLister:        roleBindingInformer.Lister(),
		controllerRef:            *ownerRef,
	}

	impl := jsmreconciler.NewImpl(ctx, r)

	logger.Info("Setting up event handlers")
	jsmInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	grCh := func(_ interface{}) {
		impl.GlobalResync(jsmInformer.Informer())
	}
	filterFunc := controller.FilterWithName(jetstream.DispatcherName)

	// Set up watches for dispatcher resources we care about, since any changes to these
	// resources will affect our Channels. So, set up a watch here, that will cause
	// a global Resync for all the channels to take stock of their health when these change.
	deploymentInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFunc,
		Handler:    controller.HandleAll(grCh),
	})
	serviceInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFunc,
		Handler:    controller.HandleAll(grCh),
	})
	endpointsInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFunc,
		Handler:    controller.HandleAll(grCh),
	})
	serviceAccountInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFunc,
		Handler:    controller.HandleAll(grCh),
	})
	roleBindingInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFunc,
		Handler:    controller.HandleAll(grCh),
	})
	podInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: reconciler.ChainFilterFuncs(
			reconciler.LabelFilterFunc(channelLabelKey, channelLabelValue, false),
			reconciler.LabelFilterFunc(roleLabelKey, dispatcherRoleLabelValue, false),
		),
		Handler: controller.HandleAll(grCh),
	})

	return impl
}

// getControllerOwnerRef builds a *metav1.OwnerReference of the deployment for this controller instance. This is then
// used by the Reconciler for adding owner references to the dispatcher deployments.
func getControllerOwnerRef(ctx context.Context) (*metav1.OwnerReference, error) {
	logger := logging.FromContext(ctx)
	ctrlDeploymentLabels := labels.Set{
		channelLabelKey: channelLabelValue,
		roleLabelKey:    controllerRoleLabelValue,
	}

	ownerRef := metav1.OwnerReference{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Controller: pointer.BoolPtr(true),
	}
	err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		k8sClient := kubeclient.Get(ctx)
		deploymentList, err := k8sClient.AppsV1().Deployments(system.Namespace()).List(ctx, metav1.ListOptions{LabelSelector: ctrlDeploymentLabels.String()})
		if err != nil {
			return true, fmt.Errorf("error listing NatsJetStreamChannel controller deployment labels %w", err)
		} else if len(deploymentList.Items) == 0 {
			// Simple exponential backoff
			logger.Debugw("found zero NatsJetStreamChannel controller deployment matching labels. Retrying.", zap.String("namespace", system.Namespace()), zap.Any("selectors", ctrlDeploymentLabels.AsSelector()))
			return false, nil
		} else if len(deploymentList.Items) > 1 {
			return true, fmt.Errorf("found an unexpected number of NatsJetStreamChannel controller deployment matching labels. Got: %d, Want: 1", len(deploymentList.Items))
		}
		d := deploymentList.Items[0]
		ownerRef.Name = d.Name
		ownerRef.UID = d.UID
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to determine the deployment of the NatsJetStreamChannel controller based on labels. %w", err)
	}
	return &ownerRef, nil
}
