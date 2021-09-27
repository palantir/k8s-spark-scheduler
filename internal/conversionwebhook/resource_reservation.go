// Copyright (c) 2019 Palantir Technologies. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package conversionwebhook

import (
	"context"
	"io/ioutil"
	"path/filepath"

	sparkscheme "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/scheme"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-server/config"
	"github.com/palantir/witchcraft-go-server/wrouter"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/conversion"
)

const (
	webhookPath = "/convert"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	_ = sparkscheme.AddToScheme(scheme)
}

// InitializeCRDConversionWebhook initialized conversion webhook and returns webhook client
// configuration pointing to the webhook.
func InitializeCRDConversionWebhook(
	ctx context.Context,
	router wrouter.Router,
	server config.Server,
	schedulerNamespace string,
	schedulerServiceName string,
) (*v1.WebhookClientConfig, error) {
	err := addConversionWebhookRoute(ctx, router)
	if err != nil {
		return nil, err
	}

	path := filepath.Join(server.ContextPath, webhookPath)
	port := int32(server.Port)

	caBundle, err := ioutil.ReadFile(server.ClientCAFiles[0])
	if err != nil {
		return nil, werror.WrapWithContextParams(ctx, err, "failed to read CA bundle from file")
	}

	return &v1.WebhookClientConfig{
		Service: &v1.ServiceReference{
			Namespace: schedulerNamespace,
			Name:      schedulerServiceName,
			Path:      &path,
			Port:      &port,
		},
		CABundle: caBundle,
	}, nil
}

// addConversionWebhookRoute adds resource reservation crd version conversion webhook
func addConversionWebhookRoute(ctx context.Context, router wrouter.Router) error {
	svc1log.FromContext(ctx).Info("Initializing resource reservation crd conversion webhook")
	webhook := conversion.Webhook{}
	err := webhook.InjectScheme(scheme)
	if err != nil {
		return werror.WrapWithContextParams(ctx, err, "failed to inject scheme into conversion webhook")
	}
	if err := router.Post(webhookPath, &webhook); err != nil {
		return werror.WrapWithContextParams(ctx, err, "failed to add /convert route")
	}
	return nil
}
