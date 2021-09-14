package conversionwebhook

import (
	"context"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-server/config"
	"github.com/palantir/witchcraft-go-server/wrouter"

	"k8s.io/apimachinery/pkg/runtime"
	"path/filepath"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"sigs.k8s.io/controller-runtime/pkg/webhook/conversion"

	sparkscheme "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/scheme"
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
) (*apiextensionsv1.WebhookClientConfig, error) {
	err := addConversionWebhookRoute(ctx, router)
	if err != nil {
		return nil, err
	}

	path := filepath.Join(server.ContextPath, webhookPath)
	port := int32(server.Port)
	return &apiextensionsv1.WebhookClientConfig{
		Service: &apiextensionsv1.ServiceReference{
			Namespace: "spark",
			Name:      "spark-scheduler",
			Path:      &path,
			Port:      &port,
		},
	}, nil
}

// addConversionWebhookRoute adds demand crd version conversion webhook
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
