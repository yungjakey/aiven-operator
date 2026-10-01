// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/

package controllers

import (
	"context"
	"fmt"
	"strconv"

	avngen "github.com/aiven/go-client-codegen"
	"github.com/aiven/go-client-codegen/handler/service"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aiven/aiven-operator/api/v1alpha1"
)

// KafkaReconciler reconciles a Kafka object
type KafkaReconciler struct {
	Controller
}

func newKafkaReconciler(c Controller) reconcilerType {
	return &KafkaReconciler{Controller: c}
}

//+kubebuilder:rbac:groups=aiven.io,resources=kafkas,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiven.io,resources=kafkas/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiven.io,resources=kafkas/finalizers,verbs=get;create;update

func (r *KafkaReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return r.reconcileInstance(ctx, req, newGenericServiceHandler(r.Client, r.Recorder, newKafkaAdapter, r.Log), &v1alpha1.Kafka{})
}

func (r *KafkaReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Kafka{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}

func newKafkaAdapter(object client.Object) (serviceAdapter, error) {
	kafka, ok := object.(*v1alpha1.Kafka)
	if !ok {
		return nil, fmt.Errorf("object is not of type v1alpha1.Kafka")
	}
	return &kafkaAdapter{Kafka: kafka}, nil
}

// kafkaAdapter handles an Aiven Kafka service
type kafkaAdapter struct {
	*v1alpha1.Kafka
}

func (a *kafkaAdapter) getObjectMeta() *metav1.ObjectMeta {
	return &a.ObjectMeta
}

func (a *kafkaAdapter) getServiceStatus() *v1alpha1.ServiceStatus {
	return &a.Status
}

func (a *kafkaAdapter) getServiceCommonSpec() *v1alpha1.ServiceCommonSpec {
	return &a.Spec.ServiceCommonSpec
}

func (a *kafkaAdapter) getUserConfig() any {
	return a.Spec.UserConfig
}

func (a *kafkaAdapter) newSecret(s *service.ServiceGetOut) *corev1.Secret {
	var userName, password string
	if len(s.Users) > 0 {
		userName = s.Users[0].Username
		password = s.Users[0].Password
	}

	prefix := getSecretPrefix(a)
	stringData := map[string]string{
		prefix + "HOST":     s.ServiceUriParams["host"],
		prefix + "PORT":     s.ServiceUriParams["port"],
		prefix + "PASSWORD": password,
		prefix + "USERNAME": userName,
		// todo: remove in future releases
		"HOST":     s.ServiceUriParams["host"],
		"PORT":     s.ServiceUriParams["port"],
		"PASSWORD": password,
		"USERNAME": userName,
	}

	// Aiven omits these when the corresponding feature is not enabled on the service:
	// the URIs when kafka_rest or schema_registry is off, the certificates when the
	// service issues none. The whole object is absent for a service with no connection
	// info at all. Such keys are left out of the secret rather than dereferenced.
	if info := s.ConnectionInfo; info != nil {
		addOptionalDetail(stringData, prefix+"ACCESS_CERT", info.KafkaAccessCert)
		addOptionalDetail(stringData, prefix+"ACCESS_KEY", info.KafkaAccessKey)
		addOptionalDetail(stringData, prefix+"REST_URI", info.KafkaRestUri)
		addOptionalDetail(stringData, prefix+"SCHEMA_REGISTRY_URI", info.SchemaRegistryUri)

		// todo: remove in future releases
		addOptionalDetail(stringData, "ACCESS_CERT", info.KafkaAccessCert)
		addOptionalDetail(stringData, "ACCESS_KEY", info.KafkaAccessKey)
	}

	addKafkaEndpointDetails(stringData, s.Components, prefix)

	for _, c := range s.Components {
		switch c.Component {
		case "kafka_connect":
			stringData[prefix+"CONNECT_HOST"] = c.Host
			stringData[prefix+"CONNECT_PORT"] = strconv.Itoa(c.Port)
		case "kafka_rest":
			stringData[prefix+"REST_HOST"] = c.Host
			stringData[prefix+"REST_PORT"] = strconv.Itoa(c.Port)
		}
	}

	return newSecret(a, stringData, false)
}

func (a *kafkaAdapter) getServiceType() serviceType {
	return serviceTypeKafka
}

func (a *kafkaAdapter) getDiskSpace() string {
	return a.Spec.DiskSpace
}

func (a *kafkaAdapter) performUpgradeTaskIfNeeded(_ context.Context, _ avngen.Client, _ *service.ServiceGetOut) error {
	return nil
}

func (a *kafkaAdapter) createOrUpdateServiceSpecific(_ context.Context, _ avngen.Client, _ *service.ServiceGetOut) error {
	return nil
}

// addOptionalDetail sets key only when Aiven returned a value for it.
func addOptionalDetail(details SecretDetails, key string, value *string) {
	if value != nil {
		details[key] = *value
	}
}

func addKafkaEndpointDetails(details SecretDetails, components []service.ComponentOut, prefix string) {
	for _, c := range components {
		switch c.Component {
		case "kafka":
			if c.KafkaAuthenticationMethod == service.KafkaAuthenticationMethodTypeSasl {
				details[prefix+"SASL_HOST"] = c.Host
				details[prefix+"SASL_PORT"] = strconv.Itoa(c.Port)
			}
		case "schema_registry":
			details[prefix+"SCHEMA_REGISTRY_HOST"] = c.Host
			details[prefix+"SCHEMA_REGISTRY_PORT"] = strconv.Itoa(c.Port)
		}
	}
}

// refreshKafkaEndpointDetails replaces optional Kafka endpoints in existing Secret data.
func refreshKafkaEndpointDetails(data map[string][]byte, components []service.ComponentOut, prefix string) {
	for _, key := range []string{
		prefix + "SASL_HOST",
		prefix + "SASL_PORT",
		prefix + "SCHEMA_REGISTRY_HOST",
		prefix + "SCHEMA_REGISTRY_PORT",
	} {
		delete(data, key)
	}
	details := make(SecretDetails)
	addKafkaEndpointDetails(details, components, prefix)
	for key, value := range details {
		data[key] = []byte(value)
	}
}
