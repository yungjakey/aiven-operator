package controllers

import (
	"testing"

	"github.com/aiven/go-client-codegen/handler/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/aiven/aiven-operator/api/v1alpha1"
)

func TestKafkaAdapterNewSecret(t *testing.T) {
	t.Parallel()

	newKafka := func() *v1alpha1.Kafka {
		return &v1alpha1.Kafka{
			TypeMeta:   metav1.TypeMeta{Kind: "Kafka", APIVersion: v1alpha1.GroupVersion.String()},
			ObjectMeta: metav1.ObjectMeta{Name: "my-kafka", Namespace: "default"},
		}
	}

	ptr := func(s string) *string { return &s }

	baseService := func() *service.ServiceGetOut {
		return &service.ServiceGetOut{
			ServiceUriParams: map[string]string{"host": "kafka.example.com", "port": "12345"},
			Users:            []service.UserOut{{Username: "avnadmin", Password: "hunter2"}},
		}
	}

	t.Run("publishes every key when the service reports all connection details", func(t *testing.T) {
		t.Parallel()

		s := baseService()
		s.ConnectionInfo = &service.ConnectionInfoOut{
			KafkaAccessCert:   ptr("cert"),
			KafkaAccessKey:    ptr("key"),
			KafkaRestUri:      ptr("https://rest.example.com"),
			SchemaRegistryUri: ptr("https://registry.example.com"),
		}

		secret := (&kafkaAdapter{Kafka: newKafka()}).newSecret(s)
		require.NotNil(t, secret)

		assert.Equal(t, "kafka.example.com", secret.StringData["KAFKA_HOST"])
		assert.Equal(t, "12345", secret.StringData["KAFKA_PORT"])
		assert.Equal(t, "avnadmin", secret.StringData["KAFKA_USERNAME"])
		assert.Equal(t, "hunter2", secret.StringData["KAFKA_PASSWORD"])
		assert.Equal(t, "cert", secret.StringData["KAFKA_ACCESS_CERT"])
		assert.Equal(t, "key", secret.StringData["KAFKA_ACCESS_KEY"])
		assert.Equal(t, "https://rest.example.com", secret.StringData["KAFKA_REST_URI"])
		assert.Equal(t, "https://registry.example.com", secret.StringData["KAFKA_SCHEMA_REGISTRY_URI"])

		// Legacy unprefixed keys.
		assert.Equal(t, "cert", secret.StringData["ACCESS_CERT"])
		assert.Equal(t, "key", secret.StringData["ACCESS_KEY"])
	})

	// kafka_rest and schema_registry are disabled by default, and Aiven omits
	// their URIs entirely for such a service.
	t.Run("omits the URIs when Kafka REST and Schema Registry are disabled", func(t *testing.T) {
		t.Parallel()

		s := baseService()
		s.ConnectionInfo = &service.ConnectionInfoOut{
			KafkaAccessCert: ptr("cert"),
			KafkaAccessKey:  ptr("key"),
		}

		secret := (&kafkaAdapter{Kafka: newKafka()}).newSecret(s)
		require.NotNil(t, secret)

		assert.Equal(t, "cert", secret.StringData["KAFKA_ACCESS_CERT"])
		assert.Equal(t, "key", secret.StringData["KAFKA_ACCESS_KEY"])
		assert.NotContains(t, secret.StringData, "KAFKA_REST_URI")
		assert.NotContains(t, secret.StringData, "KAFKA_SCHEMA_REGISTRY_URI")

		// The mandatory keys are still published.
		assert.Equal(t, "kafka.example.com", secret.StringData["KAFKA_HOST"])
		assert.Equal(t, "avnadmin", secret.StringData["KAFKA_USERNAME"])
	})

	t.Run("omits the certificates when the service issues none", func(t *testing.T) {
		t.Parallel()

		s := baseService()
		s.ConnectionInfo = &service.ConnectionInfoOut{
			KafkaRestUri: ptr("https://rest.example.com"),
		}

		secret := (&kafkaAdapter{Kafka: newKafka()}).newSecret(s)
		require.NotNil(t, secret)

		assert.Equal(t, "https://rest.example.com", secret.StringData["KAFKA_REST_URI"])
		assert.NotContains(t, secret.StringData, "KAFKA_ACCESS_CERT")
		assert.NotContains(t, secret.StringData, "KAFKA_ACCESS_KEY")
		assert.NotContains(t, secret.StringData, "ACCESS_CERT")
		assert.NotContains(t, secret.StringData, "ACCESS_KEY")
	})

	t.Run("publishes the mandatory keys when connection info is absent", func(t *testing.T) {
		t.Parallel()

		secret := (&kafkaAdapter{Kafka: newKafka()}).newSecret(baseService())
		require.NotNil(t, secret)

		assert.Equal(t, "kafka.example.com", secret.StringData["KAFKA_HOST"])
		assert.Equal(t, "12345", secret.StringData["KAFKA_PORT"])
		assert.NotContains(t, secret.StringData, "KAFKA_ACCESS_CERT")
		assert.NotContains(t, secret.StringData, "KAFKA_REST_URI")
	})

	t.Run("adds component endpoints", func(t *testing.T) {
		t.Parallel()

		s := baseService()
		s.Components = []service.ComponentOut{
			{Component: "kafka", KafkaAuthenticationMethod: service.KafkaAuthenticationMethodTypeSasl, Host: "sasl.example.com", Port: 1},
			{Component: "schema_registry", Host: "registry.example.com", Port: 2},
			{Component: "kafka_connect", Host: "connect.example.com", Port: 3},
			{Component: "kafka_rest", Host: "rest.example.com", Port: 4},
		}

		secret := (&kafkaAdapter{Kafka: newKafka()}).newSecret(s)
		require.NotNil(t, secret)

		assert.Equal(t, "sasl.example.com", secret.StringData["KAFKA_SASL_HOST"])
		assert.Equal(t, "1", secret.StringData["KAFKA_SASL_PORT"])
		assert.Equal(t, "registry.example.com", secret.StringData["KAFKA_SCHEMA_REGISTRY_HOST"])
		assert.Equal(t, "2", secret.StringData["KAFKA_SCHEMA_REGISTRY_PORT"])
		assert.Equal(t, "connect.example.com", secret.StringData["KAFKA_CONNECT_HOST"])
		assert.Equal(t, "3", secret.StringData["KAFKA_CONNECT_PORT"])
		assert.Equal(t, "rest.example.com", secret.StringData["KAFKA_REST_HOST"])
		assert.Equal(t, "4", secret.StringData["KAFKA_REST_PORT"])
	})
}
