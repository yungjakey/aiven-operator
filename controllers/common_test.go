package controllers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aiven/aiven-operator/api/v1alpha1"
	kafkaconnectuserconfig "github.com/aiven/aiven-operator/api/v1alpha1/userconfig/integration/kafka_connect"
)

// TestCreateEmptyUserConfiguration shouldn't panic
func TestCreateEmptyUserConfiguration(t *testing.T) {
	var uc *kafkaconnectuserconfig.KafkaConnectUserConfig
	m, err := CreateUserConfiguration(uc)
	assert.Empty(t, m)
	assert.NoError(t, err)
}

func TestNewSecretPrefixesEachKeyOnce(t *testing.T) {
	// Adding keys to a map while ranging over it may revisit them, which used to prefix keys twice.
	stringData := map[string]string{}
	for i := range 32 {
		stringData[fmt.Sprintf("KEY_%d", i)] = "v"
	}

	pg := &v1alpha1.PostgreSQL{}
	pg.Spec.ConnInfoSecretTarget.Prefix = "P_"
	secret := newSecret(pg, stringData, true)

	require.Len(t, secret.StringData, 32)
	for i := range 32 {
		require.Contains(t, secret.StringData, fmt.Sprintf("P_KEY_%d", i))
	}
}
