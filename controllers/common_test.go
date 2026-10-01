package controllers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

func TestErrIfEntrySharedHonorsOrphanPolicy(t *testing.T) {
	self := v1alpha1.KafkaTopic{}
	self.Name = "a"
	now := metav1.Now()

	deleting := v1alpha1.KafkaTopic{}
	deleting.Name = "b"
	deleting.DeletionTimestamp = &now
	same := func(*v1alpha1.KafkaTopic) bool { return true }

	require.NoError(t, errIfEntryShared([]v1alpha1.KafkaTopic{self, deleting}, &self, same))

	// Deleted together, but b keeps the entry at Aiven.
	deleting.Annotations = map[string]string{deletionPolicyAnnotation: deletionPolicyOrphan}
	require.ErrorIs(t, errIfEntryShared([]v1alpha1.KafkaTopic{self, deleting}, &self, same), errDeletionSkipped)
}
