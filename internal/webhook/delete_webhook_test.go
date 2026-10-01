package webhook

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aiven/aiven-operator/api/v1alpha1"
)

func TestValidateDeleteAllowsInvalidSpec(t *testing.T) {
	// A spec that slipped past validation, e.g. created while webhooks were off, must stay deletable.
	kafka := &v1alpha1.Kafka{}
	kafka.Spec.ProjectVPCID = "vpc"
	kafka.Spec.ProjectVPCRef = &v1alpha1.ResourceReference{Name: "vpc"}
	_, err := (&KafkaWebhook{}).ValidateDelete(t.Context(), kafka)
	require.NoError(t, err)
}

func TestProjectValidateDeleteAllowsNeverReconciledProject(t *testing.T) {
	_, err := (&ProjectWebhook{}).ValidateDelete(t.Context(), &v1alpha1.Project{})
	require.NoError(t, err)

	open := &v1alpha1.Project{}
	open.Status.EstimatedBalance = "12.34"
	_, err = (&ProjectWebhook{}).ValidateDelete(t.Context(), open)
	require.Error(t, err)
}
