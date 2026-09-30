package controllers

import (
	"testing"

	avngen "github.com/aiven/go-client-codegen"
	"github.com/aiven/go-client-codegen/handler/service"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/aiven/aiven-operator/api/v1alpha1"
	pguserconfig "github.com/aiven/aiven-operator/api/v1alpha1/userconfig/service/pg"
)

func TestPerformUpgradeTaskIfNeeded(t *testing.T) {
	newAdapter := func(t *testing.T) *postgreSQLAdapter {
		pg := newObjectFromYAML[v1alpha1.PostgreSQL](t, yamlPostgres)
		pg.Spec.UserConfig = &pguserconfig.PgUserConfig{PgVersion: new("17")}
		return &postgreSQLAdapter{PostgreSQL: pg}
	}

	t.Run("Reports the result of the failed check", func(t *testing.T) {
		a := newAdapter(t)
		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceTaskCreate(mock.Anything, a.Spec.Project, a.Name, mock.Anything).
			Return(&service.ServiceTaskCreateOut{TaskId: "task"}, nil).Once()
		avn.EXPECT().
			ServiceTaskGet(mock.Anything, a.Spec.Project, a.Name, "task").
			Return(&service.ServiceTaskGetOut{Result: "incompatible extension"}, nil).Once()

		err := a.performUpgradeTaskIfNeeded(t.Context(), avn, &service.ServiceGetOut{UserConfig: map[string]any{"pg_version": "16"}})
		require.ErrorContains(t, err, "incompatible extension")
	})

	t.Run("Reads the current version from metadata when user_config has none", func(t *testing.T) {
		a := newAdapter(t)
		avn := avngen.NewMockClient(t) // 17 is already running, no upgrade check task

		require.NoError(t, a.performUpgradeTaskIfNeeded(t.Context(), avn, &service.ServiceGetOut{Metadata: map[string]any{"pg_version": "17"}}))
	})

	t.Run("Checks the upgrade from the metadata version", func(t *testing.T) {
		a := newAdapter(t)
		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceTaskCreate(mock.Anything, a.Spec.Project, a.Name, mock.Anything).
			Return(&service.ServiceTaskCreateOut{TaskId: "task"}, nil).Once()
		avn.EXPECT().
			ServiceTaskGet(mock.Anything, a.Spec.Project, a.Name, "task").
			Return(&service.ServiceTaskGetOut{Success: true}, nil).Once()

		require.NoError(t, a.performUpgradeTaskIfNeeded(t.Context(), avn, &service.ServiceGetOut{Metadata: map[string]any{"pg_version": "16"}}))
	})

	t.Run("Skips the check when the current version is unknown", func(t *testing.T) {
		a := newAdapter(t)
		avn := avngen.NewMockClient(t) // no upgrade check task

		require.NoError(t, a.performUpgradeTaskIfNeeded(t.Context(), avn, &service.ServiceGetOut{}))
	})
}
