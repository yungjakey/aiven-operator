package controllers

import (
	"context"
	"testing"

	avngen "github.com/aiven/go-client-codegen"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/aiven/aiven-operator/api/v1alpha1"
)

func TestInstanceReconcilerHelper_reconcile(t *testing.T) {
	t.Run("Marks resource not ready before dependency gates when connection Secret must be published", func(t *testing.T) {
		pg := newObjectFromYAML[v1alpha1.PostgreSQL](t, yamlPostgres)
		pg.UID = types.UID("pg-uid")
		pg.Generation = 1
		metav1.SetMetaDataAnnotation(&pg.ObjectMeta, processedGenerationAnnotation, "1")
		metav1.SetMetaDataAnnotation(&pg.ObjectMeta, instanceIsRunningAnnotation, "true")
		meta.SetStatusCondition(&pg.Status.Conditions, getRunningCondition(metav1.ConditionTrue, "CheckRunning", "Instance is running on Aiven side"))
		pg.Finalizers = []string{instanceDeletionFinalizer}
		pg.Spec.ProjectVPCRef = &v1alpha1.ResourceReference{Name: "missing-vpc"}

		scheme := runtime.NewScheme()
		require.NoError(t, clientgoscheme.AddToScheme(scheme))
		require.NoError(t, v1alpha1.AddToScheme(scheme))

		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&v1alpha1.PostgreSQL{}).
			WithObjects(pg.DeepCopy()).
			Build()

		helper := &instanceReconcilerHelper{
			k8s: k8sClient,
			h:   NewMockHandlers(t),
			rec: record.NewFakeRecorder(10),
		}

		requeue, err := helper.reconcile(t.Context(), pg)
		require.NoError(t, err)
		require.True(t, requeue)

		require.False(t, hasIsRunningAnnotation(pg))
		running := meta.FindStatusCondition(pg.Status.Conditions, conditionTypeRunning)
		require.NotNil(t, running)
		require.Equal(t, metav1.ConditionUnknown, running.Status)
		require.Equal(t, string(errConditionConnInfoSecret), running.Reason)
		require.Nil(t, meta.FindStatusCondition(pg.Status.Conditions, ConditionTypeError))

		got := &v1alpha1.PostgreSQL{}
		require.NoError(t, k8sClient.Get(t.Context(), types.NamespacedName{Name: pg.Name, Namespace: pg.Namespace}, got))

		require.False(t, hasIsRunningAnnotation(got))
		running = meta.FindStatusCondition(got.Status.Conditions, conditionTypeRunning)
		require.NotNil(t, running)
		require.Equal(t, metav1.ConditionUnknown, running.Status)
		require.Equal(t, string(errConditionConnInfoSecret), running.Reason)
		require.Nil(t, meta.FindStatusCondition(got.Status.Conditions, ConditionTypeError))
	})

	t.Run("Sets error condition when a ref points to a disabled kind", func(t *testing.T) {
		pg := newObjectFromYAML[v1alpha1.PostgreSQL](t, yamlPostgresWithRef)
		pg.Finalizers = []string{instanceDeletionFinalizer}

		scheme := runtime.NewScheme()
		require.NoError(t, clientgoscheme.AddToScheme(scheme))
		require.NoError(t, v1alpha1.AddToScheme(scheme))

		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&v1alpha1.PostgreSQL{}).
			WithObjects(pg.DeepCopy()).
			Build()

		helper := &instanceReconcilerHelper{
			k8s:   k8sClient,
			h:     NewMockHandlers(t),
			rec:   record.NewFakeRecorder(10),
			kinds: kindSet{"PostgreSQL": true},
		}

		_, err := helper.getObjectRefs(t.Context(), pg)
		require.ErrorIs(t, err, errRefKindDisabled)

		_, err = helper.reconcile(t.Context(), pg)
		require.ErrorIs(t, err, errRefKindDisabled, "reconcile error must survive the status update")

		got := &v1alpha1.PostgreSQL{}
		require.NoError(t, k8sClient.Get(t.Context(), types.NamespacedName{Name: pg.Name, Namespace: pg.Namespace}, got))
		cond := meta.FindStatusCondition(got.Status.Conditions, ConditionTypeError)
		require.NotNil(t, cond)
		require.Contains(t, cond.Message, "enable ProjectVPC")
	})
	t.Run("Does not overwrite spec changes made during reconciliation", func(t *testing.T) {
		pg := newObjectFromYAML[v1alpha1.PostgreSQL](t, yamlPostgres)
		pg.Generation = 1

		scheme := runtime.NewScheme()
		require.NoError(t, clientgoscheme.AddToScheme(scheme))
		require.NoError(t, v1alpha1.AddToScheme(scheme))

		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&v1alpha1.PostgreSQL{}).
			WithObjects(pg.DeepCopy()).
			Build()

		key := types.NamespacedName{Name: pg.Name, Namespace: pg.Namespace}
		h := NewMockHandlers(t)
		h.EXPECT().checkPreconditions(mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Once()
		h.EXPECT().createOrUpdate(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, _ avngen.Client, _ client.Object, _ []client.Object) error {
				// The user edits the spec while the controller talks to Aiven.
				latest := &v1alpha1.PostgreSQL{}
				require.NoError(t, k8sClient.Get(ctx, key, latest))
				latest.Spec.Plan = "business-8"
				latest.Annotations = map[string]string{"user": "annotation"}
				return k8sClient.Update(ctx, latest)
			}).Once()
		h.EXPECT().observe(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		helper := &instanceReconcilerHelper{k8s: k8sClient, h: h, rec: record.NewFakeRecorder(100)}
		_, err := helper.reconcile(t.Context(), pg)
		require.NoError(t, err)

		got := &v1alpha1.PostgreSQL{}
		require.NoError(t, k8sClient.Get(t.Context(), key, got))
		require.Equal(t, "business-8", got.Spec.Plan)
		require.Equal(t, "annotation", got.Annotations["user"])
		require.Equal(t, "1", got.Annotations[processedGenerationAnnotation])
		require.Contains(t, got.Finalizers, instanceDeletionFinalizer)
		require.NotNil(t, meta.FindStatusCondition(got.Status.Conditions, conditionTypeInitialized))
	})
}
