package controllers

import (
	"testing"

	avngen "github.com/aiven/go-client-codegen"
	"github.com/aiven/go-client-codegen/handler/kafka"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrlruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/aiven/aiven-operator/api/v1alpha1"
)

func newKafkaACL(t *testing.T) *v1alpha1.KafkaACL {
	t.Helper()
	acl := newObjectFromExampleYAML[v1alpha1.KafkaACL](t, "kafkaacl")
	acl.Namespace = "default"
	return acl
}

func runKafkaACLScenario(
	t *testing.T,
	acl *v1alpha1.KafkaACL,
	avn avngen.Client,
	additionalObjects ...client.Object,
) (*Reconciler[*v1alpha1.KafkaACL], ctrlruntime.Result, error) {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	objects := append([]client.Object{acl}, additionalObjects...)

	r := newKafkaACLReconciler(Controller{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&v1alpha1.KafkaACL{}).
			WithObjects(objects...).
			Build(),
		Scheme:       scheme,
		Recorder:     record.NewFakeRecorder(10),
		DefaultToken: "test-token",
		PollInterval: testPollInterval,
	}).(*Reconciler[*v1alpha1.KafkaACL])
	r.newAivenGeneratedClient = func(_, _, _ string) (avngen.Client, error) {
		return avn, nil
	}
	r.jitter = nil // deterministic RequeueAfter

	res, err := r.Reconcile(t.Context(), ctrlruntime.Request{
		NamespacedName: types.NamespacedName{
			Name:      acl.Name,
			Namespace: acl.Namespace,
		},
	})
	return r, res, err
}

func TestKafkaACLReconciler(t *testing.T) {
	t.Parallel()

	t.Run("Requeues when service preconditions aren't met", func(t *testing.T) {
		acl := newKafkaACL(t)
		acl.Generation = 1

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName, mock.Anything).
			Return(nil, newAivenError(404, "service not found")).Once()

		r, res, err := runKafkaACLScenario(t, acl, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{RequeueAfter: requeueTimeout}, res)

		got := &v1alpha1.KafkaACL{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: acl.Name, Namespace: acl.Namespace}, got))
		require.Contains(t, got.Finalizers, instanceDeletionFinalizer)
		require.NotContains(t, got.Annotations, processedGenerationAnnotation)
	})

	t.Run("Creates KafkaACL on Aiven when it does not exist", func(t *testing.T) {
		acl := newKafkaACL(t)
		acl.Generation = 1

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName, mock.Anything).
			Return(runningService(), nil).Once()
		// Observe: no ACL exists yet.
		avn.EXPECT().
			ServiceKafkaAclList(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName).
			Return(nil, nil).Once()
		// applyACL does not list to find something to delete: this CR was never applied, so it
		// owns no ACL and nothing may be resolved by content.
		avn.EXPECT().
			ServiceKafkaAclAdd(
				mock.Anything, acl.Spec.Project, acl.Spec.ServiceName,
				mock.MatchedBy(func(in *kafka.ServiceKafkaAclAddIn) bool {
					return in.Permission == acl.Spec.Permission &&
						in.Topic == acl.Spec.Topic &&
						in.Username == acl.Spec.Username
				}),
			).Return(nil, nil).Once()
		avn.EXPECT().
			ServiceKafkaAclList(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName).
			Return([]kafka.ServiceKafkaAclListOut{{
				Id:         new("acl-id"),
				Permission: acl.Spec.Permission,
				Topic:      acl.Spec.Topic,
				Username:   acl.Spec.Username,
			}}, nil).Once()

		r, res, err := runKafkaACLScenario(t, acl, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{RequeueAfter: testPollInterval}, res)

		got := &v1alpha1.KafkaACL{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: acl.Name, Namespace: acl.Namespace}, got))
		require.Equal(t, "acl-id", got.Status.ID)
		require.Equal(t, "1", got.Annotations[processedGenerationAnnotation])
		require.Equal(t, "true", got.Annotations[instanceIsRunningAnnotation])
	})

	t.Run("Marks KafkaACL running when it already exists", func(t *testing.T) {
		acl := newKafkaACL(t)
		acl.Generation = 1
		acl.Annotations = map[string]string{processedGenerationAnnotation: "1"}
		acl.Status.ID = "acl-id"

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName, mock.Anything).
			Return(runningService(), nil).Once()
		// Observe matches the live ACL by content (topic/username/permission).
		avn.EXPECT().
			ServiceKafkaAclList(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName).
			Return([]kafka.ServiceKafkaAclListOut{{
				Id:         new("acl-id"),
				Permission: acl.Spec.Permission,
				Topic:      acl.Spec.Topic,
				Username:   acl.Spec.Username,
			}}, nil).Once()

		r, res, err := runKafkaACLScenario(t, acl, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{RequeueAfter: testPollInterval}, res)

		got := &v1alpha1.KafkaACL{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: acl.Name, Namespace: acl.Namespace}, got))
		require.Equal(t, "true", got.Annotations[instanceIsRunningAnnotation])
		require.Equal(t, "acl-id", got.Status.ID)
	})

	t.Run("Deletes KafkaACL and removes finalizer on deletion", func(t *testing.T) {
		acl := newKafkaACL(t)
		acl.Generation = 1
		acl.Status.ID = "acl-id"
		acl.Finalizers = []string{instanceDeletionFinalizer}
		now := metav1.Now()
		acl.DeletionTimestamp = &now

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceKafkaAclDelete(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName, "acl-id").
			Return(nil, nil).Once()

		r, res, err := runKafkaACLScenario(t, acl, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{}, res)

		got := &v1alpha1.KafkaACL{}
		err = r.Get(t.Context(), types.NamespacedName{Name: acl.Name, Namespace: acl.Namespace}, got)
		require.True(t, apierrors.IsNotFound(err))
	})

	t.Run("Treats 404 on delete as already deleted", func(t *testing.T) {
		acl := newKafkaACL(t)
		acl.Generation = 1
		acl.Status.ID = "acl-id"
		acl.Finalizers = []string{instanceDeletionFinalizer}
		now := metav1.Now()
		acl.DeletionTimestamp = &now

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceKafkaAclDelete(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName, "acl-id").
			Return(nil, newAivenError(404, "not found")).Once()

		r, res, err := runKafkaACLScenario(t, acl, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{}, res)

		got := &v1alpha1.KafkaACL{}
		err = r.Get(t.Context(), types.NamespacedName{Name: acl.Name, Namespace: acl.Namespace}, got)
		require.True(t, apierrors.IsNotFound(err))
	})

	t.Run("Does not recreate when ACL matches spec but generation annotation is missing", func(t *testing.T) {
		acl := newKafkaACL(t)
		acl.Generation = 1
		acl.Status.ID = "acl-id"

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName, mock.Anything).
			Return(runningService(), nil).Once()
		// The ACL matching the current spec already exists; no Add/Delete must happen.
		avn.EXPECT().
			ServiceKafkaAclList(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName).
			Return([]kafka.ServiceKafkaAclListOut{{
				Id:         new("acl-id"),
				Permission: acl.Spec.Permission,
				Topic:      acl.Spec.Topic,
				Username:   acl.Spec.Username,
			}}, nil).Once()

		r, res, err := runKafkaACLScenario(t, acl, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{RequeueAfter: testPollInterval}, res)

		got := &v1alpha1.KafkaACL{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: acl.Name, Namespace: acl.Namespace}, got))
		require.Equal(t, "acl-id", got.Status.ID)
		require.Equal(t, "1", got.Annotations[processedGenerationAnnotation])
	})

	t.Run("Recreates ACL when spec changed", func(t *testing.T) {
		acl := newKafkaACL(t)
		acl.Generation = 2
		acl.Annotations = map[string]string{processedGenerationAnnotation: "1"}
		acl.Status.ID = "old-id"
		// Spec now asks for a different permission than the live ACL.
		acl.Spec.Permission = kafka.PermissionTypeWrite

		liveACL := kafka.ServiceKafkaAclListOut{
			Id:         new("old-id"),
			Permission: kafka.PermissionTypeAdmin,
			Topic:      acl.Spec.Topic,
			Username:   acl.Spec.Username,
		}

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName, mock.Anything).
			Return(runningService(), nil).Once()
		// Observe: no ACL matches the new spec, but Status.ID is set -> exists, out of date.
		avn.EXPECT().
			ServiceKafkaAclList(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName).
			Return([]kafka.ServiceKafkaAclListOut{liveACL}, nil).Once()
		// Update -> applyACL -> deleteACL: Status.ID short-circuits getID, delete by old id.
		avn.EXPECT().
			ServiceKafkaAclDelete(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName, "old-id").
			Return(nil, nil).Once()
		avn.EXPECT().
			ServiceKafkaAclAdd(
				mock.Anything, acl.Spec.Project, acl.Spec.ServiceName,
				mock.MatchedBy(func(in *kafka.ServiceKafkaAclAddIn) bool {
					return in.Permission == kafka.PermissionTypeWrite &&
						in.Topic == acl.Spec.Topic &&
						in.Username == acl.Spec.Username
				}),
			).Return(nil, nil).Once()
		// applyACL final getID: Status.ID was reset, so it lists and resolves the new ACL.
		avn.EXPECT().
			ServiceKafkaAclList(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName).
			Return([]kafka.ServiceKafkaAclListOut{{
				Id:         new("new-id"),
				Permission: kafka.PermissionTypeWrite,
				Topic:      acl.Spec.Topic,
				Username:   acl.Spec.Username,
			}}, nil).Once()

		r, res, err := runKafkaACLScenario(t, acl, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{RequeueAfter: testPollInterval}, res)

		got := &v1alpha1.KafkaACL{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: acl.Name, Namespace: acl.Namespace}, got))
		require.Equal(t, "new-id", got.Status.ID)
		require.Equal(t, "2", got.Annotations[processedGenerationAnnotation])
		require.Equal(t, "true", got.Annotations[instanceIsRunningAnnotation])
	})
}

// A KafkaACL whose Observe never completed — the service was not running yet, the token was
// rejected, the API failed — has no Status.ID and no processed-generation annotation. Deleting it
// must not resolve an ACL by (topic, username, permission) alone: the only entries that match are
// ones created by hand or by another resource.
func TestKafkaACLDeleteDoesNotTouchUnownedACLs(t *testing.T) {
	t.Parallel()

	acl := newKafkaACL(t)
	acl.Generation = 1
	acl.Finalizers = []string{instanceDeletionFinalizer}
	now := metav1.Now()
	acl.DeletionTimestamp = &now
	// Never applied: no Status.ID, no processed-generation annotation.
	require.Empty(t, acl.Status.ID)
	require.False(t, wasEverApplied(acl))

	avn := avngen.NewMockClient(t)
	// Somebody else's ACL happens to match this spec. Listing it at all would be enough to go
	// on and delete it, so the list is allowed but the delete is never registered: an
	// unexpected ServiceKafkaAclDelete fails the test.
	avn.EXPECT().
		ServiceKafkaAclList(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName).
		Return([]kafka.ServiceKafkaAclListOut{{
			Id:         new("someone-elses-acl"),
			Permission: acl.Spec.Permission,
			Topic:      acl.Spec.Topic,
			Username:   acl.Spec.Username,
		}}, nil).Maybe()

	r, res, err := runKafkaACLScenario(t, acl, avn)
	require.NoError(t, err)
	require.Equal(t, ctrlruntime.Result{}, res)

	// The finalizer is gone and the resource is deleted, without an ACL being removed.
	got := &v1alpha1.KafkaACL{}
	require.True(t, apierrors.IsNotFound(
		r.Get(t.Context(), types.NamespacedName{Name: acl.Name, Namespace: acl.Namespace}, got)))
}

// An ACL created before v0.5.1 has no stored ID but was applied, so the content fallback still
// has to resolve and delete it.
func TestKafkaACLDeleteResolvesLegacyACLByContent(t *testing.T) {
	t.Parallel()

	acl := newKafkaACL(t)
	acl.Generation = 1
	acl.Finalizers = []string{instanceDeletionFinalizer}
	now := metav1.Now()
	acl.DeletionTimestamp = &now
	// Applied by an older operator version, which stored no ID.
	acl.Annotations = map[string]string{processedGenerationAnnotation: "1"}
	require.Empty(t, acl.Status.ID)

	avn := avngen.NewMockClient(t)
	avn.EXPECT().
		ServiceKafkaAclList(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName).
		Return([]kafka.ServiceKafkaAclListOut{{
			Id:         new("legacy-acl"),
			Permission: acl.Spec.Permission,
			Topic:      acl.Spec.Topic,
			Username:   acl.Spec.Username,
		}}, nil).Once()
	avn.EXPECT().
		ServiceKafkaAclDelete(mock.Anything, acl.Spec.Project, acl.Spec.ServiceName, "legacy-acl").
		Return(nil, nil).Once()

	r, res, err := runKafkaACLScenario(t, acl, avn)
	require.NoError(t, err)
	require.Equal(t, ctrlruntime.Result{}, res)

	got := &v1alpha1.KafkaACL{}
	require.True(t, apierrors.IsNotFound(
		r.Get(t.Context(), types.NamespacedName{Name: acl.Name, Namespace: acl.Namespace}, got)))
}
