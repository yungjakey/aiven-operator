package controllers

import (
	"context"
	"slices"
	"testing"
	"time"

	avngen "github.com/aiven/go-client-codegen"
	"github.com/aiven/go-client-codegen/handler/kafkatopic"
	"github.com/aiven/go-client-codegen/handler/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
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

const yamlKafkaTopic = `
apiVersion: aiven.io/v1alpha1
kind: KafkaTopic
metadata:
  name: test-topic
  namespace: default
spec:
  project: test-project
  serviceName: test-service
  partitions: 3
  replication: 2
  tags:
    - key: env
      value: test
  config:
    cleanup_policy: delete
    retention_bytes: 123
`

// topicMatchingSpec builds the topic Aiven would report for a KafkaTopic that is fully applied,
// so that Observe sees no drift.
func topicMatchingSpec(topic *v1alpha1.KafkaTopic, state kafkatopic.TopicStateType) kafkatopic.TopicOut {
	out := kafkatopic.TopicOut{
		TopicName:   topic.GetTopicName(),
		State:       state,
		Partitions:  topic.Spec.Partitions,
		Replication: topic.Spec.Replication,
	}
	for _, t := range topic.Spec.Tags {
		out.Tags = append(out.Tags, kafkatopic.TagOut{Key: t.Key, Value: t.Value})
	}
	if cfg := topic.Spec.Config; cfg != nil {
		out.CleanupPolicy = string(cfg.CleanupPolicy)
		out.MinInsyncReplicas = fromAnyPointer(cfg.MinInsyncReplicas)
		out.RetentionBytes = fromAnyPointer(cfg.RetentionBytes)
		out.DisklessEnable = cfg.DisklessEnable
		out.RemoteStorageEnable = cfg.RemoteStorageEnable
	}
	return out
}

func Test_newKafkaTopicReconciler(t *testing.T) {
	t.Parallel()

	r := newKafkaTopicReconciler(Controller{}).(*Reconciler[*v1alpha1.KafkaTopic])
	require.NotNil(t, r.options)
	require.Equal(t, kafkaTopicMaxConcurrentReconciles, r.options.MaxConcurrentReconciles)
}

func TestKafkaTopicReconciler(t *testing.T) {
	t.Parallel()

	runScenario := func(t *testing.T, topic *v1alpha1.KafkaTopic, avn avngen.Client, additionalObjects ...client.Object) (*Reconciler[*v1alpha1.KafkaTopic], ctrlruntime.Result, error) {
		t.Helper()

		scheme := runtime.NewScheme()
		require.NoError(t, clientgoscheme.AddToScheme(scheme))
		require.NoError(t, v1alpha1.AddToScheme(scheme))

		objects := append([]client.Object{topic}, additionalObjects...)

		r := newKafkaTopicReconciler(Controller{
			Client: fake.NewClientBuilder().
				WithScheme(scheme).
				WithStatusSubresource(&v1alpha1.KafkaTopic{}).
				WithObjects(objects...).
				Build(),
			Scheme:       scheme,
			Recorder:     record.NewFakeRecorder(10),
			DefaultToken: "test-token",
			PollInterval: testPollInterval,
		}).(*Reconciler[*v1alpha1.KafkaTopic])
		r.newAivenGeneratedClient = func(_, _, _ string) (avngen.Client, error) {
			return avn, nil
		}
		r.jitter = nil // deterministic RequeueAfter

		res, err := r.Reconcile(t.Context(), ctrlruntime.Request{
			NamespacedName: types.NamespacedName{
				Name:      topic.Name,
				Namespace: topic.Namespace,
			},
		})
		return r, res, err
	}

	t.Run("Requeues when service preconditions aren't met", func(t *testing.T) {
		topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
		topic.Generation = 1
		topic.Spec.Project = "test-project-preconditions"
		topic.Spec.ServiceName = "test-service-preconditions"

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return(nil, newAivenError(404, "service not found")).Once()

		r, res, err := runScenario(t, topic, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{RequeueAfter: requeueTimeout}, res)

		got := &v1alpha1.KafkaTopic{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: topic.Name, Namespace: topic.Namespace}, got))
		require.Contains(t, got.Finalizers, instanceDeletionFinalizer)
		require.NotContains(t, got.Annotations, processedGenerationAnnotation)
	})

	t.Run("Creates KafkaTopic on Aiven", func(t *testing.T) {
		topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
		topic.Generation = 1
		topic.Spec.Project = "test-project-create"
		topic.Spec.ServiceName = "test-service-create"
		topic.Spec.Config = nil

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return(&service.ServiceGetOut{
				State: service.ServiceStateTypeRunning,
				NodeStates: []service.NodeStateOut{
					{State: service.NodeStateTypeRunning},
					{State: service.NodeStateTypeRunning},
					{State: service.NodeStateTypeRunning},
				},
			}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicList(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return([]kafkatopic.TopicOut{}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicCreate(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName, mock.MatchedBy(func(in *kafkatopic.ServiceKafkaTopicCreateIn) bool {
				return *in.Partitions == topic.Spec.Partitions &&
					*in.Replication == topic.Spec.Replication &&
					in.TopicName == topic.GetTopicName() &&
					slices.Equal(*in.Tags, []kafkatopic.TagIn{{Key: "env", Value: "test"}}) &&
					in.Config == nil
			})).Return(nil).Once()

		r, res, err := runScenario(t, topic, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{RequeueAfter: requeueTimeout}, res)

		got := &v1alpha1.KafkaTopic{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: topic.Name, Namespace: topic.Namespace}, got))
		require.Contains(t, got.Finalizers, instanceDeletionFinalizer)
		require.Equal(t, "1", got.Annotations[processedGenerationAnnotation])
		require.NotContains(t, got.Annotations, instanceIsRunningAnnotation)
	})

	t.Run("Requeues when KafkaTopic list returns server error after generation is processed", func(t *testing.T) {
		topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
		topic.Generation = 1
		topic.Spec.Project = "test-project-list-5xx"
		topic.Spec.ServiceName = "test-service-list-5xx"
		topic.Annotations = map[string]string{processedGenerationAnnotation: "1"}

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return(&service.ServiceGetOut{
				State: service.ServiceStateTypeRunning,
				NodeStates: []service.NodeStateOut{
					{State: service.NodeStateTypeRunning},
					{State: service.NodeStateTypeRunning},
				},
			}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicList(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return(nil, newAivenError(500, "server error")).Once()

		r, res, err := runScenario(t, topic, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{RequeueAfter: requeueTimeout}, res)

		got := &v1alpha1.KafkaTopic{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: topic.Name, Namespace: topic.Namespace}, got))
		require.Equal(t, "1", got.Annotations[processedGenerationAnnotation])
		require.NotContains(t, got.Annotations, instanceIsRunningAnnotation)
	})

	t.Run("Updates KafkaTopic when list returns server error during spec update", func(t *testing.T) {
		topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
		topic.Generation = 2
		topic.Spec.Project = "test-project-update-list-5xx"
		topic.Spec.ServiceName = "test-service-update-list-5xx"
		topic.Annotations = map[string]string{
			processedGenerationAnnotation: "1",
			instanceIsRunningAnnotation:   "true",
		}

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return(&service.ServiceGetOut{
				State: service.ServiceStateTypeRunning,
				NodeStates: []service.NodeStateOut{
					{State: service.NodeStateTypeRunning},
					{State: service.NodeStateTypeRunning},
				},
			}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicList(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return(nil, newAivenError(500, "server error")).Once()
		avn.EXPECT().
			ServiceKafkaTopicUpdate(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName, topic.GetTopicName(), mock.Anything).
			Return(nil).Once()

		r, res, err := runScenario(t, topic, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{RequeueAfter: requeueTimeout}, res)

		got := &v1alpha1.KafkaTopic{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: topic.Name, Namespace: topic.Namespace}, got))
		require.Equal(t, "2", got.Annotations[processedGenerationAnnotation])
		require.NotContains(t, got.Annotations, instanceIsRunningAnnotation)
	})

	t.Run("Updates status and requeues when KafkaTopic is configuring", func(t *testing.T) {
		topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
		topic.Generation = 1
		topic.Spec.Project = "test-project-configuring"
		topic.Spec.ServiceName = "test-service-configuring"
		topic.Annotations = map[string]string{processedGenerationAnnotation: "1"}

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return(&service.ServiceGetOut{
				State: service.ServiceStateTypeRunning,
				NodeStates: []service.NodeStateOut{
					{State: service.NodeStateTypeRunning},
					{State: service.NodeStateTypeRunning},
				},
			}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicList(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return([]kafkatopic.TopicOut{
				topicMatchingSpec(topic, kafkatopic.TopicStateTypeConfiguring),
			}, nil).Once()

		r, res, err := runScenario(t, topic, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{RequeueAfter: requeueTimeout}, res)

		got := &v1alpha1.KafkaTopic{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: topic.Name, Namespace: topic.Namespace}, got))
		require.Equal(t, kafkatopic.TopicStateTypeConfiguring, got.Status.State)
		require.NotContains(t, got.Annotations, instanceIsRunningAnnotation)
	})

	t.Run("Repairs KafkaTopic that drifted on Aiven", func(t *testing.T) {
		topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
		topic.Generation = 1
		topic.Spec.Project = "test-project-drift"
		topic.Spec.ServiceName = "test-service-drift"
		// The generation was processed, so nothing about the manifest changed: the topic was
		// altered outside the operator.
		topic.Annotations = map[string]string{
			processedGenerationAnnotation: "1",
			instanceIsRunningAnnotation:   "true",
		}

		drifted := topicMatchingSpec(topic, kafkatopic.TopicStateTypeActive)
		drifted.Partitions = 1

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return(&service.ServiceGetOut{
				State:      service.ServiceStateTypeRunning,
				NodeStates: []service.NodeStateOut{{State: service.NodeStateTypeRunning}, {State: service.NodeStateTypeRunning}},
			}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicList(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return([]kafkatopic.TopicOut{drifted}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicUpdate(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName, topic.GetTopicName(),
				mock.MatchedBy(func(in *kafkatopic.ServiceKafkaTopicUpdateIn) bool {
					return *in.Partitions == topic.Spec.Partitions
				})).Return(nil).Once()

		_, res, err := runScenario(t, topic, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{RequeueAfter: requeueTimeout}, res)
	})

	t.Run("Clears the running marker when KafkaTopic leaves ACTIVE", func(t *testing.T) {
		topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
		topic.Generation = 1
		topic.Spec.Project = "test-project-left-active"
		topic.Spec.ServiceName = "test-service-left-active"
		topic.Annotations = map[string]string{
			processedGenerationAnnotation: "1",
			instanceIsRunningAnnotation:   "true", // set by an earlier, ACTIVE observation
		}

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return(&service.ServiceGetOut{
				State:      service.ServiceStateTypeRunning,
				NodeStates: []service.NodeStateOut{{State: service.NodeStateTypeRunning}, {State: service.NodeStateTypeRunning}},
			}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicList(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return([]kafkatopic.TopicOut{topicMatchingSpec(topic, kafkatopic.TopicStateTypeConfiguring)}, nil).Once()

		r, res, err := runScenario(t, topic, avn)
		require.NoError(t, err)
		// Not ready any more, so the short requeue rather than the poll interval.
		require.Equal(t, ctrlruntime.Result{RequeueAfter: requeueTimeout}, res)

		got := &v1alpha1.KafkaTopic{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: topic.Name, Namespace: topic.Namespace}, got))
		require.Equal(t, kafkatopic.TopicStateTypeConfiguring, got.Status.State)
		require.NotContains(t, got.Annotations, instanceIsRunningAnnotation)
		require.False(t, IsReadyToUse(got))

		running := meta.FindStatusCondition(got.Status.Conditions, conditionTypeRunning)
		require.NotNil(t, running)
		require.Equal(t, metav1.ConditionFalse, running.Status)
		require.Contains(t, running.Message, string(kafkatopic.TopicStateTypeConfiguring))
	})

	t.Run("Marks KafkaTopic running when it becomes ACTIVE", func(t *testing.T) {
		topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
		topic.Generation = 1
		topic.Spec.Project = "test-project-active"
		topic.Spec.ServiceName = "test-service-active"
		topic.Annotations = map[string]string{processedGenerationAnnotation: "1"}

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return(&service.ServiceGetOut{
				State: service.ServiceStateTypeRunning,
				NodeStates: []service.NodeStateOut{
					{State: service.NodeStateTypeRunning},
					{State: service.NodeStateTypeRunning},
				},
			}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicList(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return([]kafkatopic.TopicOut{
				topicMatchingSpec(topic, kafkatopic.TopicStateTypeActive),
			}, nil).Once()

		r, res, err := runScenario(t, topic, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{RequeueAfter: testPollInterval}, res)

		got := &v1alpha1.KafkaTopic{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: topic.Name, Namespace: topic.Namespace}, got))
		require.Equal(t, kafkatopic.TopicStateTypeActive, got.Status.State)
		require.Equal(t, "true", got.Annotations[instanceIsRunningAnnotation])
	})

	t.Run("Updates KafkaTopic on Aiven when generation changes", func(t *testing.T) {
		topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
		topic.Generation = 2
		topic.Spec.Project = "test-project-update"
		topic.Spec.ServiceName = "test-service-update"
		topic.Annotations = map[string]string{
			processedGenerationAnnotation: "1",
			instanceIsRunningAnnotation:   "true",
		}

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return(&service.ServiceGetOut{
				State: service.ServiceStateTypeRunning,
				NodeStates: []service.NodeStateOut{
					{State: service.NodeStateTypeRunning},
					{State: service.NodeStateTypeRunning},
				},
			}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicList(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return([]kafkatopic.TopicOut{
				topicMatchingSpec(topic, kafkatopic.TopicStateTypeActive),
			}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicUpdate(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName, topic.GetTopicName(), mock.MatchedBy(func(in *kafkatopic.ServiceKafkaTopicUpdateIn) bool {
				return *in.Partitions == topic.Spec.Partitions &&
					*in.Replication == topic.Spec.Replication &&
					slices.Equal(*in.Tags, []kafkatopic.TagIn{{Key: "env", Value: "test"}}) &&
					*in.Config.RetentionBytes == 123
			})).Return(nil).Once()

		r, res, err := runScenario(t, topic, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{RequeueAfter: requeueTimeout}, res)

		got := &v1alpha1.KafkaTopic{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: topic.Name, Namespace: topic.Namespace}, got))
		require.Equal(t, "2", got.Annotations[processedGenerationAnnotation])
		require.NotContains(t, got.Annotations, instanceIsRunningAnnotation)
		require.Equal(t, kafkatopic.TopicStateTypeActive, got.Status.State)
	})

	t.Run("Returns error when KafkaTopic isn't visible yet but API reports it already exists", func(t *testing.T) {
		topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
		topic.Generation = 1
		topic.Spec.Project = "test-project-not-visible"
		topic.Spec.ServiceName = "test-service-not-visible"
		topic.Annotations = map[string]string{processedGenerationAnnotation: "1"}

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return(&service.ServiceGetOut{
				State: service.ServiceStateTypeRunning,
				NodeStates: []service.NodeStateOut{
					{State: service.NodeStateTypeRunning},
					{State: service.NodeStateTypeRunning},
				},
			}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicList(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return([]kafkatopic.TopicOut{}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicCreate(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName, mock.MatchedBy(func(in *kafkatopic.ServiceKafkaTopicCreateIn) bool {
				return *in.Partitions == topic.Spec.Partitions &&
					*in.Replication == topic.Spec.Replication &&
					in.TopicName == topic.GetTopicName()
			})).Return(newAivenError(409, "already exists")).Once()

		r, res, err := runScenario(t, topic, avn)
		require.EqualError(t, err, `unable to create or update instance at aiven: creating Kafka topic: [409 ]: already exists`)
		require.Equal(t, ctrlruntime.Result{}, res)

		got := &v1alpha1.KafkaTopic{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: topic.Name, Namespace: topic.Namespace}, got))
		require.Equal(t, "1", got.Annotations[processedGenerationAnnotation])
		require.NotContains(t, got.Annotations, instanceIsRunningAnnotation)
	})

	t.Run("Recreates KafkaTopic when it disappears after being ready", func(t *testing.T) {
		topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
		topic.Generation = 1
		topic.Spec.Project = "test-project-drift"
		topic.Spec.ServiceName = "test-service-drift"
		topic.Annotations = map[string]string{
			processedGenerationAnnotation: "1",
			instanceIsRunningAnnotation:   "true",
		}

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceGet(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return(&service.ServiceGetOut{
				State: service.ServiceStateTypeRunning,
				NodeStates: []service.NodeStateOut{
					{State: service.NodeStateTypeRunning},
					{State: service.NodeStateTypeRunning},
				},
			}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicList(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
			Return([]kafkatopic.TopicOut{}, nil).Once()
		avn.EXPECT().
			ServiceKafkaTopicCreate(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName, mock.MatchedBy(func(in *kafkatopic.ServiceKafkaTopicCreateIn) bool {
				return *in.Partitions == topic.Spec.Partitions &&
					*in.Replication == topic.Spec.Replication &&
					in.TopicName == topic.GetTopicName()
			})).Return(nil).Once()

		r, res, err := runScenario(t, topic, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{RequeueAfter: requeueTimeout}, res)

		got := &v1alpha1.KafkaTopic{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: topic.Name, Namespace: topic.Namespace}, got))
		require.Equal(t, "1", got.Annotations[processedGenerationAnnotation])
		require.NotContains(t, got.Annotations, instanceIsRunningAnnotation)
	})

	t.Run("Blocks deletion when termination protection is on", func(t *testing.T) {
		topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
		topic.Generation = 1
		topic.Finalizers = []string{instanceDeletionFinalizer}
		now := metav1.Now()
		topic.DeletionTimestamp = &now
		enabled := true
		topic.Spec.TerminationProtection = &enabled

		avn := avngen.NewMockClient(t)
		r, _, err := runScenario(t, topic, avn)
		require.EqualError(t, err, `unable to delete instance: termination protection is on`)

		got := &v1alpha1.KafkaTopic{}
		require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: topic.Name, Namespace: topic.Namespace}, got))
		require.Contains(t, got.Finalizers, instanceDeletionFinalizer)
	})

	t.Run("Deletes KafkaTopic and removes finalizer on deletion", func(t *testing.T) {
		topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
		topic.Generation = 1
		topic.Finalizers = []string{instanceDeletionFinalizer}
		now := metav1.Now()
		topic.DeletionTimestamp = &now
		disabled := false
		topic.Spec.TerminationProtection = &disabled

		avn := avngen.NewMockClient(t)
		avn.EXPECT().
			ServiceKafkaTopicDelete(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName, topic.GetTopicName()).
			Return(nil).Once()

		r, res, err := runScenario(t, topic, avn)
		require.NoError(t, err)
		require.Equal(t, ctrlruntime.Result{}, res)

		got := &v1alpha1.KafkaTopic{}
		err = r.Get(t.Context(), types.NamespacedName{Name: topic.Name, Namespace: topic.Namespace}, got)
		require.True(t, apierrors.IsNotFound(err))
	})
}

func TestKafkaTopicCheckPreconditions(t *testing.T) {
	t.Parallel()

	nodes := func(n int) []service.NodeStateOut {
		out := make([]service.NodeStateOut, 0, n)
		for range n {
			out = append(out, service.NodeStateOut{State: service.NodeStateTypeRunning})
		}
		return out
	}

	for _, tc := range []struct {
		name    string
		svc     *service.ServiceGetOut
		wantErr error
	}{
		{
			name: "operational service with enough nodes",
			svc:  &service.ServiceGetOut{State: service.ServiceStateTypeRunning, NodeStates: nodes(3)},
		},
		{
			name: "rebalancing service is still usable",
			svc:  &service.ServiceGetOut{State: service.ServiceStateTypeRebalancing, NodeStates: nodes(2)},
		},
		{
			// min(len(NodeStates), replication) is 0 when the list is empty, so the
			// node comparison passed vacuously and the topic create went ahead.
			name:    "powered-off service reports no nodes",
			svc:     &service.ServiceGetOut{State: service.ServiceStateTypePoweroff},
			wantErr: errServicePoweredOff,
		},
		{
			name:    "running service that reports no nodes yet",
			svc:     &service.ServiceGetOut{State: service.ServiceStateTypeRunning},
			wantErr: errPreconditionNotMet,
		},
		{
			name:    "rebuilding service",
			svc:     &service.ServiceGetOut{State: service.ServiceStateTypeRebuilding, NodeStates: nodes(3)},
			wantErr: errPreconditionNotMet,
		},
		{
			name:    "not enough nodes running for the replication factor",
			svc:     &service.ServiceGetOut{State: service.ServiceStateTypeRunning, NodeStates: []service.NodeStateOut{{State: service.NodeStateTypeRunning}, {State: service.NodeStateTypeLeaving}}},
			wantErr: errPreconditionNotMet,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
			avn := avngen.NewMockClient(t)
			avn.EXPECT().
				ServiceGet(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
				Return(tc.svc, nil).Once()

			err := (&KafkaTopicController{avnGen: avn}).checkPreconditions(t.Context(), topic)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestKafkaTopicMatchesSpec(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// spec runs first, then the remote topic is derived from the resulting spec, then
		// remote applies the drift under test.
		spec   func(topic *v1alpha1.KafkaTopic)
		remote func(remote *kafkatopic.TopicOut)
		want   bool
	}{
		{name: "identical", want: true},
		{
			name:   "partitions changed on Aiven",
			remote: func(r *kafkatopic.TopicOut) { r.Partitions = 1 },
		},
		{
			name:   "replication changed on Aiven",
			remote: func(r *kafkatopic.TopicOut) { r.Replication = 3 },
		},
		{
			name:   "tag value changed on Aiven",
			remote: func(r *kafkatopic.TopicOut) { r.Tags[0].Value = "prod" },
		},
		{
			name:   "tag added on Aiven",
			remote: func(r *kafkatopic.TopicOut) { r.Tags = append(r.Tags, kafkatopic.TagOut{Key: "extra", Value: "x"}) },
		},
		{
			name:   "tag removed on Aiven",
			remote: func(r *kafkatopic.TopicOut) { r.Tags = nil },
		},
		{
			name:   "retention_bytes changed on Aiven",
			remote: func(r *kafkatopic.TopicOut) { r.RetentionBytes = 999 },
		},
		{
			name:   "cleanup_policy changed on Aiven",
			remote: func(r *kafkatopic.TopicOut) { r.CleanupPolicy = "compact" },
		},
		{
			name:   "min_insync_replicas changed on Aiven",
			spec:   func(topic *v1alpha1.KafkaTopic) { topic.Spec.Config.MinInsyncReplicas = anyPointerTo(2) },
			remote: func(r *kafkatopic.TopicOut) { r.MinInsyncReplicas = 1 },
		},
		{
			name:   "remote_storage_enable changed on Aiven",
			spec:   func(topic *v1alpha1.KafkaTopic) { topic.Spec.Config.RemoteStorageEnable = anyPointerTo(true) },
			remote: func(r *kafkatopic.TopicOut) { r.RemoteStorageEnable = anyPointerTo(false) },
		},
		{
			// The update payload omits keys the spec does not set, so reporting drift on
			// one of them would never converge.
			name:   "config key the spec does not set",
			spec:   func(topic *v1alpha1.KafkaTopic) { topic.Spec.Config.MinInsyncReplicas = nil },
			remote: func(r *kafkatopic.TopicOut) { r.MinInsyncReplicas = 2 },
			want:   true,
		},
		{
			name:   "spec sets no config at all",
			spec:   func(topic *v1alpha1.KafkaTopic) { topic.Spec.Config = nil },
			remote: func(r *kafkatopic.TopicOut) { r.RetentionBytes = 999 },
			want:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
			if tc.spec != nil {
				tc.spec(topic)
			}

			remote := topicMatchingSpec(topic, kafkatopic.TopicStateTypeActive)
			require.True(t, topicMatchesSpec(topic, remote), "the derived remote topic must match before drift is applied")

			if tc.remote != nil {
				tc.remote(&remote)
			}

			require.Equal(t, tc.want, topicMatchesSpec(topic, remote))
		})
	}
}

func anyPointerTo[T any](v T) *T { return &v }

// Topics in one Kafka service share a single ServiceKafkaTopicList call. Its response is handed
// to every reconcile coalesced onto that key, so the call must not carry the cancellation of
// whichever reconcile happened to win the race: otherwise one cancelled reconcile fails all the
// others, up to MaxConcurrentReconciles of them.
func TestKafkaTopicObserveDetachesSharedListFromCaller(t *testing.T) {
	t.Parallel()

	topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
	topic.Spec.Project = "test-project-detached"
	topic.Spec.ServiceName = "test-service-detached"

	avn := avngen.NewMockClient(t)
	avn.EXPECT().
		ServiceGet(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
		Return(&service.ServiceGetOut{
			State:      service.ServiceStateTypeRunning,
			NodeStates: []service.NodeStateOut{{State: service.NodeStateTypeRunning}, {State: service.NodeStateTypeRunning}},
		}, nil).Once()

	ctx, cancel := context.WithCancel(t.Context())
	avn.EXPECT().
		ServiceKafkaTopicList(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
		RunAndReturn(func(callCtx context.Context, _, _ string) ([]kafkatopic.TopicOut, error) {
			cancel() // the reconcile that owns this call goes away mid-flight

			assert.NoError(t, callCtx.Err(), "the shared call inherited the owning reconcile's cancellation")

			deadline, ok := callCtx.Deadline()
			assert.True(t, ok, "the detached call has no deadline of its own")
			assert.WithinDuration(t, time.Now().Add(kafkaTopicListTimeout), deadline, time.Minute)

			return []kafkatopic.TopicOut{topicMatchingSpec(topic, kafkatopic.TopicStateTypeActive)}, nil
		}).Once()

	// This caller was the one cancelled, so it still bails out; the point is that the call it
	// was carrying completed for everyone else.
	_, err := (&KafkaTopicController{avnGen: avn}).Observe(ctx, topic)
	require.ErrorIs(t, err, context.Canceled)
}

// A reconcile whose own context is done must not act on the shared response.
func TestKafkaTopicObserveStopsOnOwnCancellation(t *testing.T) {
	t.Parallel()

	topic := newObjectFromYAML[v1alpha1.KafkaTopic](t, yamlKafkaTopic)
	topic.Spec.Project = "test-project-self-cancel"
	topic.Spec.ServiceName = "test-service-self-cancel"

	avn := avngen.NewMockClient(t)
	avn.EXPECT().
		ServiceGet(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
		Return(&service.ServiceGetOut{
			State:      service.ServiceStateTypeRunning,
			NodeStates: []service.NodeStateOut{{State: service.NodeStateTypeRunning}, {State: service.NodeStateTypeRunning}},
		}, nil).Once()

	ctx, cancel := context.WithCancel(t.Context())
	avn.EXPECT().
		ServiceKafkaTopicList(mock.Anything, topic.Spec.Project, topic.Spec.ServiceName).
		RunAndReturn(func(context.Context, string, string) ([]kafkatopic.TopicOut, error) {
			cancel() // cancelled while the call is in flight
			return []kafkatopic.TopicOut{topicMatchingSpec(topic, kafkatopic.TopicStateTypeActive)}, nil
		}).Once()

	_, err := (&KafkaTopicController{avnGen: avn}).Observe(ctx, topic)
	require.ErrorIs(t, err, context.Canceled)
}
