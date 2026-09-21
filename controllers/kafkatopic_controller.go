// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/

package controllers

import (
	"context"
	"fmt"
	"maps"
	"time"

	avngen "github.com/aiven/go-client-codegen"
	"github.com/aiven/go-client-codegen/handler/kafkatopic"
	"github.com/aiven/go-client-codegen/handler/service"
	"golang.org/x/sync/singleflight"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	"github.com/aiven/aiven-operator/api/v1alpha1"
)

const kafkaTopicMaxConcurrentReconciles = 20

// kafkaTopicListTimeout bounds the shared topic-list call. The call is detached from any
// single reconcile's context, so it needs a deadline of its own.
const kafkaTopicListTimeout = 30 * time.Second

func newKafkaTopicReconciler(c Controller) reconcilerType {
	return newManagedReconciler(
		c,
		func(c Controller, avnGen avngen.Client) AivenController[*v1alpha1.KafkaTopic] {
			return &KafkaTopicController{
				Client: c.Client,
				avnGen: avnGen,
			}
		},
		&controller.Options{MaxConcurrentReconciles: kafkaTopicMaxConcurrentReconciles},
	)
}

//+kubebuilder:rbac:groups=aiven.io,resources=kafkatopics,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiven.io,resources=kafkatopics/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiven.io,resources=kafkatopics/finalizers,verbs=get;create;update

// KafkaTopicController reconciles a KafkaTopic object
type KafkaTopicController struct {
	client.Client
	avnGen avngen.Client
}

// singleflight group for ServiceKafkaTopicList calls
var topicListCallGroup singleflight.Group

func (r *KafkaTopicController) Observe(ctx context.Context, topic *v1alpha1.KafkaTopic) (Observation, error) {
	if err := r.checkPreconditions(ctx, topic); err != nil {
		return Observation{}, err
	}

	callKey := fmt.Sprintf("%s/%s", topic.Spec.Project, topic.Spec.ServiceName)
	targetTopicName := topic.GetTopicName()

	// let requeuing handle retries
	result, err, _ := topicListCallGroup.Do(callKey, func() (any, error) {
		// The response is handed to every reconcile coalesced onto this key, so the call must
		// not inherit the cancellation of whichever one happened to win the race. Context
		// values, the logger among them, are kept.
		callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), kafkaTopicListTimeout)
		defer cancel()

		return r.avnGen.ServiceKafkaTopicList(callCtx, topic.Spec.Project, topic.Spec.ServiceName)
	})

	// A caller whose own context is done stops here rather than acting on a shared response.
	if err := ctx.Err(); err != nil {
		return Observation{}, err
	}

	switch {
	case isServerError(err):
		// Getting topic info can sometimes temporarily fail with 5xx.
		// Don't treat that as a fatal error but keep on retrying instead.
		// When this happens during a spec update, assume the topic exists if it was applied before.
		return Observation{
			ResourceExists:   wasEverApplied(topic) || hasIsRunningAnnotation(topic),
			ResourceUpToDate: hasLatestGeneration(topic),
		}, nil
	case err != nil:
		return Observation{}, err
	}

	topicList, ok := result.([]kafkatopic.TopicOut)
	if !ok {
		return Observation{}, fmt.Errorf("unexpected result type from ServiceKafkaTopicList") // this should not happen
	}

	for _, topicInfo := range topicList {
		if topicInfo.TopicName != targetTopicName {
			continue
		}

		topic.Status.State = topicInfo.State
		if topic.Status.State == kafkatopic.TopicStateTypeActive {
			markInstanceRunning(topic)
		} else {
			// The topic left ACTIVE, so it is no longer ready for use. Leaving the marker in
			// place would keep IsReadyToUse true and release anything waiting on this topic.
			delete(topic.GetAnnotations(), instanceIsRunningAnnotation)
			meta.SetStatusCondition(&topic.Status.Conditions,
				getRunningCondition(metav1.ConditionFalse, "CheckRunning",
					fmt.Sprintf("Instance is in state %s on Aiven side", topic.Status.State)))
		}

		return Observation{
			ResourceExists:   true,
			ResourceUpToDate: hasLatestGeneration(topic) && topicMatchesSpec(topic, topicInfo),
		}, nil
	}

	// Topic not found in list. Report it as missing.
	return Observation{ResourceExists: false}, nil
}

func (r *KafkaTopicController) Create(ctx context.Context, topic *v1alpha1.KafkaTopic) (CreateResult, error) {
	delete(topic.GetAnnotations(), instanceIsRunningAnnotation)

	tags := make([]kafkatopic.TagIn, 0, len(topic.Spec.Tags))
	for _, t := range topic.Spec.Tags {
		tags = append(tags, kafkatopic.TagIn{
			Key:   t.Key,
			Value: t.Value,
		})
	}

	err := r.avnGen.ServiceKafkaTopicCreate(ctx, topic.Spec.Project, topic.Spec.ServiceName, &kafkatopic.ServiceKafkaTopicCreateIn{
		Partitions:  &topic.Spec.Partitions,
		Replication: &topic.Spec.Replication,
		TopicName:   topic.GetTopicName(),
		Tags:        &tags,
		Config:      convertKafkaTopicConfig(topic),
	})
	if err != nil {
		return CreateResult{}, fmt.Errorf("creating Kafka topic: %w", err)
	}

	const reason = "CreatedOrUpdated"
	meta.SetStatusCondition(&topic.Status.Conditions, getInitializedCondition(reason, "Successfully created or updated the instance in Aiven"))
	meta.SetStatusCondition(&topic.Status.Conditions, getRunningCondition(metav1.ConditionUnknown, reason, "Successfully created or updated the instance in Aiven, status remains unknown"))

	return CreateResult{}, nil
}

func (r *KafkaTopicController) Update(ctx context.Context, topic *v1alpha1.KafkaTopic) (UpdateResult, error) {
	delete(topic.GetAnnotations(), instanceIsRunningAnnotation)

	tags := make([]kafkatopic.TagIn, 0, len(topic.Spec.Tags))
	for _, t := range topic.Spec.Tags {
		tags = append(tags, kafkatopic.TagIn{
			Key:   t.Key,
			Value: t.Value,
		})
	}

	err := r.avnGen.ServiceKafkaTopicUpdate(ctx, topic.Spec.Project, topic.Spec.ServiceName, topic.GetTopicName(),
		&kafkatopic.ServiceKafkaTopicUpdateIn{
			Partitions:  &topic.Spec.Partitions,
			Replication: &topic.Spec.Replication,
			Tags:        &tags,
			Config:      convertKafkaTopicConfig(topic),
		})
	if err != nil {
		return UpdateResult{}, fmt.Errorf("cannot update Kafka Topic: %w", err)
	}

	const reason = "CreatedOrUpdated"
	meta.SetStatusCondition(&topic.Status.Conditions, getInitializedCondition(reason, "Successfully created or updated the instance in Aiven"))
	meta.SetStatusCondition(&topic.Status.Conditions, getRunningCondition(metav1.ConditionUnknown, reason, "Successfully created or updated the instance in Aiven, status remains unknown"))

	return UpdateResult{}, nil
}

func (r *KafkaTopicController) Delete(ctx context.Context, topic *v1alpha1.KafkaTopic) error {
	if fromAnyPointer(topic.Spec.TerminationProtection) {
		return errTerminationProtectionOn
	}

	err := r.avnGen.ServiceKafkaTopicDelete(ctx, topic.Spec.Project, topic.Spec.ServiceName, topic.GetTopicName())
	if err != nil && !isNotFound(err) {
		return err
	}

	return nil
}

func (r *KafkaTopicController) checkPreconditions(ctx context.Context, topic *v1alpha1.KafkaTopic) error {
	// Unlike the other Kafka controllers this one does not use getServiceIfOperational, to avoid
	// paying for include_secrets on every topic: there can be thousands of them per service.
	s, err := r.avnGen.ServiceGet(ctx, topic.Spec.Project, topic.Spec.ServiceName)
	if isNotFound(err) {
		return errPreconditionNotMet
	}
	if err != nil {
		return err
	}

	if err := serviceStateError(s.State, topic.Spec.Project, topic.Spec.ServiceName); err != nil {
		return err
	}

	// A service can be RUNNING and still report no usable nodes, for instance while it is being
	// rebuilt. Guarding this explicitly also keeps the comparison below from passing vacuously.
	if len(s.NodeStates) == 0 {
		return fmt.Errorf("%w: service %s/%s reports no nodes",
			errPreconditionNotMet, topic.Spec.Project, topic.Spec.ServiceName)
	}

	running := 0
	for _, node := range s.NodeStates {
		if node.State == service.NodeStateTypeRunning {
			running++
		}
	}

	// Replication factor requires enough nodes running.
	// But we want to get the backend validation error if the value is too high.
	if running < min(len(s.NodeStates), topic.Spec.Replication) {
		return fmt.Errorf("%w: service %s/%s has %d of %d nodes running",
			errPreconditionNotMet, topic.Spec.Project, topic.Spec.ServiceName, running, len(s.NodeStates))
	}

	return nil
}

// topicMatchesSpec reports whether the topic Aiven returned still matches the spec.
//
// Only what ServiceKafkaTopicList already returns is compared. A full configuration diff would
// need a ServiceKafkaTopicGet per topic on every poll, which is the request pattern that made
// large services slow to reconcile (aiven/aiven-operator#974). Configuration keys outside that
// set are therefore still applied blindly and not checked for drift.
func topicMatchesSpec(topic *v1alpha1.KafkaTopic, remote kafkatopic.TopicOut) bool {
	if remote.Partitions != topic.Spec.Partitions || remote.Replication != topic.Spec.Replication {
		return false
	}

	if !topicTagsMatch(topic.Spec.Tags, remote.Tags) {
		return false
	}

	cfg := topic.Spec.Config
	if cfg == nil {
		return true
	}

	// Only keys the spec actually sets are compared: an unset key is left to Aiven, and the
	// update payload omits it, so reporting drift on it would never converge.
	switch {
	case cfg.CleanupPolicy != "" && string(cfg.CleanupPolicy) != remote.CleanupPolicy:
		return false
	case cfg.MinInsyncReplicas != nil && *cfg.MinInsyncReplicas != remote.MinInsyncReplicas:
		return false
	case cfg.RetentionBytes != nil && *cfg.RetentionBytes != remote.RetentionBytes:
		return false
	case cfg.DisklessEnable != nil && fromAnyPointer(cfg.DisklessEnable) != fromAnyPointer(remote.DisklessEnable):
		return false
	case cfg.RemoteStorageEnable != nil && fromAnyPointer(cfg.RemoteStorageEnable) != fromAnyPointer(remote.RemoteStorageEnable):
		return false
	}

	return true
}

// topicTagsMatch compares spec tags with the tags Aiven reports. Tags are fully managed: the
// update payload always carries the complete set, so an extra tag on Aiven is drift.
func topicTagsMatch(spec []v1alpha1.KafkaTopicTag, remote []kafkatopic.TagOut) bool {
	if len(spec) != len(remote) {
		return false
	}

	want := make(map[string]string, len(spec))
	for _, t := range spec {
		want[t.Key] = t.Value
	}

	got := make(map[string]string, len(remote))
	for _, t := range remote {
		got[t.Key] = t.Value
	}

	return maps.Equal(want, got)
}

func convertKafkaTopicConfig(topic *v1alpha1.KafkaTopic) *kafkatopic.ConfigIn {
	if topic.Spec.Config == nil {
		return nil
	}

	return &kafkatopic.ConfigIn{
		CleanupPolicy:                   topic.Spec.Config.CleanupPolicy,
		CompressionType:                 topic.Spec.Config.CompressionType,
		DeleteRetentionMs:               topic.Spec.Config.DeleteRetentionMs,
		FileDeleteDelayMs:               topic.Spec.Config.FileDeleteDelayMs,
		FlushMessages:                   topic.Spec.Config.FlushMessages,
		FlushMs:                         topic.Spec.Config.FlushMs,
		IndexIntervalBytes:              topic.Spec.Config.IndexIntervalBytes,
		DisklessEnable:                  topic.Spec.Config.DisklessEnable,
		LocalRetentionBytes:             topic.Spec.Config.LocalRetentionBytes,
		LocalRetentionMs:                topic.Spec.Config.LocalRetentionMs,
		MaxCompactionLagMs:              topic.Spec.Config.MaxCompactionLagMs,
		MaxMessageBytes:                 topic.Spec.Config.MaxMessageBytes,
		MessageDownconversionEnable:     topic.Spec.Config.MessageDownconversionEnable,
		MessageFormatVersion:            topic.Spec.Config.MessageFormatVersion,
		MessageTimestampDifferenceMaxMs: topic.Spec.Config.MessageTimestampDifferenceMaxMs,
		MessageTimestampType:            topic.Spec.Config.MessageTimestampType,
		MinCleanableDirtyRatio:          topic.Spec.Config.MinCleanableDirtyRatio,
		MinCompactionLagMs:              topic.Spec.Config.MinCompactionLagMs,
		MinInsyncReplicas:               topic.Spec.Config.MinInsyncReplicas,
		Preallocate:                     topic.Spec.Config.Preallocate,
		RemoteStorageEnable:             topic.Spec.Config.RemoteStorageEnable,
		RetentionBytes:                  topic.Spec.Config.RetentionBytes,
		RetentionMs:                     topic.Spec.Config.RetentionMs,
		SegmentBytes:                    topic.Spec.Config.SegmentBytes,
		SegmentIndexBytes:               topic.Spec.Config.SegmentIndexBytes,
		SegmentJitterMs:                 topic.Spec.Config.SegmentJitterMs,
		SegmentMs:                       topic.Spec.Config.SegmentMs,
		UncleanLeaderElectionEnable:     topic.Spec.Config.UncleanLeaderElectionEnable,
	}
}
