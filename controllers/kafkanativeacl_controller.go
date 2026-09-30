// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/

package controllers

import (
	"context"
	"fmt"

	avngen "github.com/aiven/go-client-codegen"
	"github.com/aiven/go-client-codegen/handler/kafka"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aiven/aiven-operator/api/v1alpha1"
)

//+kubebuilder:rbac:groups=aiven.io,resources=kafkanativeacls,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=aiven.io,resources=kafkanativeacls/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=aiven.io,resources=kafkanativeacls/finalizers,verbs=get;create;update

// KafkaNativeACLController reconciles a KafkaNativeACL object.
type KafkaNativeACLController struct {
	client.Client
	avnGen avngen.Client
}

func newKafkaNativeACLReconciler(c Controller) reconcilerType {
	return newManagedReconciler(
		c,
		func(c Controller, avnGen avngen.Client) AivenController[*v1alpha1.KafkaNativeACL] {
			return &KafkaNativeACLController{Client: c.Client, avnGen: avnGen}
		},
		nil,
	)
}

func (r *KafkaNativeACLController) Observe(ctx context.Context, acl *v1alpha1.KafkaNativeACL) (Observation, error) {
	if _, err := getServiceIfOperational(ctx, r.avnGen, acl.Spec.Project, acl.Spec.ServiceName); err != nil {
		return Observation{}, err
	}

	list, err := r.avnGen.ServiceKafkaNativeAclList(ctx, acl.Spec.Project, acl.Spec.ServiceName)
	if err != nil {
		return Observation{}, fmt.Errorf("list Kafka-native ACLs error: %w", err)
	}

	// Prefer the entry this CR owns over an identical one, e.g. created by another CR.
	var existing *kafka.KafkaAclOut
	for i, a := range list.KafkaAcl {
		if nativeSpecMatches(acl.Spec, a) && (existing == nil || a.Id == acl.Status.ID) {
			existing = &list.KafkaAcl[i]
		}
	}

	if existing == nil {
		return Observation{ResourceExists: false}, nil
	}

	if acl.Status.ID != existing.Id {
		// Adopting an entry this CR did not create.
		logr.FromContextOrDiscard(ctx).Info("adopting existing Kafka-native ACL",
			"aclID", existing.Id, "cachedID", acl.Status.ID)
		acl.Status.ID = existing.Id
	}

	markInstanceRunning(acl)

	// The spec is immutable, so an existing ACL is always up to date.
	return Observation{ResourceExists: true, ResourceUpToDate: true}, nil
}

func (r *KafkaNativeACLController) Create(ctx context.Context, acl *v1alpha1.KafkaNativeACL) (CreateResult, error) {
	delete(acl.GetAnnotations(), instanceIsRunningAnnotation)

	in := &kafka.ServiceKafkaNativeAclAddIn{
		Host:           &acl.Spec.Host,
		Operation:      acl.Spec.Operation,
		PatternType:    acl.Spec.PatternType,
		PermissionType: acl.Spec.PermissionType,
		Principal:      acl.Spec.Principal,
		ResourceName:   acl.Spec.ResourceName,
		ResourceType:   acl.Spec.ResourceType,
	}

	rsp, err := r.avnGen.ServiceKafkaNativeAclAdd(ctx, acl.Spec.Project, acl.Spec.ServiceName, in)
	if err != nil {
		return CreateResult{}, fmt.Errorf("create Kafka-native ACL error: %w", err)
	}

	acl.Status.ID = rsp.Id
	markInstanceRunning(acl)

	return CreateResult{ResourceExists: true, ResourceUpToDate: true}, nil
}

// Update is a no-op: the spec is immutable, so an existing ACL never needs updating.
func (r *KafkaNativeACLController) Update(_ context.Context, acl *v1alpha1.KafkaNativeACL) (UpdateResult, error) {
	markInstanceRunning(acl)
	return UpdateResult{ResourceExists: true, ResourceUpToDate: true}, nil
}

func (r *KafkaNativeACLController) Delete(ctx context.Context, acl *v1alpha1.KafkaNativeACL) error {
	if acl.Status.ID == "" {
		return nil
	}

	var list v1alpha1.KafkaNativeACLList
	if err := r.List(ctx, &list); err != nil {
		return fmt.Errorf("listing KafkaNativeACL resources: %w", err)
	}
	if err := errIfEntryShared(list.Items, acl, func(o *v1alpha1.KafkaNativeACL) bool {
		return o.Spec.Project == acl.Spec.Project && o.Spec.ServiceName == acl.Spec.ServiceName && o.Status.ID == acl.Status.ID
	}); err != nil {
		return err
	}

	err := r.avnGen.ServiceKafkaNativeAclDelete(ctx, acl.Spec.Project, acl.Spec.ServiceName, acl.Status.ID)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("delete Kafka-native ACL error: %w", err)
	}
	return nil
}

// nativeSpecMatches reports whether an existing Kafka-native ACL from Aiven matches the CR
// spec. All immutable identifying fields are compared.
func nativeSpecMatches(spec v1alpha1.KafkaNativeACLSpec, existing kafka.KafkaAclOut) bool {
	return normalizeACLHost(spec.Host) == normalizeACLHost(existing.Host) &&
		spec.Principal == existing.Principal &&
		spec.ResourceName == existing.ResourceName &&
		spec.Operation == existing.Operation &&
		spec.PatternType == existing.PatternType &&
		string(spec.PermissionType) == string(existing.PermissionType) &&
		spec.ResourceType == existing.ResourceType
}

// normalizeACLHost resolves an empty host to the wildcard the spec defaults to.
func normalizeACLHost(host string) string {
	if host == "" {
		return "*"
	}
	return host
}
