package v1alpha1

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

// TestKafkaOptionalFieldsAreImmutableAtSpecLevel guards the generated CRDs against a
// regression that is invisible in the Go types: a `self == oldSelf` rule placed on an
// optional field. The kube-apiserver only evaluates a transition rule when the field is
// present in both the old and the new object, so on an optional field such a rule silently
// permits adding and removing it. Adding or removing any of the fields below repoints the
// resource at a different topic, quota or schema type and orphans the original.
//
// Closing that gap needs a rule on the spec, where has() can be applied to both sides. The
// per-field rule stays alongside it, because the docs generator reads that one to mark the
// field Immutable.
func TestKafkaOptionalFieldsAreImmutableAtSpecLevel(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		crd    string
		fields []string
	}{
		{crd: "aiven.io_kafkatopics.yaml", fields: []string{"topicName"}},
		{crd: "aiven.io_kafkaquotas.yaml", fields: []string{"user", "clientId"}},
		{crd: "aiven.io_kafkaschemas.yaml", fields: []string{"schemaType"}},
	} {
		t.Run(tc.crd, func(t *testing.T) {
			t.Parallel()

			spec := loadSpecSchema(t, tc.crd)
			specRules := validationRules(spec)
			properties, _ := spec["properties"].(map[string]any)
			require.NotEmpty(t, properties)

			for _, field := range tc.fields {
				property, ok := properties[field].(map[string]any)
				require.True(t, ok, "%s is missing from the spec schema", field)

				require.NotContains(t, requiredNames(spec), field,
					"%s became required; this test and the spec-level rule can be simplified", field)

				// The spec-level rule is the one that closes the gap.
				assert.True(t, hasPresenceAwareRule(specRules, field),
					"the spec has no presence-aware immutability rule for %s; got %v", field, specRules)

				// The field-level rule has to stay: the docs generator matches this exact
				// string to mark a field Immutable, so dropping it documents the field as
				// mutable, which is the opposite of what the spec rule enforces.
				assert.Contains(t, validationRules(property), "self == oldSelf",
					"%s lost its field-level rule, so the generated docs no longer mark it Immutable", field)
			}
		})
	}
}

// hasPresenceAwareRule reports whether some spec rule compares the field on both sides and
// requires it to be present in both or absent in both.
func hasPresenceAwareRule(rules []string, field string) bool {
	for _, rule := range rules {
		if strings.Contains(rule, "has(self."+field+")") &&
			strings.Contains(rule, "has(oldSelf."+field+")") &&
			strings.Contains(rule, "self."+field+" == oldSelf."+field) {
			return true
		}
	}
	return false
}

func loadSpecSchema(t *testing.T, name string) map[string]any {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", name))
	require.NoError(t, err)

	var crd struct {
		Spec struct {
			Versions []struct {
				Schema struct {
					OpenAPIV3Schema struct {
						Properties struct {
							Spec map[string]any `json:"spec"`
						} `json:"properties"`
					} `json:"openAPIV3Schema"`
				} `json:"schema"`
			} `json:"versions"`
		} `json:"spec"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &crd))
	require.Len(t, crd.Spec.Versions, 1)

	spec := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties.Spec
	require.NotEmpty(t, spec)
	return spec
}

func validationRules(schema map[string]any) []string {
	entries, _ := schema["x-kubernetes-validations"].([]any)
	rules := make([]string, 0, len(entries))
	for _, entry := range entries {
		if m, ok := entry.(map[string]any); ok {
			if rule, ok := m["rule"].(string); ok {
				rules = append(rules, rule)
			}
		}
	}
	return rules
}

func requiredNames(schema map[string]any) []string {
	entries, _ := schema["required"].([]any)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if s, ok := entry.(string); ok {
			names = append(names, s)
		}
	}
	return names
}
