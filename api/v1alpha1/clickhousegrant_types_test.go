package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/require"

	chUtils "github.com/aiven/aiven-operator/utils/clickhouse"
)

func TestPrivilegeGrantColumnsIgnorePrivilegeCase(t *testing.T) {
	// ClickHouse keywords are case-insensitive: a lowercase privilege must not widen a column grant to the table.
	g := &Grants{PrivilegeGrants: []PrivilegeGrant{{
		Grantees:   []Grantee{{User: "u"}},
		Privileges: []string{"select"},
		Database:   "d",
		Table:      "t",
		Columns:    []string{"c"},
	}}}
	require.Equal(t, []string{"GRANT select(`c`) ON `d`.`t` TO `u` "}, g.BuildStatements(chUtils.GRANT))
}
