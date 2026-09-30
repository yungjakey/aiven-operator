package chutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryAllDatabasesIncludesBuiltInDatabases(t *testing.T) {
	// Grants on built-in databases such as system or default are valid, so their precondition must see them.
	for _, db := range []string{"default", "system"} {
		require.NotContains(t, queryAllDatabases, "'"+db+"'")
	}
}
