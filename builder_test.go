package spanq

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQuery(t *testing.T) {
	sql := Query().
		Select("id", "name", "age").
		From("users").
		Where(And(
			Gte{Col: "age", Val: 20},
			Eq{Col: "name", Val: "john"},
		)).
		OrderBy("age DESC", "name").
		SQL()

	want := "SELECT id, name, age FROM users WHERE (age >= 20 AND name = 'john') ORDER BY age DESC, name"

	assert.Equal(t, want, sql)
}
