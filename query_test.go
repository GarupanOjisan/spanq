package spanq

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQuery(t *testing.T) {
	sql := Query().
		Select("id", "name", "age", Str("hello")).
		From("users", As("u")).
		Where(
			And(
				Eq("name", "foo"),
				Or(
					Ge("age", 20),
					Le("age", 30),
				),
			),
		).
		SQL()

	want := "SELECT id, name, age, \"hello\" FROM users AS u WHERE name = foo AND age >= 20 OR age <= 30"

	assert.Equal(t, want, sql)
}
