package spanq

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQuery(t *testing.T) {
	sql := Query().
		Select("id", "name", "age", Str("hello")).
		From("users").
		InnerJoin("orders", Eq("users.id", "orders.user_id")).
		LeftOuterJoin("products", Eq("orders.product_id", "products.id")).
		Where(
			And(
				Eq("name", Str("foo")),
				Or(
					Ge("age", 20),
					Le("age", 30),
				),
			),
		).
		SQL()

	want := "SELECT id, name, age, \"hello\" FROM users INNER JOIN orders ON users.id = orders.user_id LEFT JOIN products ON orders.product_id = products.id WHERE name = \"foo\" AND age >= 20 OR age <= 30"

	assert.Equal(t, want, sql)
}
