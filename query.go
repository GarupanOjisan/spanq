package spanq

import (
	"cloud.google.com/go/spanner/spansql"
)

type QueryBuilder struct {
	sel spansql.Select
}

func Query() QueryBuilder {
	return QueryBuilder{sel: spansql.Select{}}
}

type Expr interface {
	toSpanSQLExpr() spansql.Expr
}

type ID string

func (i ID) toSpanSQLExpr() spansql.Expr {
	return spansql.ID(i)
}

type Str string

func (s Str) toSpanSQLExpr() spansql.Expr {
	return spansql.StringLiteral(s)
}

func toSpanSQLExpr(v interface{}) spansql.Expr {
	switch t := v.(type) {
	case Expr:
		return t.toSpanSQLExpr()
	case string:
		return spansql.ID(t)
	case int64:
		return spansql.IntegerLiteral(t)
	case int:
		return spansql.IntegerLiteral(int64(t))
	}
	return nil
}

func (q QueryBuilder) Select(cols ...interface{}) QueryBuilder {
	list := make([]spansql.Expr, 0, len(cols))
	for _, c := range cols {
		list = append(list, toSpanSQLExpr(c))
	}
	q.sel.List = list
	return q
}

type FromOption interface {
	toSelectFromTableArg() interface{}
}

type FromOptionAs struct {
	Alias string
}

func (f FromOptionAs) toSelectFromTableArg() interface{} {
	return f.Alias
}

func As(alias string) FromOption {
	return FromOptionAs{Alias: alias}
}

type FromOptionHint struct {
	Key   string
	Value string
}

func (f FromOptionHint) toSelectFromTableArg() interface{} {
	return map[string]string{
		f.Key: f.Value,
	}
}

func (q QueryBuilder) From(table string, opts ...FromOption) QueryBuilder {
	var alias spansql.ID
	var hints map[string]string
	for _, opt := range opts {
		switch v := opt.(type) {
		case FromOptionAs:
			alias = spansql.ID(v.toSelectFromTableArg().(string))
		case FromOptionHint:
			hint := v.toSelectFromTableArg().(map[string]string)
			for key, value := range hint {
				hints[key] = value
			}
		}
	}

	q.sel.From = append(q.sel.From, spansql.SelectFromTable{
		Table: spansql.ID(table),
		Alias: alias,
		Hints: hints,
	})
	return q
}

func And(lhs, rhs spansql.BoolExpr) spansql.BoolExpr {
	return spansql.LogicalOp{
		Op:  spansql.And,
		LHS: lhs,
		RHS: rhs,
	}
}

func Or(lhs, rhs spansql.BoolExpr) spansql.BoolExpr {
	return spansql.LogicalOp{
		Op:  spansql.Or,
		LHS: lhs,
		RHS: rhs,
	}
}

func Eq(lhs, rhs interface{}) spansql.BoolExpr {
	return spansql.ComparisonOp{
		Op:  spansql.Eq,
		LHS: toSpanSQLExpr(lhs),
		RHS: toSpanSQLExpr(rhs),
	}
}

func Ne(lhs, rhs interface{}) spansql.BoolExpr {
	return spansql.ComparisonOp{
		Op:  spansql.Ne,
		LHS: toSpanSQLExpr(lhs),
		RHS: toSpanSQLExpr(rhs),
	}
}

func Lt(lhs, rhs interface{}) spansql.BoolExpr {
	return spansql.ComparisonOp{
		Op:  spansql.Lt,
		LHS: toSpanSQLExpr(lhs),
		RHS: toSpanSQLExpr(rhs),
	}
}

func Le(lhs, rhs interface{}) spansql.BoolExpr {
	return spansql.ComparisonOp{
		Op:  spansql.Le,
		LHS: toSpanSQLExpr(lhs),
		RHS: toSpanSQLExpr(rhs),
	}
}

func Gt(lhs, rhs interface{}) spansql.BoolExpr {
	return spansql.ComparisonOp{
		Op:  spansql.Gt,
		LHS: toSpanSQLExpr(lhs),
		RHS: toSpanSQLExpr(rhs),
	}
}

func Ge(lhs, rhs interface{}) spansql.BoolExpr {
	return spansql.ComparisonOp{
		Op:  spansql.Ge,
		LHS: toSpanSQLExpr(lhs),
		RHS: toSpanSQLExpr(rhs),
	}
}

func Like(lhs, rhs interface{}) spansql.BoolExpr {
	return spansql.ComparisonOp{
		Op:  spansql.Like,
		LHS: toSpanSQLExpr(lhs),
		RHS: toSpanSQLExpr(rhs),
	}
}

func NotLike(lhs, rhs interface{}) spansql.BoolExpr {
	return spansql.ComparisonOp{
		Op:  spansql.NotLike,
		LHS: toSpanSQLExpr(lhs),
		RHS: toSpanSQLExpr(rhs),
	}
}

func Between(lhs, rhs1, rhs2 interface{}) spansql.BoolExpr {
	return spansql.ComparisonOp{
		Op:   spansql.Between,
		LHS:  toSpanSQLExpr(lhs),
		RHS:  toSpanSQLExpr(rhs1),
		RHS2: toSpanSQLExpr(rhs2),
	}
}

func NotBetween(lhs, rhs1, rhs2 interface{}) spansql.BoolExpr {
	return spansql.ComparisonOp{
		Op:   spansql.NotBetween,
		LHS:  toSpanSQLExpr(lhs),
		RHS:  toSpanSQLExpr(rhs1),
		RHS2: toSpanSQLExpr(rhs2),
	}
}

func (q QueryBuilder) Where(expr spansql.BoolExpr) QueryBuilder {
	q.sel.Where = expr
	return q
}

func (q QueryBuilder) GroupBy(expr ...spansql.Expr) QueryBuilder {
	q.sel.GroupBy = expr
	return q
}

func (q QueryBuilder) SQL() string {
	return q.sel.SQL()
}
