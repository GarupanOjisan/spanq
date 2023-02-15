package spanq

import (
	"fmt"
	"strings"
)

type Builder struct {
	table      string
	columns    []string
	conditions []Condition
	groupBy    []string
	orderBy    []string
}

func Query() Builder {
	return Builder{}
}

func (b Builder) Select(columns ...string) Builder {
	return Builder{columns: columns}
}

func (b Builder) From(table string) Builder {
	b.table = table
	return b
}

func (b Builder) Where(condition ...Condition) Builder {
	b.conditions = condition
	return b
}

func (b Builder) SQL() string {
	var clauses []string
	if len(b.columns) > 0 {
		clauses = append(clauses, fmt.Sprintf("SELECT %s", strings.Join(b.columns, ", ")))
	}
	if b.table != "" {
		clauses = append(clauses, fmt.Sprintf("FROM %s", b.table))
	}
	if len(b.conditions) > 0 {
		var conditions []string
		for _, c := range b.conditions {
			conditions = append(conditions, c.SQL())
		}
		clauses = append(clauses, fmt.Sprintf("WHERE %s", strings.Join(conditions, " AND ")))
	}
	if len(b.orderBy) > 0 {
		clauses = append(clauses, fmt.Sprintf("ORDER BY %s", strings.Join(b.orderBy, ", ")))
	}
	if len(b.groupBy) > 0 {
		clauses = append(clauses, fmt.Sprintf("GROUP BY %s", strings.Join(b.groupBy, ", ")))
	}
	return strings.Join(clauses, " ")
}

func (b Builder) GroupBy(columns ...string) Builder {
	b.groupBy = columns
	return b
}

func (b Builder) OrderBy(columns ...string) Builder {
	b.orderBy = columns
	return b
}

type Condition interface {
	SQL() string
}

func And(conditions ...Condition) Condition {
	return and{Conditions: conditions}
}

type and struct {
	Conditions []Condition
}

func (a and) SQL() string {
	var clauses []string
	for _, c := range a.Conditions {
		clauses = append(clauses, c.SQL())
	}
	return fmt.Sprintf("(%s)", strings.Join(clauses, " AND "))
}

func Or(conditions ...Condition) Condition {
	return or{Conditions: conditions}
}

type or struct {
	Conditions []Condition
}

func (o or) SQL() string {
	var clauses []string
	for _, c := range o.Conditions {
		clauses = append(clauses, c.SQL())
	}
	return fmt.Sprintf("(%s)", strings.Join(clauses, " OR "))
}

type Eq struct {
	Col string
	Val interface{}
}

func (e Eq) SQL() string {
	switch v := e.Val.(type) {
	case string:
		return fmt.Sprintf("%s = '%s'", e.Col, v)
	}
	return fmt.Sprintf("%s = %v", e.Col, e.Val)
}

type Neq struct {
	Col string
	Val interface{}
}

func (n Neq) SQL() string {
	switch v := n.Val.(type) {
	case string:
		return fmt.Sprintf("%s != '%s'", n.Col, v)
	}
	return fmt.Sprintf("%s != %v", n.Col, n.Val)
}

type Gt struct {
	Col string
	Val interface{}
}

func (g Gt) SQL() string {
	switch v := g.Val.(type) {
	case string:
		return fmt.Sprintf("%s > '%s'", g.Col, v)
	}
	return fmt.Sprintf("%s > %v", g.Col, g.Val)
}

type Lt struct {
	Col string
	Val interface{}
}

func (l Lt) SQL() string {
	switch v := l.Val.(type) {
	case string:
		return fmt.Sprintf("%s < '%s'", l.Col, v)
	}
	return fmt.Sprintf("%s < %v", l.Col, l.Val)
}

type Gte struct {
	Col string
	Val interface{}
}

func (g Gte) SQL() string {
	switch v := g.Val.(type) {
	case string:
		return fmt.Sprintf("%s >= '%s'", g.Col, v)
	}
	return fmt.Sprintf("%s >= %v", g.Col, g.Val)
}

type Lte struct {
	Col string
	Val interface{}
}

func (l Lte) SQL() string {
	switch v := l.Val.(type) {
	case string:
		return fmt.Sprintf("%s <= '%s'", l.Col, v)
	}
	return fmt.Sprintf("%s <= %v", l.Col, l.Val)
}

type In struct {
	Col string
	Val []interface{}
}

func (i In) SQL() string {
	var values []string
	for _, v := range i.Val {
		switch val := v.(type) {
		case string:
			values = append(values, fmt.Sprintf("'%s'", val))
		default:
			values = append(values, fmt.Sprintf("%v", v))
		}
	}
	return fmt.Sprintf("%s IN (%s)", i.Col, strings.Join(values, ", "))
}
