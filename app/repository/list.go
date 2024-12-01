package repository

import "github.com/go-jet/jet/v2/postgres"

// some structs for sorting and filtering

type ListDirection string

const (
	Ascending  ListDirection = "ASC"
	Descending ListDirection = "DESC"
)

type SortOption struct {
	Column    postgres.Column
	Direction ListDirection
}

type SortOptions []SortOption

func (s SortOptions) ToSql() []postgres.OrderByClause {
	orderByClause := []postgres.OrderByClause{}
	for _, sortOption := range s {
		if sortOption.Direction == Descending {
			orderByClause = append(orderByClause, sortOption.Column.DESC())
		} else {
			orderByClause = append(orderByClause, sortOption.Column.ASC())
		}
	}
	return orderByClause
}

type FilterOptions []postgres.BoolExpression
