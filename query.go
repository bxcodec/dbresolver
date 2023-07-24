package dbresolver

import "strings"

type QueryType int

const (
	QueryTypeUnknown QueryType = iota
	QueryTypeRead
	QueryTypeWrite
)

// QueryTypeChecker is used to try to detect the query type, like for detecting RETURNING clauses in
// INSERT/UPDATE clauses.
type QueryTypeChecker interface {
	Check(query string) QueryType
}

// DefaultQueryTypeChecker searches for a "RETURNING" string inside the query to detect a write query.
type DefaultQueryTypeChecker struct {
}

func (c DefaultQueryTypeChecker) Check(query string) QueryType {
	if strings.Contains(strings.ToUpper(query), "RETURNING") {
		return QueryTypeWrite
	}
	return QueryTypeUnknown
}
