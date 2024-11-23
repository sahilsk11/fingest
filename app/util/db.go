package util

import "database/sql"

func NewConnection(connectionCredentials DatabaseCredentials) (*sql.DB, error) {
	connStr := "postgresql://postgres:postgres@localhost:5441/postgres?sslmode=disable"
	dbConn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	return dbConn, nil
}
