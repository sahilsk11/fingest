//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package table

import (
	"github.com/go-jet/jet/v2/postgres"
)

var ImportRunState = newImportRunStateTable("public", "import_run_state", "")

type importRunStateTable struct {
	postgres.Table

	//Columns
	ImportRunStateID postgres.ColumnString
	ImportRunID      postgres.ColumnString
	Status           postgres.ColumnString
	CreatedAt        postgres.ColumnTimestampz
	UpdatedAt        postgres.ColumnTimestampz

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type ImportRunStateTable struct {
	importRunStateTable

	EXCLUDED importRunStateTable
}

// AS creates new ImportRunStateTable with assigned alias
func (a ImportRunStateTable) AS(alias string) *ImportRunStateTable {
	return newImportRunStateTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new ImportRunStateTable with assigned schema name
func (a ImportRunStateTable) FromSchema(schemaName string) *ImportRunStateTable {
	return newImportRunStateTable(schemaName, a.TableName(), a.Alias())
}

func newImportRunStateTable(schemaName, tableName, alias string) *ImportRunStateTable {
	return &ImportRunStateTable{
		importRunStateTable: newImportRunStateTableImpl(schemaName, tableName, alias),
		EXCLUDED:            newImportRunStateTableImpl("", "excluded", ""),
	}
}

func newImportRunStateTableImpl(schemaName, tableName, alias string) importRunStateTable {
	var (
		ImportRunStateIDColumn = postgres.StringColumn("import_run_state_id")
		ImportRunIDColumn      = postgres.StringColumn("import_run_id")
		StatusColumn           = postgres.StringColumn("status")
		CreatedAtColumn        = postgres.TimestampzColumn("created_at")
		UpdatedAtColumn        = postgres.TimestampzColumn("updated_at")
		allColumns             = postgres.ColumnList{ImportRunStateIDColumn, ImportRunIDColumn, StatusColumn, CreatedAtColumn, UpdatedAtColumn}
		mutableColumns         = postgres.ColumnList{ImportRunIDColumn, StatusColumn, CreatedAtColumn, UpdatedAtColumn}
	)

	return importRunStateTable{
		Table: postgres.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		ImportRunStateID: ImportRunStateIDColumn,
		ImportRunID:      ImportRunIDColumn,
		Status:           StatusColumn,
		CreatedAt:        CreatedAtColumn,
		UpdatedAt:        UpdatedAtColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
