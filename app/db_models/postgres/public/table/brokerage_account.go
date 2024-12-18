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

var BrokerageAccount = newBrokerageAccountTable("public", "brokerage_account", "")

type brokerageAccountTable struct {
	postgres.Table

	//Columns
	BrokerageAccountID postgres.ColumnString
	UserID             postgres.ColumnString
	SourceInstitution  postgres.ColumnString
	AccountName        postgres.ColumnString
	CreatedAt          postgres.ColumnTimestampz
	UpdatedAt          postgres.ColumnTimestampz

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type BrokerageAccountTable struct {
	brokerageAccountTable

	EXCLUDED brokerageAccountTable
}

// AS creates new BrokerageAccountTable with assigned alias
func (a BrokerageAccountTable) AS(alias string) *BrokerageAccountTable {
	return newBrokerageAccountTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new BrokerageAccountTable with assigned schema name
func (a BrokerageAccountTable) FromSchema(schemaName string) *BrokerageAccountTable {
	return newBrokerageAccountTable(schemaName, a.TableName(), a.Alias())
}

func newBrokerageAccountTable(schemaName, tableName, alias string) *BrokerageAccountTable {
	return &BrokerageAccountTable{
		brokerageAccountTable: newBrokerageAccountTableImpl(schemaName, tableName, alias),
		EXCLUDED:              newBrokerageAccountTableImpl("", "excluded", ""),
	}
}

func newBrokerageAccountTableImpl(schemaName, tableName, alias string) brokerageAccountTable {
	var (
		BrokerageAccountIDColumn = postgres.StringColumn("brokerage_account_id")
		UserIDColumn             = postgres.StringColumn("user_id")
		SourceInstitutionColumn  = postgres.StringColumn("source_institution")
		AccountNameColumn        = postgres.StringColumn("account_name")
		CreatedAtColumn          = postgres.TimestampzColumn("created_at")
		UpdatedAtColumn          = postgres.TimestampzColumn("updated_at")
		allColumns               = postgres.ColumnList{BrokerageAccountIDColumn, UserIDColumn, SourceInstitutionColumn, AccountNameColumn, CreatedAtColumn, UpdatedAtColumn}
		mutableColumns           = postgres.ColumnList{UserIDColumn, SourceInstitutionColumn, AccountNameColumn, CreatedAtColumn, UpdatedAtColumn}
	)

	return brokerageAccountTable{
		Table: postgres.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		BrokerageAccountID: BrokerageAccountIDColumn,
		UserID:             UserIDColumn,
		SourceInstitution:  SourceInstitutionColumn,
		AccountName:        AccountNameColumn,
		CreatedAt:          CreatedAtColumn,
		UpdatedAt:          UpdatedAtColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
