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

var BankAccountTransactionImportRun = newBankAccountTransactionImportRunTable("public", "bank_account_transaction_import_run", "")

type bankAccountTransactionImportRunTable struct {
	postgres.Table

	//Columns
	BankAccountImportRunID postgres.ColumnString
	BankAccountID          postgres.ColumnString
	ImportRunID            postgres.ColumnString
	CreatedAt              postgres.ColumnTimestamp

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type BankAccountTransactionImportRunTable struct {
	bankAccountTransactionImportRunTable

	EXCLUDED bankAccountTransactionImportRunTable
}

// AS creates new BankAccountTransactionImportRunTable with assigned alias
func (a BankAccountTransactionImportRunTable) AS(alias string) *BankAccountTransactionImportRunTable {
	return newBankAccountTransactionImportRunTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new BankAccountTransactionImportRunTable with assigned schema name
func (a BankAccountTransactionImportRunTable) FromSchema(schemaName string) *BankAccountTransactionImportRunTable {
	return newBankAccountTransactionImportRunTable(schemaName, a.TableName(), a.Alias())
}

func newBankAccountTransactionImportRunTable(schemaName, tableName, alias string) *BankAccountTransactionImportRunTable {
	return &BankAccountTransactionImportRunTable{
		bankAccountTransactionImportRunTable: newBankAccountTransactionImportRunTableImpl(schemaName, tableName, alias),
		EXCLUDED:                             newBankAccountTransactionImportRunTableImpl("", "excluded", ""),
	}
}

func newBankAccountTransactionImportRunTableImpl(schemaName, tableName, alias string) bankAccountTransactionImportRunTable {
	var (
		BankAccountImportRunIDColumn = postgres.StringColumn("bank_account_import_run_id")
		BankAccountIDColumn          = postgres.StringColumn("bank_account_id")
		ImportRunIDColumn            = postgres.StringColumn("import_run_id")
		CreatedAtColumn              = postgres.TimestampColumn("created_at")
		allColumns                   = postgres.ColumnList{BankAccountImportRunIDColumn, BankAccountIDColumn, ImportRunIDColumn, CreatedAtColumn}
		mutableColumns               = postgres.ColumnList{BankAccountIDColumn, ImportRunIDColumn, CreatedAtColumn}
	)

	return bankAccountTransactionImportRunTable{
		Table: postgres.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		BankAccountImportRunID: BankAccountImportRunIDColumn,
		BankAccountID:          BankAccountIDColumn,
		ImportRunID:            ImportRunIDColumn,
		CreatedAt:              CreatedAtColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
