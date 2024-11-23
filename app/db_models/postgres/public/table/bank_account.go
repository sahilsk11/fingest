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

var BankAccount = newBankAccountTable("public", "bank_account", "")

type bankAccountTable struct {
	postgres.Table

	//Columns
	BankAccountID     postgres.ColumnString
	UserID            postgres.ColumnString
	SourceInstitution postgres.ColumnString
	AccountName       postgres.ColumnString
	AccountType       postgres.ColumnString
	CreatedAt         postgres.ColumnTimestamp
	UpdatedAt         postgres.ColumnTimestamp

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type BankAccountTable struct {
	bankAccountTable

	EXCLUDED bankAccountTable
}

// AS creates new BankAccountTable with assigned alias
func (a BankAccountTable) AS(alias string) *BankAccountTable {
	return newBankAccountTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new BankAccountTable with assigned schema name
func (a BankAccountTable) FromSchema(schemaName string) *BankAccountTable {
	return newBankAccountTable(schemaName, a.TableName(), a.Alias())
}

func newBankAccountTable(schemaName, tableName, alias string) *BankAccountTable {
	return &BankAccountTable{
		bankAccountTable: newBankAccountTableImpl(schemaName, tableName, alias),
		EXCLUDED:         newBankAccountTableImpl("", "excluded", ""),
	}
}

func newBankAccountTableImpl(schemaName, tableName, alias string) bankAccountTable {
	var (
		BankAccountIDColumn     = postgres.StringColumn("bank_account_id")
		UserIDColumn            = postgres.StringColumn("user_id")
		SourceInstitutionColumn = postgres.StringColumn("source_institution")
		AccountNameColumn       = postgres.StringColumn("account_name")
		AccountTypeColumn       = postgres.StringColumn("account_type")
		CreatedAtColumn         = postgres.TimestampColumn("created_at")
		UpdatedAtColumn         = postgres.TimestampColumn("updated_at")
		allColumns              = postgres.ColumnList{BankAccountIDColumn, UserIDColumn, SourceInstitutionColumn, AccountNameColumn, AccountTypeColumn, CreatedAtColumn, UpdatedAtColumn}
		mutableColumns          = postgres.ColumnList{UserIDColumn, SourceInstitutionColumn, AccountNameColumn, AccountTypeColumn, CreatedAtColumn, UpdatedAtColumn}
	)

	return bankAccountTable{
		Table: postgres.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		BankAccountID:     BankAccountIDColumn,
		UserID:            UserIDColumn,
		SourceInstitution: SourceInstitutionColumn,
		AccountName:       AccountNameColumn,
		AccountType:       AccountTypeColumn,
		CreatedAt:         CreatedAtColumn,
		UpdatedAt:         UpdatedAtColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
