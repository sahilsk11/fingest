//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package view

import (
	"github.com/go-jet/jet/v2/postgres"
)

var CurrentBrokerageAccountOpenLot = newCurrentBrokerageAccountOpenLotTable("public", "current_brokerage_account_open_lot", "")

type currentBrokerageAccountOpenLotTable struct {
	postgres.Table

	//Columns
	BrokerageAccountOpenLotID postgres.ColumnString
	BrokerageAccountID        postgres.ColumnString
	SymbolOrCusip             postgres.ColumnString
	LotCreationDate           postgres.ColumnTimestampz
	ImportRunID               postgres.ColumnString
	VersionDate               postgres.ColumnTimestampz
	TotalCostBasis            postgres.ColumnFloat
	Quantity                  postgres.ColumnFloat

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type CurrentBrokerageAccountOpenLotTable struct {
	currentBrokerageAccountOpenLotTable

	EXCLUDED currentBrokerageAccountOpenLotTable
}

// AS creates new CurrentBrokerageAccountOpenLotTable with assigned alias
func (a CurrentBrokerageAccountOpenLotTable) AS(alias string) *CurrentBrokerageAccountOpenLotTable {
	return newCurrentBrokerageAccountOpenLotTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new CurrentBrokerageAccountOpenLotTable with assigned schema name
func (a CurrentBrokerageAccountOpenLotTable) FromSchema(schemaName string) *CurrentBrokerageAccountOpenLotTable {
	return newCurrentBrokerageAccountOpenLotTable(schemaName, a.TableName(), a.Alias())
}

func newCurrentBrokerageAccountOpenLotTable(schemaName, tableName, alias string) *CurrentBrokerageAccountOpenLotTable {
	return &CurrentBrokerageAccountOpenLotTable{
		currentBrokerageAccountOpenLotTable: newCurrentBrokerageAccountOpenLotTableImpl(schemaName, tableName, alias),
		EXCLUDED:                            newCurrentBrokerageAccountOpenLotTableImpl("", "excluded", ""),
	}
}

func newCurrentBrokerageAccountOpenLotTableImpl(schemaName, tableName, alias string) currentBrokerageAccountOpenLotTable {
	var (
		BrokerageAccountOpenLotIDColumn = postgres.StringColumn("brokerage_account_open_lot_id")
		BrokerageAccountIDColumn        = postgres.StringColumn("brokerage_account_id")
		SymbolOrCusipColumn             = postgres.StringColumn("symbol_or_cusip")
		LotCreationDateColumn           = postgres.TimestampzColumn("lot_creation_date")
		ImportRunIDColumn               = postgres.StringColumn("import_run_id")
		VersionDateColumn               = postgres.TimestampzColumn("version_date")
		TotalCostBasisColumn            = postgres.FloatColumn("total_cost_basis")
		QuantityColumn                  = postgres.FloatColumn("quantity")
		allColumns                      = postgres.ColumnList{BrokerageAccountOpenLotIDColumn, BrokerageAccountIDColumn, SymbolOrCusipColumn, LotCreationDateColumn, ImportRunIDColumn, VersionDateColumn, TotalCostBasisColumn, QuantityColumn}
		mutableColumns                  = postgres.ColumnList{BrokerageAccountOpenLotIDColumn, BrokerageAccountIDColumn, SymbolOrCusipColumn, LotCreationDateColumn, ImportRunIDColumn, VersionDateColumn, TotalCostBasisColumn, QuantityColumn}
	)

	return currentBrokerageAccountOpenLotTable{
		Table: postgres.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		BrokerageAccountOpenLotID: BrokerageAccountOpenLotIDColumn,
		BrokerageAccountID:        BrokerageAccountIDColumn,
		SymbolOrCusip:             SymbolOrCusipColumn,
		LotCreationDate:           LotCreationDateColumn,
		ImportRunID:               ImportRunIDColumn,
		VersionDate:               VersionDateColumn,
		TotalCostBasis:            TotalCostBasisColumn,
		Quantity:                  QuantityColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
