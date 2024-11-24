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

var BrokerageAccountOpenLotVersion = newBrokerageAccountOpenLotVersionTable("public", "brokerage_account_open_lot_version", "")

type brokerageAccountOpenLotVersionTable struct {
	postgres.Table

	//Columns
	BrokerageAccountOpenLotVersionID postgres.ColumnString
	BrokerageAccountOpenLotID        postgres.ColumnString
	VersionDate                      postgres.ColumnTimestampz
	ImportRunID                      postgres.ColumnString
	TotalCostBasis                   postgres.ColumnFloat
	Quantity                         postgres.ColumnFloat
	CreatedAt                        postgres.ColumnTimestamp

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type BrokerageAccountOpenLotVersionTable struct {
	brokerageAccountOpenLotVersionTable

	EXCLUDED brokerageAccountOpenLotVersionTable
}

// AS creates new BrokerageAccountOpenLotVersionTable with assigned alias
func (a BrokerageAccountOpenLotVersionTable) AS(alias string) *BrokerageAccountOpenLotVersionTable {
	return newBrokerageAccountOpenLotVersionTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new BrokerageAccountOpenLotVersionTable with assigned schema name
func (a BrokerageAccountOpenLotVersionTable) FromSchema(schemaName string) *BrokerageAccountOpenLotVersionTable {
	return newBrokerageAccountOpenLotVersionTable(schemaName, a.TableName(), a.Alias())
}

func newBrokerageAccountOpenLotVersionTable(schemaName, tableName, alias string) *BrokerageAccountOpenLotVersionTable {
	return &BrokerageAccountOpenLotVersionTable{
		brokerageAccountOpenLotVersionTable: newBrokerageAccountOpenLotVersionTableImpl(schemaName, tableName, alias),
		EXCLUDED:                            newBrokerageAccountOpenLotVersionTableImpl("", "excluded", ""),
	}
}

func newBrokerageAccountOpenLotVersionTableImpl(schemaName, tableName, alias string) brokerageAccountOpenLotVersionTable {
	var (
		BrokerageAccountOpenLotVersionIDColumn = postgres.StringColumn("brokerage_account_open_lot_version_id")
		BrokerageAccountOpenLotIDColumn        = postgres.StringColumn("brokerage_account_open_lot_id")
		VersionDateColumn                      = postgres.TimestampzColumn("version_date")
		ImportRunIDColumn                      = postgres.StringColumn("import_run_id")
		TotalCostBasisColumn                   = postgres.FloatColumn("total_cost_basis")
		QuantityColumn                         = postgres.FloatColumn("quantity")
		CreatedAtColumn                        = postgres.TimestampColumn("created_at")
		allColumns                             = postgres.ColumnList{BrokerageAccountOpenLotVersionIDColumn, BrokerageAccountOpenLotIDColumn, VersionDateColumn, ImportRunIDColumn, TotalCostBasisColumn, QuantityColumn, CreatedAtColumn}
		mutableColumns                         = postgres.ColumnList{BrokerageAccountOpenLotIDColumn, VersionDateColumn, ImportRunIDColumn, TotalCostBasisColumn, QuantityColumn, CreatedAtColumn}
	)

	return brokerageAccountOpenLotVersionTable{
		Table: postgres.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		BrokerageAccountOpenLotVersionID: BrokerageAccountOpenLotVersionIDColumn,
		BrokerageAccountOpenLotID:        BrokerageAccountOpenLotIDColumn,
		VersionDate:                      VersionDateColumn,
		ImportRunID:                      ImportRunIDColumn,
		TotalCostBasis:                   TotalCostBasisColumn,
		Quantity:                         QuantityColumn,
		CreatedAt:                        CreatedAtColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}