package domain

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

type ColumnDataType string

const (
	STRING   ColumnDataType = "STRING"
	NUMBER   ColumnDataType = "NUMBER"
	DATE     ColumnDataType = "DATE"
	DATETIME ColumnDataType = "DATETIME"
	BOOLEAN  ColumnDataType = "BOOLEAN"
)

type TransformerOutputColumn struct {
	ColumnName  string         `json:"columnName"`
	DataType    ColumnDataType `json:"dataType"`
	Description string         `json:"description"`
	IsNullable  bool           `json:"isNullable"`
}

type TransformerOutputSchema struct {
	Description string                    `json:"description"`
	Columns     []TransformerOutputColumn `json:"columns"`
}

func (s TransformerOutputSchema) Hash() string {
	b, err := json.Marshal(s.Columns)
	if err != nil {
		return ""
	}
	h := sha256.New()
	h.Write(b)
	return fmt.Sprintf("%x", h.Sum(nil))
}
