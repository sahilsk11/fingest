//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package model

import "errors"

type Side string

const (
	Side_Buy  Side = "buy"
	Side_Sell Side = "sell"
)

func (e *Side) Scan(value interface{}) error {
	var enumValue string
	switch val := value.(type) {
	case string:
		enumValue = val
	case []byte:
		enumValue = string(val)
	default:
		return errors.New("jet: Invalid scan value for AllTypesEnum enum. Enum value has to be of type string or []byte")
	}

	switch enumValue {
	case "buy":
		*e = Side_Buy
	case "sell":
		*e = Side_Sell
	default:
		return errors.New("jet: Invalid scan value '" + enumValue + "' for Side enum")
	}

	return nil
}

func (e Side) String() string {
	return string(e)
}
