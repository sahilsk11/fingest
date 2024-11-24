//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package model

import "errors"

type BankAccountType string

const (
	BankAccountType_Checking    BankAccountType = "checking"
	BankAccountType_Savings     BankAccountType = "savings"
	BankAccountType_MoneyMarket BankAccountType = "money_market"
	BankAccountType_Other       BankAccountType = "other"
)

func (e *BankAccountType) Scan(value interface{}) error {
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
	case "checking":
		*e = BankAccountType_Checking
	case "savings":
		*e = BankAccountType_Savings
	case "money_market":
		*e = BankAccountType_MoneyMarket
	case "other":
		*e = BankAccountType_Other
	default:
		return errors.New("jet: Invalid scan value '" + enumValue + "' for BankAccountType enum")
	}

	return nil
}

func (e BankAccountType) String() string {
	return string(e)
}