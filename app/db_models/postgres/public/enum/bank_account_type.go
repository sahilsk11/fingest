//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package enum

import "github.com/go-jet/jet/v2/postgres"

var BankAccountType = &struct {
	Checking    postgres.StringExpression
	Savings     postgres.StringExpression
	MoneyMarket postgres.StringExpression
	Other       postgres.StringExpression
}{
	Checking:    postgres.NewEnumValue("checking"),
	Savings:     postgres.NewEnumValue("savings"),
	MoneyMarket: postgres.NewEnumValue("money_market"),
	Other:       postgres.NewEnumValue("other"),
}
