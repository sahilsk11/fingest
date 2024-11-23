// baml functions for ingesting data

enum AccountType {
  BANK
  BROKERAGE @description(#"
    account related to trading securities (i.e. stocks, bonds, etc). buying, selling
    or holding tickers or CUSIPs is indicative of a brokerage account.
  "#)
  CREDIT_CARD
  CRYPTO_EXCHANGE
  OTHER
}

enum DataType {
  TRANSACTION
  POSITION
  OPENLOT
  BALANCES
}

class CategorizeCsvDataResult {
  account_type AccountType
  data_type DataType
}

function CategorizeCsvData(source_institution: string | null,headers: string[], rows: string[][] | float[][]) -> CategorizeCsvDataResult {
  client Main

  prompt #"
    Here's the header of a CSV file and one or two rows of data. Use the
    provided headers and rows to figure out the type of account that this
    data could have come from.

    source institution:
    {{ source_institution }}

    headers:
    {{ headers }}

    rows:
    {{ rows }}

    output_format:
    {{ ctx.output_format }}
  "#
}

test TestName {
  functions [CategorizeCsvData]
  args {
    source_institution "Robinhood"
    headers ["price", "activity_date", "description", "instrument", "settle_date", "amount", "quantity", "process_date", "trans_code"]
    rows [
      [
        "5/31/2024","5/31/2024","5/31/2024","","Interest Payment","INT","","","$0.02"
      ]
    ]
  }
}
