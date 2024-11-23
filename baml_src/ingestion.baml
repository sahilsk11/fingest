// baml functions for ingesting data


enum AccountType {
  Bank
  Brokerage
  CreditCard
  CryptoExchange
  Other
}

function CategorizeData(headers: string[], rows: string[][]) -> AccountType {
  client Main

  prompt #"
    Here's the header of a CSV file and one or two rows of data. Use the provided output to figure out the type of account.

    headers:
    {{ headers }}

    rows:
    {{ rows }}

    output_format:
    {{ ctx.output_format }}
  "#
}

test TestName {
  functions [CategorizeData]
  args {
    headers [
      #"
        hello world
      "#,
      #"
        hello world
      "#
    ]
  }
}
