maybe in snowflake, we have a table for import run

something like, id, date, status (?), error_message, origin (key?)

then we have a table like, patelco csv data? and then another table for patelco pdf data?
this is kinda messy bc we need a new table for each origin. what happens if someone
adds a new format? could we dynamically create tables?

we would need a mapping for the tables then, so something like id, provider_name, provider_type, table_name
but what happens if there's data that matches one of the existing types but the format is different? we could
have a table for the different formats? or we could have a field for the format in the mapping table.

alternatively, a better solution for all of this is to probably keep json blobs in the import run table.
hm this only applies to unstructured data, right? so we'd have a table for unstructured data maybe. and anything
which we get as csv, we'd check if a format exists already? this seems hacky.



okay but within our application, we'll have a postgres db. this needs to be structured and normalized.
we could have tables for different types of origins, so like bank_account, bank_account_transaction,
brokerage_account, brokerage_account_transaction, exchange_account, exchange_account_transaction,
credit_card_account, credit_card_transaction, etc.

so we can develop some sort of import process from snowflake which will import latest data,
but how do we figure out what the latest data is? we need to also account for duplicates.