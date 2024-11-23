drop table import_run_state;

alter table bank_account_transaction_import_run
add column status import_run_status;

alter table brokerage_account_transaction_import_run
add column status import_run_status;