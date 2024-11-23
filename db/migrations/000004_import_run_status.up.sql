create table import_run_state (
    import_run_status_id UUID primary key,
    import_run_id UUID NOT NULL,
    status import_run_status,
    created_at timestamp with time zone NOT NULL default now(),
    updated_at timestamp with time zone NOT NULL default now()
);

alter table bank_account_transaction_import_run
drop column status;

alter table brokerage_account_transaction_import_run
drop column status;