create type bank_account_type as enum ('checking', 'savings', 'money_market', 'other');

CREATE TABLE bank_account (
    bank_account_id UUID primary key,
    user_id UUID NOT NULL references "user"(user_id),
    source_institution text not null,
    account_name text,
    account_type bank_account_type,
    created_at timestamp NOT NULL default now(),
    updated_at timestamp NOT NULL default now()
);

CREATE TABLE bank_account_transaction (
    bank_account_transaction_id UUID primary key,
    external_transaction_id text,
    import_run_id UUID NOT NULL,
    amount decimal not null,
    category text,
    new_balance decimal,
    description text,
    transaction_date date not null
);

create type import_run_status as enum ('pending', 'running', 'completed', 'failed');

create table bank_account_transaction_import_run (
    bank_account_import_run_id UUID primary key,
    bank_account_id UUID NOT NULL references bank_account(bank_account_id),
    import_run_id uuid not null,
    status import_run_status not null,
    created_at timestamp NOT NULL default now()
);