-- we could have tables for different types of origins, so like bank_account, bank_account_transaction,
-- brokerage_account, brokerage_account_transaction, exchange_account, exchange_account_transaction,
-- credit_card_account, credit_card_transaction, etc.
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

create table "user" (
    user_id UUID primary key,
    first_name text,
    last_name text,
    email text,
    phone_number text,
    created_at timestamp with time zone NOT NULL
);