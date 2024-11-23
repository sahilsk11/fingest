-- create type brokerage_account_type as enum ('individual', 'joint', 'retirement', 'other');
CREATE TABLE brokerage_account (
    brokerage_account_id UUID primary key,
    user_id UUID NOT NULL references "user"(user_id),
    source_institution text not null,
    account_name text,
    -- account_type brokerage_account_type,
    created_at timestamp with time zone NOT NULL default now(),
    updated_at timestamp with time zone NOT NULL default now()
);

create type side as enum ('buy', 'sell');

CREATE TABLE brokerage_account_transaction (
    brokerage_account_transaction_id UUID primary key,
    external_transaction_id text,
    import_run_id UUID NOT NULL,
    symbol_or_cusip text,
    price decimal,
    quantity decimal,
    amount decimal,
    side side,
    transaction_date timestamp with time zone,
    description text
);

create table brokerage_account_transaction_import_run (
    brokerage_account_import_run_id UUID primary key,
    brokerage_account_id UUID NOT NULL references brokerage_account(brokerage_account_id),
    import_run_id uuid not null,
    status import_run_status not null,
    created_at timestamp with time zone NOT NULL default now()
);

create table brokerage_account_open_lot(
    brokerage_account_open_lot_id UUID primary key,
    brokerage_account_id UUID NOT NULL references brokerage_account(brokerage_account_id),
    symbol_or_cusip text,
    lot_creation_date timestamp with time zone,
    created_at timestamp NOT NULL default now()
);

create table brokerage_account_open_lot_version (
    brokerage_account_open_lot_version_id UUID primary key,
    brokerage_account_open_lot_id UUID NOT NULL references brokerage_account_open_lot(brokerage_account_open_lot_id),
    version_date timestamp with time zone not null,
    import_run_id UUID NOT NULL,
    total_cost_basis decimal,
    quantity decimal,
    created_at timestamp NOT NULL default now()
);

CREATE VIEW current_brokerage_account_open_lot AS WITH ranked_lots AS (
    SELECT
        ol.brokerage_account_open_lot_id,
        ol.brokerage_account_id,
        ol.symbol_or_cusip,
        ol.lot_creation_date,
        ol.created_at,
        olv.version_date,
        olv.total_cost_basis,
        olv.quantity,
        olv.import_run_id,
        ROW_NUMBER() OVER (
            PARTITION BY ol.brokerage_account_open_lot_id
            ORDER BY
                olv.version_date desc
        ) AS row_num
    FROM
        brokerage_account_open_lot ol
        JOIN brokerage_account_open_lot_version olv ON ol.brokerage_account_open_lot_id = olv.brokerage_account_open_lot_id
)
SELECT
    r.brokerage_account_open_lot_id,
    r.brokerage_account_id,
    r.symbol_or_cusip,
    r.lot_creation_date,
    r.import_run_id,
    r.version_date,
    r.total_cost_basis,
    r.quantity
FROM
    ranked_lots r
WHERE
    r.row_num = 1
    AND r.quantity > 0;