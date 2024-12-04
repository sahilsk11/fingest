-- unsure of the best way to track schemas in SF, so just dump them here so we can track
use database FINGEST;

use schema public;

create table import_run (
  import_run_id VARCHAR(36) not null,
  source_institution VARCHAR(40) not null,
  -- robinhood, schwab, etc
  account_type VARCHAR(30) not null,
  -- bank, brokerage, credit_card, crypto_exchange, other
  data_type VARCHAR(30) not null,
  -- transaction, position, open_lot, balances
  file_source_format VARCHAR(10) not null,
  -- csv, json, pdf, email
  table_name VARCHAR(40) not null,
  -- robinhood_transactions, schwab_transactions, etc
  s3_bucket VARCHAR(20),
  -- bucket name
  s3_path VARCHAR(40),
  -- dir/
  file_name VARCHAR(40),
  -- file.csv
  created_at timestamp not null,
  primary key (import_run_id)
);

create table import_table_registry (
  import_table_registry_id VARCHAR(36) not null,
  table_name VARCHAR(40) not null,
  source_institution VARCHAR(40),
  -- robinhood, schwab, etc
  account_type VARCHAR(40),
  -- bank, brokerage, credit_card, crypto_exchange, other
  data_type VARCHAR(40),
  -- transaction, position, open_lot, balances
  file_source_format VARCHAR(10),
  -- csv, json, pdf, email
  column versioned_normalization_pipeline_id VARCHAR(36),
  primary key (import_table_registry_id)
);

alter table import_run drop column versioned_normalization_pipeline_id;

create table versioned_normalization_pipeline (
  versioned_normalization_pipeline_id VARCHAR(36) not null,
  python_code VARCHAR(10000) not null,
  feedback_or_error VARCHAR(500),
  previous_version_id VARCHAR(36),
  created_at timestamp not null,
  primary key (versioned_normalization_pipeline_id)
);

alter table versioned_normalization_pipeline
add column output_schema_hash varchar(100);

alter table
  import_table_registry
add
  column versioned_normalization_pipeline_id VARCHAR(36);

-- mirrored tables
CREATE TABLE staged_brokerage_account_transaction (
  staged_brokerage_account_transaction_id VARCHAR(36),
  versioned_normalization_pipeline_id VARCHAR(36),
  external_transaction_id VARCHAR(100),
  import_run_id VARCHAR(36) NOT NULL,
  symbol_or_cusip VARCHAR(100),
  price numeric,
  quantity numeric,
  amount numeric,
  side VARCHAR(10),
  transaction_date timestamp,
  description VARCHAR(500),
  primary key (brokerage_account_transaction_id)
);