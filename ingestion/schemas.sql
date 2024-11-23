-- unsure of the best way to track schemas in SF, so just dump them here so we can track

use database FINGEST;
use schema public;

create table import_run (
  import_run_id VARCHAR(36) not null,
  file_source_format VARCHAR(10), -- csv, json, pdf, email
  category VARCHAR(40) not null,
  source_institution VARCHAR(20), -- robinhood, schwab, etc

  s3_bucket VARCHAR(20), -- bucket name
  s3_path VARCHAR(20), -- dir/
  file_name VARCHAR(20), -- file.csv
  
  created_at timestamp not null,
  primary key (import_run_id)
);

create table import_table_registry (
  import_table_registry_id VARCHAR(36) not null,
  table_name VARCHAR(20) not null,

  source_institution VARCHAR(20), -- robinhood, schwab, etc
  category VARCHAR(40) not null,
  
  primary key (import_table_registry_id),
  unique (table_name)
);