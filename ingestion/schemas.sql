-- unsure of the best way to track schemas in SF, so just dump them here so we can track

use database FINGEST;
use schema public;

create table import_run (
  import_run_id VARCHAR(36) not null,
  category VARCHAR(36) not null,
  created_at timestamp not null,
  error_message string not null,
  origin string not null,
  primary key (import_run_id)
);