alter table import_run_state
alter column import_run_state_id drop default;

alter table import_run_state
rename column import_run_state_id to import_run_status_id;

alter table import_run_state
alter column created_at drop default;

alter table import_run_state
alter column updated_at drop default;