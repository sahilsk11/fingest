alter table import_run_state
rename column import_run_status_id to import_run_state_id;

alter table import_run_state
alter column import_run_state_id set default uuid_generate_v4();

alter table import_run_state
alter column created_at set default now();

alter table import_run_state
alter column updated_at set default now();
