alter table import_run_state
drop column status;

drop type import_run_status;

alter table import_run_state
add column status text not null default '';

alter table import_run_state
alter column status drop default;

-- rename the table
ALTER TABLE import_run_state RENAME TO import_run_status;

alter table import_run_status
rename column import_run_state_id to import_run_status_id;

alter table import_run_status
add column description text;