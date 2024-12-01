alter table import_run_status
rename column import_run_status_id to import_run_state_id;

alter table import_run_status
drop column description;

ALTER TABLE import_run_status RENAME TO import_run_state;

alter table import_run_state
drop column status;

create type import_run_status as enum ('pending', 'running', 'completed', 'failed');

alter table import_run_state
add column status import_run_status default 'pending';

alter table import_run_state
alter column status drop default;