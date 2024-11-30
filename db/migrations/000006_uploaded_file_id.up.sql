alter table uploaded_file 
  alter column uploaded_file_id set default uuid_generate_v4();

alter table uploaded_file
alter column created_at set default now();


alter table uploaded_file
add constraint uploaded_file_user_fk
foreign key (uploaded_by_user_id) references "user"(user_id);