alter table uploaded_file 
  alter column uploaded_file_id drop default;

alter table uploaded_file 
  alter column created_at drop default;

alter table uploaded_file 
  drop constraint uploaded_file_user_fk;
