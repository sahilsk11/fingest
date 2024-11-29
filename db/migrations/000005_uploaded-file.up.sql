create table uploaded_file (
  uploaded_file_id uuid primary key,
  uploaded_by_user_id uuid not null,
  file_name text not null,
  s3_bucket text not null,
  s3_file_path text not null,
  file_size_kb int not null,
  created_at timestamp not null
);