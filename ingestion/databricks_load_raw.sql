-- Turns the CSV in the landing volume into the raw table.
-- Runs automatically as Task 1 of the Databricks Job when a new CSV is uploaded,
-- or by hand: SQL Editor -> paste -> Run all.

create schema if not exists workspace.raw;
create volume if not exists workspace.raw.landing;

create or replace table workspace.raw.college_major_roi as
select *, current_timestamp() as _loaded_at
from read_files(
  '/Volumes/workspace/raw/landing/college_major_roi.csv',
  format => 'csv',
  header => true
);

-- Check: stop here (with an error) if the file was empty, so dbt never builds on a broken load.
-- No fixed row count: a new CSV may have more or fewer graduates. dbt's tests check the rest.
select assert_true(count(*) > 0, 'Raw load failed: workspace.raw.college_major_roi is empty. Check the uploaded CSV.') as is_loaded
from workspace.raw.college_major_roi;
