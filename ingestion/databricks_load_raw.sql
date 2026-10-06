-- Run this in Databricks: SQL Editor -> paste -> Run all.
-- It turns the CSV you uploaded to the landing volume into a raw table.

create schema if not exists workspace.raw;
create volume if not exists workspace.raw.landing;

create or replace table workspace.raw.college_major_roi as
select *, current_timestamp() as _loaded_at
from read_files(
  '/Volumes/workspace/raw/landing/college_major_roi.csv',
  format => 'csv',
  header => true
);

-- Check: should be 30,000 rows
select count(*) as row_count from workspace.raw.college_major_roi;
