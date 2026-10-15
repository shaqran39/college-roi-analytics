-- Turns the NEWEST CSV in the landing volume into the raw table, whatever the file is called.
-- Runs automatically as Task 1 of the Databricks Job when a new CSV is uploaded,
-- or by hand: SQL Editor -> paste -> Run all.
-- To refresh the data, upload a CSV with a NEW name (e.g. college_major_roi_2026-11.csv):
-- the job's file arrival trigger only fires for new file names, not re-uploads of the same name.
-- Keep every statement free of begin/end blocks: the job's SQL file task splits this file on semicolons.

create schema if not exists workspace.raw;

create volume if not exists workspace.raw.landing;

-- Read every CSV in the folder, then keep only the rows from the most recently modified file.
-- _source_file records which file the rows came from.
create or replace table workspace.raw.college_major_roi as
with all_files as (
  select
    *,
    _metadata.file_path              as _source_file,
    _metadata.file_modification_time as _file_modified_at
  from read_files(
    '/Volumes/workspace/raw/landing/',
    format => 'csv',
    header => true,
    pathGlobFilter => '*.csv'
  )
),

newest_file as (
  select max_by(_source_file, _file_modified_at) as file_path
  from all_files
)

select * except (_file_modified_at), current_timestamp() as _loaded_at
from all_files
where _source_file = (select file_path from newest_file);

-- Check: stop here (with an error) if the load came out empty, so dbt never builds on a broken load.
-- No fixed row count: a new CSV may have more or fewer graduates. dbt's tests check the rest.
select assert_true(count(*) > 0, 'Raw load failed: workspace.raw.college_major_roi is empty. Check the uploaded CSV.') as is_loaded
from workspace.raw.college_major_roi;
