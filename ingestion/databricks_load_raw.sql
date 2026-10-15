-- Turns the NEWEST CSV in the landing volume into the raw table, whatever the file is called.
-- Runs automatically as Task 1 of the Databricks Job when a new CSV is uploaded,
-- or by hand: SQL Editor -> paste -> Run all.
-- To refresh the data, upload a CSV with a NEW name (e.g. college_major_roi_2026-11.csv):
-- the job's file arrival trigger only fires for new file names, not re-uploads of the same name.

create schema if not exists workspace.raw;
create volume if not exists workspace.raw.landing;

begin
  declare newest_csv string;

  -- Find the most recently modified .csv file. Only file details are read here, not the data.
  -- The fixed schema means "no files" gives null instead of a confusing error.
  set newest_csv = (
    select max_by(path, modificationTime)
    from read_files(
      '/Volumes/workspace/raw/landing/',
      format => 'binaryFile',
      pathGlobFilter => '*.csv',
      schema => 'path string, modificationTime timestamp, length bigint, content binary'
    )
  );

  if newest_csv is null then
    signal sqlstate '22000'
      set message_text = 'Raw load failed: no CSV found in /Volumes/workspace/raw/landing/.';
  end if;

  -- Load that one file. _source_file records which file the rows came from.
  execute immediate
    'create or replace table workspace.raw.college_major_roi as
     select *, current_timestamp() as _loaded_at, :file as _source_file
     from read_files(:file, format => "csv", header => true)'
    using newest_csv as file;
end;

-- Check: stop here (with an error) if the file was empty, so dbt never builds on a broken load.
-- No fixed row count: a new CSV may have more or fewer graduates. dbt's tests check the rest.
select assert_true(count(*) > 0, 'Raw load failed: workspace.raw.college_major_roi is empty. Check the uploaded CSV.') as is_loaded
from workspace.raw.college_major_roi;
