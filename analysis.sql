use database snowhouse_import;
use warehouse snowadhoc;
use schema prod;

desc view snowscience.staging.clustering_state;

select table_id, * from snowscience.staging.clustering_state
-- where timestamp >= dateadd('day', -1, current_timestamp());
where timestamp between '2025-05-12' and '2025-05-12 00:02:00' and account_id = 477 and table_id = 2052791698834
limit 10;
-- has table_id
-- eg: 727343003370866


-- checking which deployments are in prod
select * from job_etl_v where deployment = 'va3' and created_on > '2025-05-20' limit 5;

desc view snowscience.staging.clustering_jobs;


-- this table is the same as snowscience.staging.clustering_state: shows file selections
select * from Clustering_state_history_v 
where timestamp >= '2025-05-20' 
limit 10;


-- this table shows actual clustering jobs
select * from snowscience.staging.clustering_jobs
-- where timestamp >= dateadd('day', -1, current_timestamp());
where  DS >= '2025-05-20'
    
limit 10;


-- snowscience.staging.clustering_jobs : info on all clustering levels, combined
-- snowscience.staging.clustering_state : separate clustering levels

-- table with a lot of ingestion
set tableid = 2052791698834;
-- table with little ingestion
-- set tableid = 739344320151370;

--------------------------------------------------------------------
--                       Official Analysis
--------------------------------------------------------------------

set start_date = '2025-05-12 00:00:00';
set end_date = '2025-05-13';
-- set end_date = '2025-05-12 01:00:00';


-- get table id's
with table_ids as (
    select distinct table_id
    from snowscience.staging.clustering_state
    where timestamp between $start_date and $end_date),

-- get file selections, indexed by timestamp per table
fs_indexed as (
    select
       ROW_NUMBER() OVER (partition by table_id ORDER BY timestamp) as i, 
       parse_json(file_selection_state):extremeCounter>0 as extreme, 
       not ((extreme and num_batches < 1024) or (not extreme and num_batches < 128)) as saturated,
       *
    from snowscience.staging.clustering_state as cs
    where true 
        and timestamp between $start_date and $end_date
        -- and target_file_level = 1
        order by timestamp),

-- get clustering execution jobs, try to match them up with file selections
-- relavant columns: created_on, end_time
ce as (
    select
        to_number(parse_json(bindings):"1":value) as table_id, 
        *
    from job_etl_v
    where
       statement_properties = 14336 and
       description like '%recluster execute_only%' and 
       created_on between $start_date and $end_date
),

--connect each clustering execution job to the corresponding file selection job, by finding the most recent FS preceding a CE
fs_ce as (
    SELECT fs_uuid, ce_uuid
    FROM 
    (
        SELECT job_uuid fs_uuid, uuid ce_uuid, ROW_NUMBER() OVER (partition by ce_uuid ORDER BY fs.timestamp DESC) AS rn
        FROM fs_indexed fs right join ce on 
        (
            fs.table_id = ce.table_id
            and fs.timestamp <= ce.created_on
        )
    )
    WHERE rn = 1
),

-- collect the "end time" of a file selection (the end_time of the last corresponding clustering exeuction job)
fs_end_time as (
    select fs_ce.fs_uuid, max(ce.end_time) as end_time from 
    fs_indexed fs join fs_ce on fs.job_uuid = fs_ce.fs_uuid join ce on fs_ce.ce_uuid = ce.uuid
    group by fs_ce.fs_uuid
),

-- join file selection data with end time and total length
fs as (
    select *, timestampdiff('MILLISECOND', fs_indexed.timestamp, end_time) as duration_ms 
    from fs_indexed left join fs_end_time on fs_indexed.job_uuid = fs_end_time.fs_uuid
),

-- join each file selection with the previous file selection, and find time deltas
-- d_time: the time between the end of the previous FS's clustering jobs, and the start of the next FS
-- max_time: the time between the start of the previous FS, and the end of the next FS's clustering jobs.
  --  represents the maximum time a newly ingested file could spend before getting clustered.
d_fs as (
    select prev.end_time as prev_end_time, prev.saturated as prev_saturated, prev.duration_ms as prev_duration_ms, 
    fs.timestamp, fs.table_id, fs.num_batches, fs.saturated,
        -- a.clustering_levels, a.clustering_levels_pre_selection,
        timestampdiff('MILLISECOND', prev.end_time, fs.timestamp) as d_time, 
        timestampdiff('MILLISECOND', prev.timestamp, fs.end_time) as max_time
    from fs join fs prev on fs.i = prev.i + 1 and fs.table_id = prev.table_id
)

-- for each table, get percentage of unsaturated FS's, and 

select * from 
(select count(*) from fs) a, (select count(*) from d_fs where d_fs.prev_saturated = false and d_time < 10000 and max_time > 30000) b
-- (select count(*) from fs) a, (select count(*) from ce) b

-- select table_id, count(*) from d_fs where d_fs.prev_saturated = false
-- group by table_id

-- select * from fs_indexed, ce where fs_indexed.table_id = ce.table_id limit 10

-- select count(*) from ce where table_id = 1350672729219694
-- select count(*) as c, table_id from fs_indexed group by table_id order by c desc -- max: 5670, table_id = 1350672726311426, 1077925122551898, 1350672729193118, 1350672729219694
-- select count(*) from ce -- 473159   -- same as from fs_ce
-- select count(*) from fs_ce where fs_uuid is null -- 1135
;



-- look at file selections for a single table, in chronological order
with indexed as (
    select
       -- *, parse_json(cs.clustering_state):target_file_level as target_level
       ROW_NUMBER() OVER (ORDER BY timestamp) as i, timestamp, clustering_state, clustering_levels_pre_selection, clustering_levels, target_file_level
    from snowscience.staging.clustering_state as cs
    where true 
        and timestamp > '2025-05-18' -- TODO: change timestamp range
        and table_id = $tableid -- TODO: change table(s)?
        and target_file_level = 1
        order by timestamp)

select a.timestamp, 
        -- a.clustering_levels, a.clustering_levels_pre_selection,
        parse_json(a.clustering_levels):"0":numFiles files_post,
        parse_json(a.clustering_levels_pre_selection):"0":numFiles files_pre,
        parse_json(prev.clustering_levels):"0":numFiles prev_files_post,
        parse_json(prev.clustering_levels_pre_selection):"0":numFiles prev_files_pre,
        files_pre - prev_files_post as ingested,
        timestampdiff("SECOND", prev.timestamp, a.timestamp) as d_time,
        a.target_file_level, a.clustering_levels, a.clustering_levels_pre_selection, *
from indexed a join indexed prev on a.i = prev.i + 1;




select
   *
from job_etl_v
where true
   and statement_properties = 14336
   and description like '%recluster execute_only%'
   and created_on > '2025-05-20'
   -- and description like '2052791698834'
limit 10;


select * from INGEST_FDN_REGISTRATIONS_V
where timestamp > '2025-05-20'
limit 50;
-- TODO: ask about why not use this?
    


    
