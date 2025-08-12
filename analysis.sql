use database snowhouse_import;
use warehouse snowhouse;
use schema preprod;

select * from snowhouse_import.temptest001030.gs_logs_v
where timestamp between $start_date and $end_date
order by timestamp;


set start_date = '2025-08-07 12:00:00';
set end_date = '2025-08-08 00:00:00';




---------------------------------------------------- Part 1: Experimenting - mostly ad hoc queries -----------
select file_selection_state, clustering_levels, clustering_levels_pre_selection, table_id, event_timestamp, * from snowscience.staging.clustering_state
-- where timestamp >= dateadd('day', -1, current_timestamp());
where timestamp between '2025-05-12' and '2025-05-20'
and table_id = 216625266783554
-- and is_file_defragmentation = false
order by timestamp
limit 10;
-- has table_id
-- eg: 727343003370866

-- desc view snowscience.staging.clustering_jobs;
-- desc view snowscience.staging.clustering_state;

select * from table_etl_v where is_auto_clustering_on limit 10;


-- this table is the same as snowscience.staging.clustering_state: shows file selections
select * from Clustering_state_history_v 
where timestamp >= '2025-05-20' 
limit 10;

select * from Clustering_information_history_v 
where timestamp >= '2025-06-1' 
limit 100;


-- this table shows actual clustering jobs
select count(*) from snowscience.staging.clustering_jobs
-- where timestamp >= dateadd('day', -1, current_timestamp());
where  DS >= '2025-05-20'
and total_duration > 60 * 60 * 1000
limit 10;


-- find file selection jobs in job_etl_v
select
        *
    from job_etl_v
    where
       description like '%clustering_service_select_files%' and 
       created_on > '2025-05-22'
    limit 10;

-- snowscience.staging.clustering_jobs : info on all clustering levels, combined
-- snowscience.staging.clustering_state : separate clustering levels


desc view snowscience.staging.clustering_jobs;
select * from snowscience.staging.clustering_jobs
-- where timestamp >= dateadd('day', -1, current_timestamp());
where ds between '2025-05-12' and '2025-05-20'
-- and table_id = 216625266783554
-- and is_file_defragmentation = false
-- order by timestamp
limit 10;

-- table with a lot of ingestion
set tableid = 2052791698834;
-- table with little ingestion
-- set tableid = 739344320151370;

--------------------------------------------------------------------
--                       Part 2: The Actual Analysis
--------------------------------------------------------------------

set start_date = '2025-05-12 00:00:00';
set end_date = '2025-05-13 00:00:00';

-- create schema temp.abright;

-- get table id's during relevant period
create or replace table temp.abright.table_ids as (select distinct table_id
    from snowscience.staging.clustering_state
    where timestamp between $start_date and $end_date
);

-- get all file selection jobs that actually select batches
create or replace table temp.abright.job_etl_v_filtered as (
    select * from job_etl_v 
    where created_on between $start_date and $end_date and 
        statement_properties = 4096 and 
        description like '%clustering_service_select_files%'
);

-- select count(*) from temp.abright.job_etl_v_filtered; -- 9779300

create or replace table temp.abright.cs_filtered as (
    select target_file_level - 1 as level,
    case   --case bash since dynamic json parsing no work :(
        when level = 0 then parse_json(clustering_levels):"0":avgDepth
        when level = 1 then parse_json(clustering_levels):"1":avgDepth
        when level = 2 then parse_json(clustering_levels):"2":avgDepth
        when level = 3 then parse_json(clustering_levels):"3":avgDepth
        when level = 4 then parse_json(clustering_levels):"4":avgDepth
        when level = 5 then parse_json(clustering_levels):"5":avgDepth
        when level = 6 then parse_json(clustering_levels):"6":avgDepth
        else null
        end as avg_depth,
    case 
        when level = 0 then parse_json(clustering_levels_pre_selection):"0":avgDepth
        when level = 1 then parse_json(clustering_levels_pre_selection):"1":avgDepth
        when level = 2 then parse_json(clustering_levels_pre_selection):"2":avgDepth
        when level = 3 then parse_json(clustering_levels_pre_selection):"3":avgDepth
        when level = 4 then parse_json(clustering_levels_pre_selection):"4":avgDepth
        when level = 5 then parse_json(clustering_levels_pre_selection):"5":avgDepth
        when level = 6 then parse_json(clustering_levels_pre_selection):"6":avgDepth
        else null
        end as prev_avg_depth,
    prev_avg_depth - avg_depth as d_avg_depth,
    * from snowscience.staging.clustering_state cs 
    where timestamp between $start_date and $end_date
); 

-- select count(*) from temp.abright.cs_filtered; -- 1937050
-- select * from temp.abright.cs_filtered limit 10;


create or replace table temp.abright.fs_all2 as (
    select 
        uuid, created_on, 
        to_number(parse_json(jobs.bindings):"2":value) as table_id, 
        cs.table_id table_id2,
        cs.* exclude (table_id, job_uuid), 
        (job_uuid is not null) as batches_selected
    from temp.abright.job_etl_v_filtered jobs left join temp.abright.cs_filtered cs on jobs.uuid = cs.job_uuid
);


-- checks
-- select count(*) from job_etl_v where description like '%clustering_service_select_files%' and created_on between $start_date and $end_date; -- 9779308
-- select count(*) from temp.abright.fs_all2; -- 7851423
-- select count(*) from temp.abright.fs_all where batches_selected; -- 1920855


-- treat timestamp as file selection finish, created_on as file selection start
-- select timestamp, created_on, timestampdiff('MILLISECONDS', timestamp, created_on) a from temp.abright.fs_all where a > 0 limit 100;

-- get file selections, indexed by timestamp per table
create or replace table temp.abright.fs_indexed2 as (
    select
       ROW_NUMBER() OVER (partition by table_id ORDER BY created_on) as i, 
       parse_json(file_selection_state):extremeCounter>0 as extreme, 
       iff(extreme, 1024, 128) as max_batches,
       num_batches >= max_batches as saturated,
       greatest(0, max_batches - num_batches) as unsaturation,
       unsaturation/max_batches as unsaturation_fraction,
       *
    from temp.abright.fs_all2
    order by created_on);



-- select count(*) from temp.abright.fs_indexed2; -- 7851432

-- select * from temp.abright.fs_indexed  where table_id = 377985935286 order by i limit 200;
-- select count(*), table_id from temp.abright.fs_indexed group by table_id order by count(*) desc limit 10;

-- get clustering execution jobs
-- relavant columns: created_on, end_time
create or replace table temp.abright.ce as (
    select
        to_number(parse_json(bindings):"1":value) as table_id, 
        *
    from job_etl_v
    where
       statement_properties = 14336 and
       description like '%recluster execute_only%' and 
       created_on between $start_date and $end_date
);

--connect each clustering execution job to the corresponding file selection job, by finding the most recent FS preceding a CE
create or replace table temp.abright.fs_ce as (
    SELECT fs_uuid, ce_uuid
    FROM 
    (
        SELECT fsi.uuid fs_uuid, ce.uuid ce_uuid, ROW_NUMBER() OVER (partition by ce_uuid ORDER BY fsi.created_on DESC) AS rn
        FROM temp.abright.fs_indexed2 fsi right join temp.abright.ce ce on 
        (
            fsi.batches_selected
            and fsi.table_id = ce.table_id
            and fsi.created_on <= ce.created_on
        )
    )
    WHERE rn = 1
);

-- analyze consecutive pairs of FS's for the same table
create or replace table temp.abright.d_fs as (
    
    -- collect the "end time" of a file selection (the end_time of the last corresponding clustering exeuction job)
    with fs_end_time as (
        select fs_ce.fs_uuid, max(ce.end_time) as end_time from 
        temp.abright.fs_indexed2 fsi join temp.abright.fs_ce fs_ce on fsi.uuid = fs_ce.fs_uuid 
        join temp.abright.ce ce on fs_ce.ce_uuid = ce.uuid
        group by fs_ce.fs_uuid
    ),
    
    -- join file selection data with end time and total length
    fs as (
        select *, timestampdiff('MILLISECOND', fsi.created_on, end_time) as duration_ms 
        from temp.abright.fs_indexed2 fsi left join fs_end_time on fsi.uuid = fs_end_time.fs_uuid
    )
    
    -- join each file selection with the previous file selection, and find time deltas
    -- d_time: the time between the end of the previous FS's clustering jobs, and the start of the next FS
    -- max_time: the time between the start of the previous FS, and the end of the next FS's clustering jobs.
      --  represents the maximum time a newly ingested file could spend before getting clustered.
    select 
        prev.end_time as prev_end_time, prev.saturated as prev_saturated, prev.duration_ms as prev_duration_ms, 
        prev.unsaturation_fraction as prev_unsaturation_fraction, prev.uuid as prev_uuid,
        prev.avg_depth as prev_avg_depth2, prev.prev_avg_depth as prev_prev_avg_depth, prev.d_avg_depth as prev_d_avg_depth,
        fs.timestamp, fs.created_on, fs.table_id, fs.num_batches, fs.saturated, fs.batches_selected,
        fs.avg_depth as avg_depth, fs.prev_avg_depth as prev_avg_depth, fs.d_avg_depth as d_avg_depth,
        fs.uuid as uuid, fs.duration_ms as duration_ms,
        -- a.clustering_levels, a.clustering_levels_pre_selection,
        timestampdiff('MILLISECOND', prev.end_time, fs.created_on) as d_time, 
        timestampdiff('MILLISECOND', prev.timestamp, fs.end_time) as max_time
    from fs join fs prev on fs.i = prev.i + 1 and fs.table_id = prev.table_id
    where prev.batches_selected
    and fs.batches_selected
    and prev.target_file_level = fs.target_file_level
    -- and (prev.is_file_defragmentation = false)
    -- and fs.is_file_defragmentation = false
);

--------------------------------------------------------------

create or replace table temp.abright.freq_table_ids as (
    select table_id
    from temp.abright.d_fs
    group by table_id
    order by count(*) desc
    limit 300
);


-- find 600 tables with the most file registrations (from non-clustering jobs)
create or replace table temp.abright.freq_table_ids2 as (
    -- select table_id from temp.abright.file_events_s 
    select table_id from temp.abright.file_events_non_clustering_s 
    where event_type = 'FILE_REGISTRATION'
    group by table_id
    order by count(*) desc
limit 600);

select * from temp.abright.file_events_s  limit 10;

-------------------------------------------------- graph 1: d_time distribution
select d_time_tier, count(*) from
(
    select case
        when d_time < 0 then 'a. < 0'
        when d_time < 1000 then 'b. [0, 1)'
        when d_time < 2000 then 'c. [1, 2)'
        when d_time < 3000 then 'd. [2, 3)'
        when d_time < 4000 then 'e. [3, 4)'
        when d_time < 5000 then 'f. [4, 5)'
        when d_time < 60000 then 'g. [5, 60)'
        when d_time >= 60000 then 'h. > 60'
        else 'null'
        end as d_time_tier
    from temp.abright.d_fs
    -- where table_id = 1350672726311426   -- a table with a lot of ingestion
    -- and table_id in (select * from temp.abright.freq_table_ids)

)
group by d_time_tier;

-------------------------------------------------- graph 2: unsaturated d_times
select d_time_tier, count(*) from
(
    select case
        when d_time < 500 then 'a. < 0.5'
        when d_time < 1000 then 'b. < 1'
        when d_time < 1500 then 'c. < 1.5'
        when d_time < 2000 then 'd. < 2'
        when d_time < 2500 then 'e. < 2.5'
        when d_time < 3000 then 'f. < 3'
        when d_time >= 3000 then 'h. >= 3'
        else 'null'
        end as d_time_tier
    from temp.abright.d_fs
    where prev_unsaturation_fraction > 0.5
    -- where prev_saturated = false
    and table_id in (select * from temp.abright.freq_table_ids2)

)
group by d_time_tier;
-------------------------------------------------- graph 3: unsaturated max_times when d_time is low
select max_time_tier, count(*) from
(
    select case
        when max_time < 0 then 'a. < 0'
        when max_time < 1000 then 'b. [0, 1)'
        when max_time < 2000 then 'c. [1, 2)'
        when max_time < 5000 then 'd. [2, 5)'
        when max_time < 10000 then 'e. [5, 10)'
        when max_time < 20000 then 'f. [10, 20)'
        when max_time < 30000 then 'g. [20, 30)'
        when max_time < 60000 then 'h. [30, 60)'
        when max_time < 120000 then 'i. [60, 120)'
        when max_time < 300000 then 'j. [120, 300)'
        when max_time < 600000 then 'k. [300, 600)'
        when max_time < 1200000 then 'l. [600, 1200)'
        when max_time < 1800000 then 'm. [1200, 1800)'
        when max_time < 3600000 then 'm. [1800, 3600)'
        when max_time is null then 'z. null'
        else 'o. > 1h'
        end as max_time_tier
    from temp.abright.d_fs
    where prev_unsaturation_fraction > 0.5
    and d_time < 5000
    and table_id in (select * from temp.abright.freq_table_ids2)
)
group by max_time_tier;
-------------------------------------------------- graph 4: unsaturated clustering durations when d_time is low
select duration_tier, count(*) from
(
    select case
        when prev_duration_ms < 0 then 'a. < 0'
        when prev_duration_ms < 1000 then 'b. [0, 1)'
        when prev_duration_ms < 2000 then 'c. [1, 2)'
        when prev_duration_ms < 5000 then 'd. [2, 5)'
        when prev_duration_ms < 10000 then 'e. [5, 10)'
        when prev_duration_ms < 20000 then 'f. [10, 20)'
        when prev_duration_ms < 30000 then 'g. [20, 30)'
        when prev_duration_ms < 60000 then 'h. [30, 60)'
        when prev_duration_ms < 120000 then 'i. [60, 120)'
        when prev_duration_ms < 300000 then 'j. [120, 300)'
        when prev_duration_ms < 600000 then 'k. [300, 600)'
        when prev_duration_ms < 1200000 then 'l. [600, 1200)'
        when prev_duration_ms < 1800000 then 'm. [1200, 1800)'
        when prev_duration_ms < 3600000 then 'm. [1800, 3600)'
        when prev_duration_ms is null then 'z. null'
        else 'o. > 1h'
        end as duration_tier
    from temp.abright.d_fs
    where prev_unsaturation_fraction > 0.5
    and d_time < 5000
    and table_id in (select * from temp.abright.freq_table_ids2)
)
group by duration_tier;

-------------------------------------------------- find mean max_time when d_time is low
select avg(max_time) /1000
from temp.abright.d_fs
where prev_unsaturation_fraction > 0.5
and d_time < 5000
and table_id in (select * from temp.abright.freq_table_ids2)
-- and prev_duration_ms < 1000 * 60 * 30    -- remove outliers
;
-------------------------------------------------- find depth reduction stats prev_d_avg_depth
select avg(avg_depth)
from temp.abright.fs_indexed2
where table_id in (select * from temp.abright.freq_table_ids2)
;

select avg(d_avg_depth/prev_avg_depth)
from temp.abright.fs_indexed2
where prev_avg_depth > 0  -- only one entry violates this; it seems like an anomaly as the target_file_level is incorrect
and table_id in (select * from temp.abright.freq_table_ids2)
;

create or replace table temp.abright.clustering_history as (
    select parse_json(clusteringinformation):average_depth as avg_depth, *
    from Clustering_information_history_v 
    where timestamp between $start_date and $end_date
);


create or replace table temp.abright.clustering_history_grouped as (
    select table_id, avg(avg_depth) as avg_depth, count(*) as cnt 
    from temp.abright.clustering_history
    group by table_id
);

select count(distinct(table_id)) from temp.abright.clustering_history;

-- bucket avg_depth
select avg_depth_tier, count(*) from
(
    select case
        when avg_depth < 1 then 'c. 1'
        when avg_depth < 2 then 'd. 2'
        when avg_depth < 3 then 'e. 3'
        when avg_depth < 5 then 'f. 5'
        when avg_depth < 10 then 'g. 10'
        when avg_depth < 50 then 'h. 50'
        when avg_depth < 100 then 'i. 100'
        when avg_depth < 200 then 'j. 200'
        when avg_depth < 500 then 'k. 500'
        when avg_depth < 1000 then 'l. 1000'
        when avg_depth is null then 'z. null'
        else 'o. > 1000'
        end as avg_depth_tier
    from temp.abright.clustering_history_grouped
    -- from temp.abright.clustering_history
    -- where table_id in (select * from temp.abright.freq_table_ids2)
)
group by avg_depth_tier order by avg_depth_tier;


select avg(avg_depth)
from temp.abright.clustering_history_grouped
-- where table_id in (select * from temp.abright.freq_table_ids2)
; 

------------------------------------------------------    count total FS's and unsaturated FS diffs
select count(*) from temp.abright.fs_indexed2
where table_id in (select * from temp.abright.freq_table_ids2)
;

select count(*) from temp.abright.d_fs2
where prev_unsaturation_fraction > 0.5
and table_id in (select * from temp.abright.freq_table_ids2)
;
------------------------------------------------------    find time til cluster
create or replace table temp.abright.file_events as (
    select a.*
    from event_logging_v a join job_etl_v b
    on a.job_uuid = b.uuid
    where true
    and error_code is null
    and a.account_id in (4,477) // files unregistered by bg jobs have account_id = SNOWFLAKE
    and b.account_id = 477
    and statement_properties = 14336 // 14337 for defrag, 14336 for clustering
    and a.timestamp between $start_date and $end_date
    and b.created_on between $start_date and $end_date
    and event_type in ('FILE_REGISTRATION','FILE_UNREGISTRATION')
);


create or replace table temp.abright.file_events_non_clustering as (
    select a.*
    from event_logging_v a join job_etl_v b
    on a.job_uuid = b.uuid
    where true
    and error_code is null
    and a.account_id in (4,477) // files unregistered by bg jobs have account_id = SNOWFLAKE
    and b.account_id = 477
    and statement_properties <> 14336
    and a.timestamp between $start_date and $end_date
    and b.created_on between $start_date and $end_date
    and event_type in ('FILE_REGISTRATION','FILE_UNREGISTRATION')
);

CREATE OR REPLACE FUNCTION temp.abright.base36_to_int(str VARCHAR)
  RETURNS INT
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  HANDLER = 'addone_py'
AS $$
def addone_py(str):
 return int(str, 36)
$$;

-- parse the registered timestamps
create or replace table temp.abright.file_events_s as (
    select 
        to_timestamp(temp.abright.base36_to_int(split_part(event:fileId, '_', 0))) as registered, 
        event:tableId as table_id, *
    from temp.abright.file_events
);

-- parse the registered timestamps
create or replace table temp.abright.file_events_non_clustering_s as (
    select 
        to_timestamp(temp.abright.base36_to_int(split_part(event:fileId, '_', 0))) as registered, 
        event:tableId as table_id, *
    from temp.abright.file_events_non_clustering
);

select time_to_cluster_tier, count(*) from (
    select timestampdiff('MILLISECONDS', registered, timestamp) as time_to_cluster,
        case
            when time_to_cluster < 0 then 'a. < 0'
            when time_to_cluster < 1000 then 'b. [0, 1)'
            when time_to_cluster < 2000 then 'c. [1, 2)'
            when time_to_cluster < 5000 then 'd. [2, 5)'
            when time_to_cluster < 10000 then 'e. [5, 10)'
            when time_to_cluster < 20000 then 'f. [10, 20)'
            when time_to_cluster < 30000 then 'g. [20, 30)'
            when time_to_cluster < 60000 then 'h. [30, 60)'
            when time_to_cluster < 120000 then 'i. [60, 120)'
            when time_to_cluster < 300000 then 'j. [120, 300)'
            when time_to_cluster < 600000 then 'k. [300, 600)'
            when time_to_cluster < 1200000 then 'l. [600, 1200)'
            when time_to_cluster < 1800000 then 'm. [1200, 1800)'
            when time_to_cluster < 3600000 then 'm. [1800, 3600)'
            when time_to_cluster < 3600000 * 2 then 'n. 2h'
            when time_to_cluster < 3600000 * 5 then 'o. 5h'
            when time_to_cluster < 3600000 * 12 then 'p. 12h'
            when time_to_cluster < 3600000 * 24 then 'q. 24h'
            when time_to_cluster is null then 'z. null'
            else 'r. > 24h'
            end as time_to_cluster_tier
    from temp.abright.file_events_s 
    where event_type = 'FILE_UNREGISTRATION'
    and parse_json(event):level = 0   -- level 0 files only
    and table_id in (select * from temp.abright.freq_table_ids2)
)
group by time_to_cluster_tier;

-- different bucketing (better visualization for larger time_to_cluster)
select time_to_cluster_tier, count(*) from (
    select timestampdiff('MILLISECONDS', registered, timestamp) as time_to_cluster,
        case
            when time_to_cluster < 0 then 'a. < 0'
            when time_to_cluster < 30000 then 'g. [0, 30)'
            when time_to_cluster < 60000 then 'h. [30, 60)'
            when time_to_cluster < 120000 then 'i. [60, 120)'
            when time_to_cluster < 300000 then 'j. [120, 300)'
            when time_to_cluster < 600000 then 'k. [300, 600)'
            when time_to_cluster < 1200000 then 'l. [600, 1200)'
            when time_to_cluster < 1800000 then 'm. [1200, 1800)'
            when time_to_cluster < 3600000 then 'm. [30m, 1h)'
            when time_to_cluster < 3600000 * 2 then 'n. [1h, 2h)'
            when time_to_cluster < 3600000 * 6 then 'o. [2h, 6h)'
            when time_to_cluster < 3600000 * 12 then 'p. [6h, 12h)'
            when time_to_cluster < 3600000 * 24 then 'q. [12h, 24h)'
            when time_to_cluster is null then 'z. null'
            else 'r. > 24h'
            end as time_to_cluster_tier
    from temp.abright.file_events_s 
    where event_type = 'FILE_UNREGISTRATION'
)
group by time_to_cluster_tier;


select avg(time_to_cluster)/1000 from (
    select timestampdiff('MILLISECONDS', registered, timestamp) as time_to_cluster,
    from temp.abright.file_events_s 
    where event_type = 'FILE_UNREGISTRATION'
    and parse_json(event):level = 0   -- level 0 files only
    and table_id in (select * from temp.abright.freq_table_ids2)
)
;




------------------------------------------------------  MISCELLANEOUS -- more ad hoc queries ---------
-- collect some stats
-- select * from 
-- (select count(*) from d_fs) a, (select count(*) from d_fs where d_fs.prev_saturated = true) b


-- select table_id, count(*) from d_fs where d_fs.prev_saturated = false
-- group by table_id

-- select * from fs_indexed, ce where fs_indexed.table_id = ce.table_id limit 10

-- select count(*) from ce where table_id = 1350672729219694
-- select count(*) as c, table_id from fs_indexed group by table_id order by c desc -- max: 5670, table_id = 1350672726311426, 1077925122551898, 1350672729193118, 1350672729219694
-- select count(*) from ce -- 473159   -- same as from fs_ce
-- select count(*) from fs_ce where fs_uuid is null -- 1135




-- look at file selections for a single table, in chronological order (outdated)
-- with indexed as (
--     select
--        -- *, parse_json(cs.clustering_state):target_file_level as target_level
--        ROW_NUMBER() OVER (ORDER BY timestamp) as i, timestamp, clustering_state, clustering_levels_pre_selection, clustering_levels, target_file_level
--     from snowscience.staging.clustering_state as cs
--     where true 
--         and timestamp > '2025-05-18' -- TODO: change timestamp range
--         and table_id = $tableid -- TODO: change table(s)?
--         and target_file_level = 1
--         order by timestamp)

-- select a.timestamp, 
--         -- a.clustering_levels, a.clustering_levels_pre_selection,
--         parse_json(a.clustering_levels):"0":numFiles files_post,
--         parse_json(a.clustering_levels_pre_selection):"0":numFiles files_pre,
--         parse_json(prev.clustering_levels):"0":numFiles prev_files_post,
--         parse_json(prev.clustering_levels_pre_selection):"0":numFiles prev_files_pre,
--         files_pre - prev_files_post as ingested,
--         timestampdiff("SECOND", prev.timestamp, a.timestamp) as d_time,
--         a.target_file_level, a.clustering_levels, a.clustering_levels_pre_selection, *
-- from indexed a join indexed prev on a.i = prev.i + 1;


-- look at past query results
SELECT * FROM TABLE(RESULT_SCAN('01bc8b65-0810-2dfc-0001-dd4b7ad83ee7'));


-- debugging
select * from temp.abright.fs_indexed where parse_json(file_selection_state):depthGrowing = true  limit 10;


select created_on c2, * from temp.abright.d_fs where 
table_id = 493998554432522 and 
 d_time > 60 * 60 * 1000
 order by created_on;

 -- prev end time: 2025-05-12 12:13:47.388
 -- created on: 2025-05-12 13:16:15.996 +0000

 -- previous fs job created on 2025-05-12 12:13:45.208 +0000

 select * from temp.abright.d_fs where 
 d_time is null
 order by created_on;

 select * from temp.abright.fs_indexed2 where uuid = '01bc498c-0000-87bb-0000-0014d790e3e6';

select ce.uuid, fsi.uuid, * from 
        temp.abright.fs_indexed2 fsi join temp.abright.fs_ce fs_ce on fsi.uuid = fs_ce.fs_uuid 
        join temp.abright.ce ce on fs_ce.ce_uuid = ce.uuid
where fsi.table_id = 493998554432522
and timestamp = '2025-05-12 04:15:23.047';
-- fs_uuid: 01bc4dc3-0815-200e-756c-83083a157abb  - created at 18:11:05.454, ends at 2025-05-12 18:11:05.993
-- ce_uuid: 01bc4dc3-0815-2738-756c-830839bcffa3  - created at 14:24

select created_on, timestamp, i from temp.abright.fs_indexed2 where table_id = 493998554432522 and 
created_on >= '2025-05-12 12:12:47' order by created_on;
-- 2025-05-12 10:13:33.713
select created_on from temp.abright.job_etl_v_filtered where description like '%493998554432522%' and created_on between '2025-05-12 14:24:56.094' and '2025-05-12 18:11:05.993';


select created_on, end_time end_time2, * from temp.abright.ce ce 
where ce.table_id = 493998554432522
and created_on < '2025-05-12 10:15:23.047'
order by end_time;


select * from 
        -- temp.abright.fs_indexed2 fsi join temp.abright.fs_ce fs_ce on fsi.uuid = fs_ce.fs_uuid 
        temp.abright.fs_ce fs_ce join temp.abright.ce ce on fs_ce.ce_uuid = ce.uuid
where ce.table_id = 33051882511077274
and ce.created_on between '2025-05-12 14:24:55' and '2025-05-12 18:11:07';


SELECT fs_uuid, ce_uuid, rn, timestamp FROM (
SELECT fsi.uuid fs_uuid, ce.uuid ce_uuid, fsi.timestamp, ROW_NUMBER() OVER (partition by ce_uuid ORDER BY fsi.timestamp DESC) AS rn
        FROM temp.abright.fs_indexed2 fsi right join temp.abright.ce ce on 
        (
            fsi.batches_selected
            and fsi.table_id = ce.table_id
            and fsi.table_id = 33051882511077274
            and fsi.timestamp <= ce.created_on
        ))
        where ce_uuid = '01bc4dc3-0815-2738-756c-830839bcffa3';


create or replace table temp.abright.bad_jobs as (
select * from job_etl_v
where 
       description ilike '%clustering_service_select_files%' and 
       description ilike '%33051882511077274%' and
       created_on between '2025-05-12 14:24:56.094' and '2025-05-12 18:11:05.993');

select * from temp.abright.bad_jobs;
-- 01bc4dc3-0815-2738-756c-830839bcffa3



select * from temp.abright.d_fs where prev_duration_ms > 60 * 60 * 1000;
-- uuid: 01bc4c9d-0910-0a52-0002-bd0172b9d02f
-- end time: 2025-05-12 17:11:23.256 +0000
-- table_id: 3011168731362

-- fs created on 2025-05-12 13:17:28.473
select created_on from temp.abright.fs_indexed2 where uuid = '01bc4c9d-0910-0a52-0002-bd0172b9d02f';

select * from temp.abright.ce where table_id = 3011168731362 and created_on >= '2025-05-12 13:17:28.473'
order by created_on;

select * from temp.abright.ce where timestampdiff('minutes', created_on, end_time) > 60;


-- dur_txn_lock, dur_gs_postexecuting are the execution waiting for lock
-- the first job in the above query takes 4 hours to complete; most of it is spent on dur_txn_lock

select * from gs_snowhouse_view_def.properties limit 10;

select count(*) from temp.abright.fs_indexed
where max_selected_batch_size >= 512 * 1024 * 1024 limit 10;
;

select * from temp.abright.file_events_s limit 10;

select * from temp.abright.clustering_history where table_id = 12261233987693066 order by timestamp limit 10;

select * from temp.abright.ce limit 10;
