use database snowhouse_import;
use warehouse snowhouse;
use schema prod;


---------------------------------------------------- Part 1: Experimenting
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


-- this table is the same as snowscience.staging.clustering_state: shows file selections
select * from Clustering_state_history_v 
where timestamp >= '2025-05-20' 
limit 10;

select * from Clustering_information_history_v 
where timestamp >= '2025-05-20' 
limit 10;


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
       ROW_NUMBER() OVER (partition by table_id ORDER BY timestamp) as i, 
       parse_json(file_selection_state):extremeCounter>0 as extreme, 
       iff(extreme, 1024, 128) as max_batches,
       num_batches >= max_batches as saturated,
       greatest(0, max_batches - num_batches) as unsaturation,
       unsaturation/max_batches as unsaturation_fraction,
       *
    from temp.abright.fs_all2
    order by timestamp);



-- select count(*) from temp.abright.fs_indexed2; -- 7851432

-- select * from temp.abright.fs_indexed  where table_id = 377985935286 order by i limit 200;
-- select count(*), table_id from temp.abright.fs_indexed group by table_id order by count(*) desc limit 10;

-- get clustering execution jobs
-- relavant columns: created_on, end_time
create or replace table temp.abright.d_fs as (
    with ce as (
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
            SELECT fsi.uuid fs_uuid, ce.uuid ce_uuid, ROW_NUMBER() OVER (partition by ce_uuid ORDER BY fsi.timestamp DESC) AS rn
            FROM temp.abright.fs_indexed2 fsi right join ce on 
            (
                fsi.batches_selected
                and fsi.table_id = ce.table_id
                and fsi.timestamp <= ce.created_on
            )
        )
        WHERE rn = 1
    ),
    
    -- collect the "end time" of a file selection (the end_time of the last corresponding clustering exeuction job)
    fs_end_time as (
        select fs_ce.fs_uuid, max(ce.end_time) as end_time from 
        temp.abright.fs_indexed2 fsi join fs_ce on fsi.uuid = fs_ce.fs_uuid join ce on fs_ce.ce_uuid = ce.uuid
        group by fs_ce.fs_uuid
    ),
    
    -- join file selection data with end time and total length
    fs as (
        select *, timestampdiff('MILLISECOND', fsi.timestamp, end_time) as duration_ms 
        from temp.abright.fs_indexed2 fsi left join fs_end_time on fsi.uuid = fs_end_time.fs_uuid
    )
    
    -- join each file selection with the previous file selection, and find time deltas
    -- d_time: the time between the end of the previous FS's clustering jobs, and the start of the next FS
    -- max_time: the time between the start of the previous FS, and the end of the next FS's clustering jobs.
      --  represents the maximum time a newly ingested file could spend before getting clustered.
    -- also, the previous FS must have actually selected files. https://docs.google.com/document/d/16cJ3GHER_RvTk7q6NlyMKxH9HvqDH8mh-_IqlNBuh4o/edit?disco=AAABkEKk5Hs
     
    select 
        prev.end_time as prev_end_time, prev.saturated as prev_saturated, prev.duration_ms as prev_duration_ms, 
        prev.unsaturation_fraction as prev_unsaturation_fraction,
        prev.avg_depth as prev_avg_depth2, prev.prev_avg_depth as prev_prev_avg_depth, prev.d_avg_depth as prev_d_avg_depth,
        fs.timestamp, fs.table_id, fs.num_batches, fs.saturated, fs.batches_selected,
        fs.avg_depth as avg_depth, fs.prev_avg_depth as prev_avg_depth, fs.d_avg_depth as d_avg_depth,
        -- a.clustering_levels, a.clustering_levels_pre_selection,
        timestampdiff('MILLISECOND', prev.end_time, fs.created_on) as d_time, 
        timestampdiff('MILLISECOND', prev.timestamp, fs.end_time) as max_time
    from fs join fs prev on fs.i = prev.i + 1 and fs.table_id = prev.table_id
    where prev.batches_selected
    -- and fs.batches_selected
    -- and (prev.is_file_defragmentation = false)
    -- and fs.is_file_defragmentation = false
);

create or replace table temp.abright.freq_table_ids as (
    select table_id
    from temp.abright.d_fs
    group by table_id
    order by count(*) desc
    limit 300
);

-------------------------------------------------- graph 1: d_time distribution
select d_time_tier, count(*) from
(
    select case
        when d_time < 0 then 'a. < 0'
        when d_time < 1000 then 'b. [0, 1)'
        when d_time < 2000 then 'c. [1, 2)'
        when d_time < 5000 then 'd. [2, 5)'
        when d_time < 10000 then 'e. [5, 10)'
        when d_time < 20000 then 'f. [10, 20)'
        when d_time < 30000 then 'g. [20, 30)'
        when d_time < 60000 then 'h. [30, 60)'
        when d_time < 120000 then 'i. [60, 120)'
        when d_time < 300000 then 'j. [120, 300)'
        when d_time < 600000 then 'k. [300, 600)'
        when d_time < 1200000 then 'l. [600, 1200)'
        when d_time < 1800000 then 'm. [1200, 1800)'
        when d_time < 3600000 then 'm. [1800, 3600)'
        when d_time >= 3600000 then 'n. > 3600'
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
        when d_time < 0 then 'a. < 0'
        when d_time < 1000 then 'b. [0, 1)'
        when d_time < 2000 then 'c. [1, 2)'
        when d_time < 5000 then 'd. [2, 5)'
        when d_time < 10000 then 'e. [5, 10)'
        when d_time < 20000 then 'f. [10, 20)'
        when d_time < 30000 then 'g. [20, 30)'
        when d_time < 60000 then 'h. [30, 60)'
        when d_time < 120000 then 'i. [60, 120)'
        when d_time < 300000 then 'j. [120, 300)'
        when d_time < 600000 then 'k. [300, 600)'
        when d_time < 1200000 then 'l. [600, 1200)'
        when d_time < 1800000 then 'm. [1200, 1800)'
        when d_time < 3600000 then 'm. [1800, 3600)'
        else 'n. > 3600'
        end as d_time_tier
    from temp.abright.d_fs
    where prev_unsaturation_fraction > 0.5
    -- and table_id in (select * from temp.abright.freq_table_ids)

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
    -- and table_id in (select * from temp.abright.freq_table_ids)
    and d_time < 5000
)
group by max_time_tier;
-------------------------------------------------- find mean max_time when d_time is low
select avg(max_time) 
from temp.abright.d_fs
where prev_unsaturation_fraction > 0.5
and d_time < 5000
and table_id in (select * from temp.abright.freq_table_ids)
and max_time is not null;
-------------------------------------------------- find depth reduction stats prev_d_avg_depth
select avg(d_avg_depth/prev_avg_depth)
from temp.abright.fs_indexed2
where prev_avg_depth > 0  -- only one entry violates this; it seems like an anomaly as the target_file_level is incorrect
-- where table_id in (select * from temp.abright.freq_table_ids)

;


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


    
