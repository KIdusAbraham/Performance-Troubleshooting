use medcompass
/*
copy out and execute the script in the kill_command column to stop a problematic statistic

use this script to turn off auto update stats on a problematic statistic
just replace the table and stistics names with the data from the query results

--exec sp_autostats '<table_name>', 'OFF','<statistic_name>'

*/

SELECT
DB_NAME(database_id) AS [Database],
        OBJECT_NAME(object_id1) table_name,
        ss.name statistic_name,
time_queued,
in_progress,
		'KILL STATS JOB '+convert(varchar(25),job_id) kill_command,
        case 
		when auto_created = 1 then 'auto_created'
        when user_created = 1 then 'user_created'
		else 'other'
		end as stat_type,
        no_recompute,
        has_filter,
        is_temporary,
        is_incremental,
		bq.*
FROM    sys.dm_exec_background_job_queue bq
JOIN    sys.stats ss ON ss.object_id = bq.object_id1
                        AND ss.stats_id = bq.object_id2
order by  bq.in_progress desc

