select @@servername server_name, sqlserver_start_time, getdate() sqlserver_curr_time, physical_memory_kb*1.00/1024/1024 [Memory GB], cpu_count/hyperthread_ratio [Physical CPUs], cpu_count [Cores], virtual_machine_type_desc [VM Type], @@version sql_version
from sys.dm_os_sys_info
go

IF OBJECT_ID(N'tempdb..#temp_sp_who2') IS NOT NULL
BEGIN
DROP TABLE #temp_sp_who2
END

CREATE	TABLE #temp_sp_who2
	(
	SPID		INT, 
	[Status]	VARCHAR(256) NULL, 
	[Login]		SYSNAME NULL, 
	HostName	SYSNAME NULL, 
	BlkBy		SYSNAME NULL, 
	DBName		SYSNAME NULL, 
	Command		VARCHAR(512) NULL, 
	CPUTime		INT NULL, 
	DiskIO		VARCHAR(255),--INT NULL, 
	LastBatch	varchar(256) NULL, 
	ProgramName	VARCHAR(256) NULL, 
	SPID2		INT		,
	RequestID	INT
	)

INSERT	INTO #temp_sp_who2
EXEC	sp_who2

select 'Blocked Session Count' 'Blocked Session Count'
	, count(1)
--SELECT	*
FROM	#temp_sp_who2
where BlkBy <> '  .'
and BlkBy <> SPID

drop table #temp_sp_who2
go

/*
select distinct 'Blocked Session Details' 'Blocked Session Details'
	, a.spid 'blocked spid'
	, b.spid 'first level blocking spid'
	, c.spid 'second level blocking spid'
	, d.spid 'third level blocking spid'
	, e.spid 'fourth level blocking spid'
	,a.* 
from master..sysprocesses a
left join master..sysprocesses b
	on a.blocked = b.spid
left join master..sysprocesses c
	on b.blocked = c.spid
left join master..sysprocesses d
	on c.blocked = d.spid
left join master..sysprocesses e
	on d.blocked = e.spid
where a.blocked > 0
and a.spid <> b.spid
order by a.spid
*/

select 'User Session Count' 'User Session Count'
	, count(1)
from sys.dm_exec_sessions
where is_user_process = 1

select 'User Connection Count' 'User Connection Count'
	, count(1)
from sys.dm_exec_connections

if exists (select 1 from sys.databases where name = 'dbatools')
begin
	if exists (select 1 from dbatools.sys.objects where name = 'sp_whoisactive')
	begin
		exec dbatools..sp_whoisactive @get_plans = 1, @get_additional_info = 1, @get_outer_command = 1
	end
end
go

IF OBJECT_ID(N'tempdb..#temp_active_requests') IS NOT NULL
BEGIN
DROP TABLE #temp_active_requests
END

SELECT 'Active User Request Details' 'Active User Request Details'
	,[Spid] = sp.session_Id
	,er.request_id
	,er.command
	,percent_complete
	,[Database] = DB_NAME(er.database_id)
	,[User] = login_name
	,er.blocking_session_id
	,[Status] = er.status
	,[Wait] = wait_type
	,[Last_Wait] = last_wait_type
	,wait_resource
	,CASE sp.transaction_isolation_level 
		WHEN 0 THEN 'Unspecified' 
		WHEN 1 THEN 'ReadUncommitted' 
		WHEN 2 THEN 'ReadCommitted' 
		WHEN 3 THEN 'Repeatable' 
		WHEN 4 THEN 'Serializable' 
		WHEN 5 THEN 'Snapshot' 
	END AS Transaction_Isolation_Level
	,CAST('<?query --'+CHAR(13)+SUBSTRING(qt.text,
	(er.statement_start_offset / 2)+1,     ((CASE er.statement_end_offset
	WHEN -1 THEN DATALENGTH(qt.text)    ELSE er.statement_end_offset
	END - er.statement_start_offset)/2) + 1)+CHAR(13)+'--?>' AS xml) as sql_statement
	,[Parent Query] = qt.text
	,p.query_plan
	,er.cpu_time
	, er.reads
	, er.writes
	, er.Logical_reads
	, er.row_count
	, Program = program_name
	,Host_name
	,start_time
into #temp_active_requests
FROM sys.dm_exec_requests er INNER JOIN sys.dm_exec_sessions sp
	ON er.session_id = sp.session_id
outer APPLY sys.dm_exec_sql_text(er.sql_handle)as qt
outer APPLY sys.dm_exec_query_plan(er.plan_handle) p
WHERE sp.is_user_process = 1
ORDER BY 1 asc

select 'Active User Request Count' 'Active User Request Count'
	, count(1)
from #temp_active_requests

select *
from #temp_active_requests
order by spid

drop table #temp_active_requests
go



if @@VERSION like '%2019%' or @@VERSION like '%2022%'
begin
	SELECT 'Page Wait Resources'
		, er.session_id
		, er.wait_type
		, er.wait_resource
		, er.wait_time --If the request is currently blocked, this column returns the duration in milliseconds, of the current wait
		, er.percent_complete,
		object_name(page_info.[object_id], page_info.database_id) as [object_name],
		er.blocking_session_id, er.command,
			substring(st.text, (er.statement_start_offset/2)+1,
				((case er.statement_end_offset
					when -1 then Datalength(st.text)
					else er.statement_end_offset
					end - er.statement_start_offset)/2) + 1) as statement_text
		, page_info.[object_id]
		, page_info.database_id 
		, page_info.[file_id]
		, page_info.page_id
		, page_info.index_id
		, page_info.page_type_desc
	from sys.dm_exec_requests as er
		cross apply sys.dm_exec_sql_text(er.sql_handle) as st
		cross apply sys.fn_PageResCracker(er.page_resource) AS r
		cross apply sys.dm_db_page_info (r.[db_id], r.[file_id], r.page_id, 'detailed') as page_info
	where er.wait_type like '%page%'
end
go

-- Last updated October 1, 2021
;WITH [Waits] AS
    (SELECT
        [wait_type],
        [wait_time_ms] / 1000.0 AS [WaitS],
        ([wait_time_ms] - [signal_wait_time_ms]) / 1000.0 AS [ResourceS],
        [signal_wait_time_ms] / 1000.0 AS [SignalS],
        [waiting_tasks_count] AS [WaitCount],
        100.0 * [wait_time_ms] / SUM ([wait_time_ms]) OVER() AS [Percentage],
        ROW_NUMBER() OVER(ORDER BY [wait_time_ms] DESC) AS [RowNum]
    FROM sys.dm_os_wait_stats
    WHERE [wait_type] NOT IN (
        -- These wait types are almost 100% never a problem and so they are
        -- filtered out to avoid them skewing the results. Click on the URL
        -- for more information.
        N'BROKER_EVENTHANDLER', -- https://www.sqlskills.com/help/waits/BROKER_EVENTHANDLER
        N'BROKER_RECEIVE_WAITFOR', -- https://www.sqlskills.com/help/waits/BROKER_RECEIVE_WAITFOR
        N'BROKER_TASK_STOP', -- https://www.sqlskills.com/help/waits/BROKER_TASK_STOP
        N'BROKER_TO_FLUSH', -- https://www.sqlskills.com/help/waits/BROKER_TO_FLUSH
        N'BROKER_TRANSMITTER', -- https://www.sqlskills.com/help/waits/BROKER_TRANSMITTER
        N'CHECKPOINT_QUEUE', -- https://www.sqlskills.com/help/waits/CHECKPOINT_QUEUE
        N'CHKPT', -- https://www.sqlskills.com/help/waits/CHKPT
        N'CLR_AUTO_EVENT', -- https://www.sqlskills.com/help/waits/CLR_AUTO_EVENT
        N'CLR_MANUAL_EVENT', -- https://www.sqlskills.com/help/waits/CLR_MANUAL_EVENT
        N'CLR_SEMAPHORE', -- https://www.sqlskills.com/help/waits/CLR_SEMAPHORE
 
        -- Maybe comment this out if you have parallelism issues
        N'CXCONSUMER', -- https://www.sqlskills.com/help/waits/CXCONSUMER
 
        -- Maybe comment these four out if you have mirroring issues
        N'DBMIRROR_DBM_EVENT', -- https://www.sqlskills.com/help/waits/DBMIRROR_DBM_EVENT
        N'DBMIRROR_EVENTS_QUEUE', -- https://www.sqlskills.com/help/waits/DBMIRROR_EVENTS_QUEUE
        N'DBMIRROR_WORKER_QUEUE', -- https://www.sqlskills.com/help/waits/DBMIRROR_WORKER_QUEUE
        N'DBMIRRORING_CMD', -- https://www.sqlskills.com/help/waits/DBMIRRORING_CMD
        N'DIRTY_PAGE_POLL', -- https://www.sqlskills.com/help/waits/DIRTY_PAGE_POLL
        N'DISPATCHER_QUEUE_SEMAPHORE', -- https://www.sqlskills.com/help/waits/DISPATCHER_QUEUE_SEMAPHORE
        N'EXECSYNC', -- https://www.sqlskills.com/help/waits/EXECSYNC
        N'FSAGENT', -- https://www.sqlskills.com/help/waits/FSAGENT
        N'FT_IFTS_SCHEDULER_IDLE_WAIT', -- https://www.sqlskills.com/help/waits/FT_IFTS_SCHEDULER_IDLE_WAIT
        N'FT_IFTSHC_MUTEX', -- https://www.sqlskills.com/help/waits/FT_IFTSHC_MUTEX
       
	   -- Maybe comment these six out if you have AG issues
        N'HADR_CLUSAPI_CALL', -- https://www.sqlskills.com/help/waits/HADR_CLUSAPI_CALL
        N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', -- https://www.sqlskills.com/help/waits/HADR_FILESTREAM_IOMGR_IOCOMPLETION
        N'HADR_LOGCAPTURE_WAIT', -- https://www.sqlskills.com/help/waits/HADR_LOGCAPTURE_WAIT
        N'HADR_NOTIFICATION_DEQUEUE', -- https://www.sqlskills.com/help/waits/HADR_NOTIFICATION_DEQUEUE
        N'HADR_TIMER_TASK', -- https://www.sqlskills.com/help/waits/HADR_TIMER_TASK
        N'HADR_WORK_QUEUE', -- https://www.sqlskills.com/help/waits/HADR_WORK_QUEUE
		--End comment for AG issues
		
        N'KSOURCE_WAKEUP', -- https://www.sqlskills.com/help/waits/KSOURCE_WAKEUP
        N'LAZYWRITER_SLEEP', -- https://www.sqlskills.com/help/waits/LAZYWRITER_SLEEP
        N'LOGMGR_QUEUE', -- https://www.sqlskills.com/help/waits/LOGMGR_QUEUE
        N'MEMORY_ALLOCATION_EXT', -- https://www.sqlskills.com/help/waits/MEMORY_ALLOCATION_EXT
        N'ONDEMAND_TASK_QUEUE', -- https://www.sqlskills.com/help/waits/ONDEMAND_TASK_QUEUE
        N'PARALLEL_REDO_DRAIN_WORKER', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_DRAIN_WORKER
        N'PARALLEL_REDO_LOG_CACHE', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_LOG_CACHE
        N'PARALLEL_REDO_TRAN_LIST', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_TRAN_LIST
        N'PARALLEL_REDO_WORKER_SYNC', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_SYNC
        N'PARALLEL_REDO_WORKER_WAIT_WORK', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_WAIT_WORK
        N'PREEMPTIVE_OS_FLUSHFILEBUFFERS', -- https://www.sqlskills.com/help/waits/PREEMPTIVE_OS_FLUSHFILEBUFFERS
        N'PREEMPTIVE_XE_GETTARGETSTATE', -- https://www.sqlskills.com/help/waits/PREEMPTIVE_XE_GETTARGETSTATE
        N'PVS_PREALLOCATE', -- https://www.sqlskills.com/help/waits/PVS_PREALLOCATE
        N'PWAIT_ALL_COMPONENTS_INITIALIZED', -- https://www.sqlskills.com/help/waits/PWAIT_ALL_COMPONENTS_INITIALIZED
        N'PWAIT_DIRECTLOGCONSUMER_GETNEXT', -- https://www.sqlskills.com/help/waits/PWAIT_DIRECTLOGCONSUMER_GETNEXT
        N'PWAIT_EXTENSIBILITY_CLEANUP_TASK', -- https://www.sqlskills.com/help/waits/PWAIT_EXTENSIBILITY_CLEANUP_TASK
        N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', -- https://www.sqlskills.com/help/waits/QDS_PERSIST_TASK_MAIN_LOOP_SLEEP
        N'QDS_ASYNC_QUEUE', -- https://www.sqlskills.com/help/waits/QDS_ASYNC_QUEUE
        N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
            -- https://www.sqlskills.com/help/waits/QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP
        N'QDS_SHUTDOWN_QUEUE', -- https://www.sqlskills.com/help/waits/QDS_SHUTDOWN_QUEUE
        N'REDO_THREAD_PENDING_WORK', -- https://www.sqlskills.com/help/waits/REDO_THREAD_PENDING_WORK
        N'REQUEST_FOR_DEADLOCK_SEARCH', -- https://www.sqlskills.com/help/waits/REQUEST_FOR_DEADLOCK_SEARCH
        N'RESOURCE_QUEUE', -- https://www.sqlskills.com/help/waits/RESOURCE_QUEUE
        N'SERVER_IDLE_CHECK', -- https://www.sqlskills.com/help/waits/SERVER_IDLE_CHECK
        N'SLEEP_BPOOL_FLUSH', -- https://www.sqlskills.com/help/waits/SLEEP_BPOOL_FLUSH
        N'SLEEP_DBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_DBSTARTUP
        N'SLEEP_DCOMSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_DCOMSTARTUP
        N'SLEEP_MASTERDBREADY', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERDBREADY
        N'SLEEP_MASTERMDREADY', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERMDREADY
        N'SLEEP_MASTERUPGRADED', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERUPGRADED
        N'SLEEP_MSDBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_MSDBSTARTUP
        N'SLEEP_SYSTEMTASK', -- https://www.sqlskills.com/help/waits/SLEEP_SYSTEMTASK
        N'SLEEP_TASK', -- https://www.sqlskills.com/help/waits/SLEEP_TASK
        N'SLEEP_TEMPDBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_TEMPDBSTARTUP
        N'SNI_HTTP_ACCEPT', -- https://www.sqlskills.com/help/waits/SNI_HTTP_ACCEPT
        N'SOS_WORK_DISPATCHER', -- https://www.sqlskills.com/help/waits/SOS_WORK_DISPATCHER
        N'SP_SERVER_DIAGNOSTICS_SLEEP', -- https://www.sqlskills.com/help/waits/SP_SERVER_DIAGNOSTICS_SLEEP
        N'SQLTRACE_BUFFER_FLUSH', -- https://www.sqlskills.com/help/waits/SQLTRACE_BUFFER_FLUSH
        N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', -- https://www.sqlskills.com/help/waits/SQLTRACE_INCREMENTAL_FLUSH_SLEEP
        N'SQLTRACE_WAIT_ENTRIES', -- https://www.sqlskills.com/help/waits/SQLTRACE_WAIT_ENTRIES
        N'VDI_CLIENT_OTHER', -- https://www.sqlskills.com/help/waits/VDI_CLIENT_OTHER
        N'WAIT_FOR_RESULTS', -- https://www.sqlskills.com/help/waits/WAIT_FOR_RESULTS
        N'WAITFOR', -- https://www.sqlskills.com/help/waits/WAITFOR
        N'WAITFOR_TASKSHUTDOWN', -- https://www.sqlskills.com/help/waits/WAITFOR_TASKSHUTDOWN
        N'WAIT_XTP_RECOVERY', -- https://www.sqlskills.com/help/waits/WAIT_XTP_RECOVERY
        N'WAIT_XTP_HOST_WAIT', -- https://www.sqlskills.com/help/waits/WAIT_XTP_HOST_WAIT
        N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', -- https://www.sqlskills.com/help/waits/WAIT_XTP_OFFLINE_CKPT_NEW_LOG
        N'WAIT_XTP_CKPT_CLOSE', -- https://www.sqlskills.com/help/waits/WAIT_XTP_CKPT_CLOSE
        N'XE_DISPATCHER_JOIN', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_JOIN
        N'XE_DISPATCHER_WAIT', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_WAIT
        N'XE_TIMER_EVENT' -- https://www.sqlskills.com/help/waits/XE_TIMER_EVENT
        )
    AND [waiting_tasks_count] > 0
    )
SELECT
    MAX ([W1].[wait_type]) AS [WaitType],
    CAST (MAX ([W1].[WaitS]) AS DECIMAL (16,2)) AS [Wait_S],
    CAST (MAX ([W1].[ResourceS]) AS DECIMAL (16,2)) AS [Resource_S],
    CAST (MAX ([W1].[SignalS]) AS DECIMAL (16,2)) AS [Signal_S],
    MAX ([W1].[WaitCount]) AS [WaitCount],
    CAST (MAX ([W1].[Percentage]) AS DECIMAL (5,2)) AS [Percentage],
    CAST ((MAX ([W1].[WaitS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgWait_S],
    CAST ((MAX ([W1].[ResourceS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgRes_S],
    CAST ((MAX ([W1].[SignalS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgSig_S],
    CAST ('https://www.sqlskills.com/help/waits/' + MAX ([W1].[wait_type]) as XML) AS [Help/Info URL]
FROM [Waits] AS [W1]
INNER JOIN [Waits] AS [W2] ON [W2].[RowNum] <= [W1].[RowNum]
GROUP BY [W1].[RowNum]
HAVING SUM ([W2].[Percentage]) - MAX( [W1].[Percentage] ) < 95; -- percentage threshold
GO


SELECT 'Page Life Expectancy' 'Page Life Expectancy', 
	object_name AS [Object Name], 
	cntr_value AS [Page Life Expectancy (s)]
FROM sys.dm_os_performance_counters
WHERE counter_name = N'Page life expectancy' AND object_name LIKE '%Buffer Manager%'
go

SELECT 'Buffer Cache Hit Ratio' 'Buffer Cache Hit Ratio',
	(a.cntr_value * 1.0 / b.cntr_value) * 100.0 as [Buffer Cache Hit Ratio %]
FROM sys.dm_os_performance_counters  a
JOIN  (SELECT cntr_value,OBJECT_NAME 
    FROM sys.dm_os_performance_counters  
    WHERE counter_name = 'Buffer cache hit ratio base'
        AND OBJECT_NAME LIKE '%Buffer Manager%') b ON  a.OBJECT_NAME = b.OBJECT_NAME
WHERE a.counter_name = 'Buffer cache hit ratio'
AND a.OBJECT_NAME LIKE '%Buffer Manager%'
go

/*
-- This query will run for 5 minutes. The WAITFOR can be changed
DECLARE @IOStats TABLE
    (
      [database_id] [smallint] NOT NULL,
      [file_id] [smallint] NOT NULL,
      [num_of_reads] [bigint] NOT NULL,
      [num_of_bytes_read] [bigint] NOT NULL,
      [io_stall_read_ms] [bigint] NOT NULL,
      [num_of_writes] [bigint] NOT NULL,
      [num_of_bytes_written] [bigint] NOT NULL,
      [io_stall_write_ms] [bigint] NOT NULL
    )
INSERT  INTO @IOStats
SELECT database_id,
    vio.file_id,
    num_of_reads,
    num_of_bytes_read,
    io_stall_read_ms,
    num_of_writes,
    num_of_bytes_written,
    io_stall_write_ms
FROM sys.dm_io_virtual_file_stats(NULL, NULL) vio OPTION (RECOMPILE);
DECLARE @StartTime DATETIME,
    @DurationInSecs INT
SET @StartTime = GETDATE()
WAITFOR DELAY '00:05:00'
SET @DurationInSecs = DATEDIFF(ss, @startTime, GETDATE())
SELECT 'Live DB Latency' 'Live DB Latency',
	DB_NAME(vio.database_id) AS [Database],
    mf.name AS [Logical name],
    mf.type_desc AS [Type],
    ( vio.io_stall_read_ms - old.io_stall_read_ms )
    / CASE ( vio.num_of_reads - old.num_of_reads )
        WHEN 0 THEN 1
        ELSE vio.num_of_reads - old.num_of_reads
        END AS [Ave read speed (ms)],
    vio.num_of_reads - old.num_of_reads AS [No of reads over period],
    CONVERT(DEC(14, 2), ( vio.num_of_reads - old.num_of_reads )
    / ( @DurationInSecs * 1.00 )) AS [No of reads/sec],
    CONVERT(DEC(14, 2), ( vio.num_of_bytes_read - old.num_of_bytes_read )
    / 1048576.0) AS [Tot MB read over period],
    CONVERT(DEC(14, 2), ( ( vio.num_of_bytes_read - old.num_of_bytes_read )
                            / 1048576.0 ) / @DurationInSecs) AS [Tot MB read/sec],
    ( vio.num_of_bytes_read - old.num_of_bytes_read )
    / CASE ( vio.num_of_reads - old.num_of_reads )
        WHEN 0 THEN 1
        ELSE vio.num_of_reads - old.num_of_reads
        END AS [Ave read size (bytes)],
    ( vio.io_stall_write_ms - old.io_stall_write_ms )
    / CASE ( vio.num_of_writes - old.num_of_writes )
        WHEN 0 THEN 1
        ELSE vio.num_of_writes - old.num_of_writes
        END AS [Ave write speed (ms)],
    vio.num_of_writes - old.num_of_writes AS [No of writes over period],
    CONVERT(DEC(14, 2), ( vio.num_of_writes - old.num_of_writes )
    / ( @DurationInSecs * 1.00 )) AS [No of writes/sec],
    CONVERT(DEC(14, 2), ( vio.num_of_bytes_written
                            - old.num_of_bytes_written ) / 1048576.0) AS [Tot MB written over period],
    CONVERT(DEC(14, 2), ( ( vio.num_of_bytes_written
                            - old.num_of_bytes_written ) / 1048576.0 )
    / @DurationInSecs) AS [Tot MB written/sec],
    ( vio.num_of_bytes_written - old.num_of_bytes_written )
    / CASE ( vio.num_of_writes - old.num_of_writes )
        WHEN 0 THEN 1
        ELSE vio.num_of_writes - old.num_of_writes
        END AS [Ave write size (bytes)],
    mf.physical_name AS [Physical file name],
    size_on_disk_bytes / 1048576 AS [File size on disk (MB)]
FROM sys.dm_io_virtual_file_stats(NULL, NULL) vio,
    sys.master_files mf,
    @IOStats old
WHERE mf.database_id = vio.database_id
    AND mf.file_id = vio.file_id
    AND old.database_id = vio.database_id
    AND old.file_id = vio.file_id
    AND ( ( vio.num_of_bytes_read - old.num_of_bytes_read ) + ( vio.num_of_bytes_written - old.num_of_bytes_written ) ) > 0
ORDER BY ( ( vio.num_of_bytes_read - old.num_of_bytes_read ) + ( vio.num_of_bytes_written - old.num_of_bytes_written ) ) DESC


SELECT 'Average DB Latency' 'Average DB Latency',
 DB_NAME(fs.database_id) AS [Database Name] ,
 mf.physical_name AS [Physical Name],
 io_stall_read_ms AS [Total Read (ms)],
 num_of_reads as [Reads],
 CAST(io_stall_read_ms / ( 1.0 + num_of_reads ) AS NUMERIC(10, 1)) AS [Avg Read Stall (ms)] ,
 io_stall_write_ms AS [Total Write (ms)],
 num_of_writes [Writes],
 CAST(io_stall_write_ms / ( 1.0 + num_of_writes ) AS NUMERIC(10, 1)) AS [Avg Write Stall (ms)] ,
 io_stall_read_ms + io_stall_write_ms AS [Total ms] ,
 num_of_reads + num_of_writes AS [Total IO] ,
 CAST(( io_stall_read_ms + io_stall_write_ms ) / ( 1.0 + num_of_reads + num_of_writes ) AS NUMERIC(10, 1)) AS [Avg IO (ms)]
FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS fs
INNER JOIN sys.master_files AS mf 
	ON fs.database_id = mf.database_id
 AND fs.[file_id] = mf.[file_id]
ORDER BY [Avg IO (ms)] DESC

SELECT 'Missing Index Count' 'Missing Index Count',
	count(1)
FROM sys.dm_db_missing_index_group_stats AS migs WITH ( NOLOCK )
INNER JOIN sys.dm_db_missing_index_groups AS mig WITH ( NOLOCK ) 
	ON migs.group_handle = mig.index_group_handle
INNER JOIN sys.dm_db_missing_index_details AS mid WITH ( NOLOCK ) 
	ON mig.index_handle = mid.index_handle
WHERE CAST((avg_total_user_cost * avg_user_impact) * (user_seeks + user_scans) AS BIGINT) >= 10000


SELECT 'Missing Index Details' 'Missing Index Details',
	mid.[statement] AS [Statement],
    CAST((avg_total_user_cost * avg_user_impact) * (user_seeks + user_scans) AS BIGINT) AS [Impact],
    CAST(migs.avg_total_user_cost AS NUMERIC(10,2)) AS [Average Total User Cost],
    CAST(migs.avg_user_impact AS BIGINT) AS [Estimated % Reduction of Cost],
    migs.user_seeks + migs.user_scans AS [Missed Opportunities],
    mid.equality_columns AS [Equality Columns],
    mid.inequality_columns AS [Inequality Columns],
    mid.included_columns AS [Included Columns]
FROM sys.dm_db_missing_index_group_stats AS migs WITH ( NOLOCK )
INNER JOIN sys.dm_db_missing_index_groups AS mig WITH ( NOLOCK ) 
	ON migs.group_handle = mig.index_group_handle
INNER JOIN sys.dm_db_missing_index_details AS mid WITH ( NOLOCK ) 
	ON mig.index_handle = mid.index_handle
WHERE CAST((avg_total_user_cost * avg_user_impact) * (user_seeks + user_scans) AS BIGINT) >= 10000
ORDER BY impact DESC
*/
