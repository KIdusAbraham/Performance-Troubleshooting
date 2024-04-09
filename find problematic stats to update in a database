create table #stats(
	database_name varchar(2000),
	[schema_name] varchar(2000),
	table_name varchar(2000),
	stats_name varchar(2000),
	[rows] numeric(38,0),
	rows_sampled bigint,
	modification_counter numeric(38,0),
	dynamic_update_threshold numeric(38,0),
	last_updated datetime
)

create table #statsToUpdate(
	database_name varchar(2000),
	[schema_name] varchar(2000),
	table_name varchar(2000),
	stats_name varchar(2000),
	[rows] numeric(38,0),
	rows_sampled bigint,
	modification_counter numeric(38,0),
	dynamic_update_threshold numeric(38,0),
	last_updated datetime,
	above_update_threshold bit,
	pct_changed decimal(32,2),
	pct_sampled decimal(32,4),
	[command] varchar(2000)
)

	insert into #stats
	SELECT 'MedCompass', sc.name, t.name, s.name, rows, rows_sampled, modification_counter,  round(sqrt(1000*rows),0), last_updated
	FROM sys.stats AS s   
	CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
	join sys.tables t on t.object_id=s.object_id
	join sys.schemas sc on t.schema_id = sc.schema_id
	where rows is not NULL and is_ms_shipped = 0

	insert into #statsToUpdate
	SELECT *
	,case when dynamic_update_threshold<=modification_counter then 1
	else 0
	end as above_update_threshold
	,cast(round(modification_counter*1.0/rows*100,2) as decimal(32,2)) as pct_changed
	,rows_sampled/rows*100.00 pct_sampled
	,'update statistics ['+[schema_name]+'].['+table_name+'] ['+stats_name+'] with fullscan, PERSIST_SAMPLE_PERCENT = ON
	GO' [command]
	FROM #stats
	where modification_counter=0
	and rows_sampled/rows*100.00 <100
	
	insert into #statsToUpdate
	SELECT *
	,case when dynamic_update_threshold<=modification_counter then 1
	else 0
	end as above_update_threshold
	,cast(round(modification_counter*1.0/rows*100,2) as decimal(32,2)) as pct_changed
	,rows_sampled/rows*100.00 pct_sampled
	,'update statistics ['+[schema_name]+'].['+table_name+'] ['+stats_name+'] with fullscan, PERSIST_SAMPLE_PERCENT = ON
	GO' [command]
	FROM #stats
	where rows_sampled/rows*100.00 =100
	and dynamic_update_threshold<=modification_counter
	
	insert into #statsToUpdate
	SELECT *
	,case when dynamic_update_threshold<=modification_counter then 1
	else 0
	end as above_update_threshold
	,cast(round(modification_counter*1.0/rows*100,2) as decimal(32,2)) as pct_changed
	,rows_sampled/rows*100.00 pct_sampled
	,'update statistics ['+[schema_name]+'].['+table_name+'] ['+stats_name+'] with fullscan, PERSIST_SAMPLE_PERCENT = ON
	GO' [command]
	FROM #stats
	where cast(round(modification_counter*1.0/rows*100,2) as decimal(10,2))>0
	or rows_sampled/rows*100.00 <100

	select count(distinct stats_name)
	from #statsToUpdate

	select distinct table_name
	from #statsToUpdate

	select distinct *
	from #statsToUpdate
	order by rows asc

	drop table #statsToUpdate
drop table #stats
 
