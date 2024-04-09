create table #stats(
	database_name varchar(2000),
	[schema_name] varchar(2000),
	table_name varchar(2000),
	stats_name varchar(2000),
	[rows] numeric(38,0),
	modification_counter numeric(38,0),
	dynamic_update_threshold numeric(38,0),
	last_updated datetime
)

use [MedCompass]
	go

	insert into #stats
	SELECT 'MedCompass', sc.name, t.name, s.name, rows, modification_counter,  round(sqrt(1000*rows),0), last_updated
	FROM sys.stats AS s   
	CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
	join sys.tables t on t.object_id=s.object_id
	join sys.schemas sc on t.schema_id = sc.schema_id
	where rows is not NULL and is_ms_shipped = 0


--use [master]
--	go

	select 'All', count(1)
	from #stats
	union
	select 'Above MS threshold change', count(1)
	from #stats
	where dynamic_update_threshold<=modification_counter
	union 
	select'Above 5% change', count(1)
	from #stats
	where round(modification_counter*1.0/rows*100,2) > 5
	union 
	select'Above 10% change', count(1)
	from #stats
	where round(modification_counter*1.0/rows*100,2) > 10
	union 
	select'Above 15% change', count(1)
	from #stats
	where round(modification_counter*1.0/rows*100,2) > 15
	union 
	select'Above 20% change', count(1)
	from #stats
	where round(modification_counter*1.0/rows*100,2) > 20
	union 
	select'Above 25% change', count(1)
	from #stats
	where round(modification_counter*1.0/rows*100,2) > 25
	union
	select 'At least 1 change', count(1)
	from #stats
	where modification_counter>1
	order by 2 desc

	select
	*
	,case when dynamic_update_threshold<=modification_counter then 1
	else 0
	end as above_update_threshold
	, cast(round(modification_counter*1.0/rows*100,2) as decimal(10,2)) as pct_changed
	,'update statistics ['+[schema_name]+'].['+table_name+'] ['+stats_name+'] with fullscan
	GO' [command]
	from #stats s
	where dynamic_update_threshold<=modification_counter
	--where round(modification_counter*1.0/rows*100,2) > 25

drop table #stats
