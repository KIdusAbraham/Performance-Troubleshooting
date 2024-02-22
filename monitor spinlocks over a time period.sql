declare @collectionPeriod datetime
set @collectionPeriod = '00:00:05' --change this to capture data for a longer period of time

select 1 as [execution], * 
into #spinlocks
from sys.dm_os_spinlock_stats
order by backoffs desc

WAITFOR DELAY @collectionPeriod

select 'Spinlock stats over the last HH:MM:SS - '+convert(varchar,@collectionPeriod,108)
,curr.name
,curr.collisions - prev.collisions as collisions
,curr.spins - prev.spins as spins
,curr.spins_per_collision - prev.spins_per_collision as spins_per_collision
,curr.sleep_time - prev.sleep_time as sleep_time
,curr.backoffs - prev.backoffs as backoffs
from sys.dm_os_spinlock_stats curr
join #spinlocks prev on curr.name = prev.name
order by backoffs desc

drop table #spinlocks
