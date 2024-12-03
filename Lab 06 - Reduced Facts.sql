-- Shuffle is the bottleneck of parallelism
---- Extremely parallel: SELECT, FROM, WHERE
---- Kinda parallel: GROUP BY, JOIN, HAVING
------ GROUP BY: All the rows for a user need to be in a single machine to count how many events a user has >> the algorithm has to correctly separate the samples (usually in 200 by default)
------ JOIN: have to do the same as the group by, but for the left table and for the right table
------ HAVING: only is possible after a group by, so it happens in processes that already triggers shuffle
---- Painfully not parallel: ORDER BY
------ Use it only at the end if needed, since it needs to make a global inspection
---- WINDOW FUNCTION: is more efficient when used with PARTITION BY, since it slices to shuffle partitions >> behaves similarly to a GROUP BY

-- Making GROUP BY more efficient
---- Bucket your data (pre-shuffle) to prevent shuffle
---- Reduce the data volume

-- Reduced Fact Data Modeling
---- Reduce the grain of the table: event >> daily >> user array
------ The daily granularity works great for analysis in a 1 to 2 years max time window for big data
------ Reduced Fact: use the year-month as a column and the day as the index of the array
---- Could have 1 row per month or per year per user
---- Trade-off: as the grain is reduced, you lose flexibility on the analytics, but you can gain significantly performance

-- Lab 06: Reduce Fact Data Model

create table array_metrics (
	user_id numeric,
	month_start date,
	metric_name text,
	metric_array real[],
	primary key ( user_id, month_start, metric_name )
);

insert into array_metrics
with daily_aggregate as (
	select
		user_id,
		date(event_time) as date,
		count(1) as num_site_hits
	from
		events
	where
		date(event_time) = '2023-01-03'
		and user_id is not null
	group by
		user_id,
		date
)
, yesterday_array as (
	select
		*
	from
		array_metrics
	where
		month_start = date('2023-01-01')
)
select
	coalesce(da.user_id, ya.user_id) as user_id,
	coalesce( ya.month_start, date_trunc('month', da.date) ) as month_start,
	'site_hits' as metric_name,
	case 
		when ya.metric_array is not null
			then ya.metric_array || array[ coalesce(da.num_site_hits, 0) ]
		when ya.metric_array is null
			then array_fill( 0, array[ coalesce( date - date(date_trunc('month', date)), 0 ) ] ) -- Fills with 0s the days before if the user has appeared only in the middle of the month
				|| array[ coalesce(da.num_site_hits, 0) ] -- Concatenate today's data
	end as metric_array
from
	daily_aggregate as da
full outer join
	yesterday_array as ya
		on da.user_id = ya.user_id
-- Overwrite would do the job below
on conflict (user_id, month_start, metric_name)
do
	update set metric_array = excluded.metric_array
;

select * from array_metrics;

with agg as (
	select 
		metric_name,
		month_start,
		array[
			sum(metric_array[1]),
			sum(metric_array[2]),
			sum(metric_array[3]),
			sum(metric_array[4]),
			sum(metric_array[5])
		] as summed_array -- Usually this part could be an UDF that generate only the correct dates
	from
		array_metrics
	group by
		metric_name,
		month_start
)
select 
	metric_name,
	month_start + cast( cast(index - 1 as text) || ' day'  as interval ) as date,
	elem as value
from
	agg
cross join
	unnest(agg.summed_array)
		with ordinality as a(elem, index)
;