-- Creating a reduced host fact table
create table host_activity_reduced (
	month date,
	host text,
	hit_array integer[],
	unique_visitors_array integer[],
	primary key ( month, host )
);

-- Creating type to handle array of arrays
create type metrics_array as (
	hit_array integer[],
	unique_visitors_array integer[]
);

-- Populating the reduced host fact directly from the source tables
insert into host_activity_reduced
with yesterday as (
	select
		*
	from
		host_activity_reduced
	where
		month = date(date_trunc('month', date(:date_ref) - interval '1' day))
)
, events_dedup as (
	select
		*,
		row_number() over (partition by host, event_time, user_id, device_id) as rn
	from
		events
)
, today as (
	select
		date(event_time) as date,
		host,
		count(1) as hits,
		count(distinct user_id) as users
	from
		events_dedup
	where
		rn = 1
		and date(event_time) = :date_ref
	group by
		date(event_time),
		host
)
, metrics_array as (
	select
		date(date_trunc('month', date(:date_ref))) as month,
		coalesce(y.host, t.host) as host,
		y.hit_array,
		y.unique_visitors_array,
		case
			when extract(day from date(:date_ref)) = 1
				then case
					when t.date is not null
						then row( array[t.hits], array[t.users] )::metrics_array -- Populate the first day of the month with events
					else row( array[0], array[0] )::metrics_array -- Populate the first day of the month for existing users that didn't make events
				end
			when y.hit_array is not null
				then row(
					y.hit_array || array[ coalesce( t.hits, 0 ) ],
					y.unique_visitors_array || array[ coalesce( t.users, 0 ) ]
				)::metrics_array -- Populating records in the middle of the month
			else row(
				array_fill( 0, array[ coalesce( date - date(date_trunc('month', t.date)), 0 ) ] )
				|| array[t.hits],
				array_fill( 0, array[ coalesce( date - date(date_trunc('month', t.date)), 0 ) ] )
				|| array[t.users]
			)::metrics_array
		end as metrics_array
	from
		today as t
	full outer join
		yesterday as y
			on t.host = y.host
)
select
	month,
	host,
	(metrics_array::metrics_array).hit_array,
	(metrics_array::metrics_array).unique_visitors_array
from
	metrics_array
-- Overwrite would do the job below
on conflict (month, host)
do
	update set hit_array = excluded.hit_array, unique_visitors_array = excluded.unique_visitors_array
;

-- An alternative to create the reduced fact in a full batch instead of incremental based on the cumulated table
with date_series as (
	select
		date(series_date) as series_date
	from
		generate_series(
			(select date_trunc('month', min(date)) from hosts_cumulated),
			(select max(date) from hosts_cumulated),
			interval '1' day
		) as series_date
)
, max_date as (
	select max(date) as max_date from hosts_cumulated
)
, unnested_hosts as (
	select
		hc.host,
		(unnest(hc.host_activity_datelist)::daily_activity).date,
		(unnest(hc.host_activity_datelist)::daily_activity).events_count,
		(unnest(hc.host_activity_datelist)::daily_activity).users_count
	from
		hosts_cumulated as hc
	join
		max_date as md
			on md.max_date = hc.date
)
select
	date(date_trunc('month', ds.series_date)) as month,
	dh.host,
	array_agg( coalesce( uh.events_count, 0 ) ) as hit_array,
	array_agg( coalesce( uh.users_count, 0 ) ) as unique_visitors_array
from
	(select distinct host from hosts_cumulated) as dh
cross join
	date_series as ds
left join
	unnested_hosts as uh
		on dh.host = uh.host
		and ds.series_date = uh.date
group by
	date(date_trunc('month', ds.series_date)),
	dh.host
;