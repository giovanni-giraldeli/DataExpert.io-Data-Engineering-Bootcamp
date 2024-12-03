-- Create a type to map the daily activity in the website
create type daily_activity as (
	date date,
	events_count integer,
	users_count integer
);

-- Creating a table to map the website hosts activity
create table hosts_cumulated (
	host text,
	date date,
	host_activity_datelist daily_activity[],
	primary key ( host, date )
);

-- Inputing values in the table hosts_cumulates based in a date variable
insert into hosts_cumulated
with yesterday as (
	select
		*
	from
		hosts_cumulated
	where
		date = date(:date_ref) - interval '1' day
)
, events_dedup as (
	select
		*,
		row_number() over (partition by host, event_time, user_id, device_id) as rn
	from
		events
	where
		date(event_time) = date(:date_ref)
)
, host_dedup as (
	select
		host,
		date(event_time) as date,
		count(1) as events_count,
		count(distinct user_id) as users_count
	from
		events_dedup
	where
		rn = 1
	group by
		host,
		date(event_time)
)
select
	coalesce(hd.host, y.host) as host,
	coalesce(hd.date, y.date + interval '1' day) as date,
	case
		when y.host_activity_datelist is null
			then array[ row(hd.date, hd.events_count, hd.users_count)::daily_activity ]
		when hd.host is not null
			then y.host_activity_datelist || array[ row(hd.date, hd.events_count, hd.users_count)::daily_activity ]
		else y.host_activity_datelist
	end as host_activity_datelist
from
	host_dedup as hd
full outer join
	yesterday as y
		on hd.host = y.host
;

select * from hosts_cumulated;