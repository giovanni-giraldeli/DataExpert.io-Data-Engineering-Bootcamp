-- Create a type to map the daily activity in the website by browser_type
create type device_activity_datelist as (
	browser_type text,
	date date,
	events_count integer
);

-- Create a table that cumulates the user behavior in the website
create table users_devices_cumulated (
	user_id numeric,
	device_id numeric,
	date date,
	device_activity_datelist device_activity_datelist[],
	primary key ( user_id, device_id, date )
);

-- Inputing values in the users_devices_cumulated table with a date variable
insert into users_devices_cumulated
with yesterday as (
	select
		*
	from
		users_devices_cumulated
	where
		date = date(:date_ref) - interval '1' day
)
, events_dedup as (
	select
		*,
		row_number() over (partition by user_id, device_id, event_time) as rn
	from
		events
)
, devices_dedup as (
	select
		*,
		row_number() over (partition by device_id, browser_type) as rn
	from
		devices
)
, today as (
	select
		e.user_id,
		e.device_id,
		d.browser_type,
		date(:date_ref) as date,
		count(1) as events_count
	from
		events_dedup as e
	join
		devices_dedup as d
			on d.device_id = e.device_id
	where
		date(e.event_time) = :date_ref
		and e.user_id is not null
		and e.rn = 1
		and d.rn = 1
	group by
		e.user_id,
		e.device_id,
		d.browser_type,
		date(:date_ref)
)
select
	coalesce(t.user_id, y.user_id) as user_id,
	coalesce(t.device_id, y.device_id) as device_id,
	date( coalesce(t.date, y.date + interval '1' day) ) as date,
	case 
		when y.device_activity_datelist is null
			then array[row(t.browser_type, t.date, t.events_count)::device_activity_datelist]
		when t.user_id is not null
			then y.device_activity_datelist || array[row(t.browser_type, t.date, t.events_count)::device_activity_datelist]
		else y.device_activity_datelist
	end as device_activity_datelist
from
	today as t
full outer join
	yesterday as y
		on t.user_id = y.user_id
		and t.device_id = y.device_id
;

-- Retrieve the datelist for the users and devices
with series_date as (
	select series_date::date from generate_series( date(:date_ref) - interval '30' day, :date_ref, interval '1' day ) as series_date
)
, datelist_int as (
	select
		ud.user_id,
		ud.device_id,
		ud.device_activity_datelist,
		case 
			when array( select (elem).date from unnest(ud.device_activity_datelist) as elem ) @> array[sd.series_date]
				then POW( 2, 32 - (ud.date - sd.series_date + 1) )
			else 0
		end as datelist_int
	from
		users_devices_cumulated as ud
	cross join
		series_date as sd
	where
		ud.date = :date_ref
)
	select
		user_id,
		device_id,
		device_activity_datelist,
		cast( cast( sum( datelist_int ) as bigint ) as bit(32) ) as datelist
	from
		datelist_int
	group by
		user_id,
		device_id,
		device_activity_datelist
;