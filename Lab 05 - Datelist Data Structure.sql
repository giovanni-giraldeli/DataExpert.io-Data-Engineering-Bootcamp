-- Bucketization: reducing cardinality by creating bucket of values in order to smooth the behavior of the data
---- Tip: 5 to 10 buckets
---- The buckets should be informed and based on the distribution
-- Facts and Dimensions are blurry and can be one based in another and vice-versa
---- An Airbnb price for a night sounds like a fact, but it's a dimension, since it's an attribute of a night
---- Labeling an account as fake in Facebook creates the dimension dim_ever_labeled_fake, which is based on fact data
-- Date List data structure
---- Cumulative table design that process a time window in a row
---- E.g. active users in Facebook in the last 30 days
------ user_id, date, datelist_int
------ 32, 2023-01-01, 10000001000000000001
------ Every day the new records drop the right-most value and add the current situation on the left-most value

-- Lab 05: Datelist Data Structure

create table users_cumulated (
	user_id text,
	dates_active date[], -- The list of dates in the past where the user was active
	date date, -- The current date for the user
	primary key ( user_id, date )
);

insert into users_cumulated
with yesterday as (
	select 
		*
	from
		users_cumulated
	where
		date = date('2023-01-30')
)
, today as (
	select 
		user_id::text,
		date( cast(event_time as timestamp) ) as date_active
	from
		events
	where
		DATE(cast(event_time as timestamp)) = date('2023-01-31')
		and user_id is not null
	group by
		user_id,
		date( cast(event_time as timestamp) )
)
select
	coalesce(t.user_id, y.user_id) as user_id,
	case
		when y.dates_active is null
			then array[t.date_active]
		when t.date_active is null
			then y.dates_active
		else array[t.date_active] || y.dates_active
	end as dates_active,
	coalesce(t.date_active, y.date + interval '1' day) as date
from
	today as t
full outer join
	yesterday as y
		on t.user_id = y.user_id
;

with users as (
	select * from users_cumulated where date = '2023-01-31'
)
, series as (
	select * from generate_series(date('2023-01-01'), date('2023-01-31'), interval '1' day) as series_date
)
, placeholder_ints as (
	select
		case 
			when u.dates_active @> array [date(s.series_date)]
				then POW(2, 32 - (date - date(series_date)))
			else 0
		end as placeholder_int_value,
		*
	from
		users as u
	cross join
		series as s
)
select
	user_id,
	cast( sum(placeholder_int_value)::bigint as bit(32) ) as dim_datelist, -- sums the POW(2) nums and determine back the bits that generated it and flag them as 1
	bit_count( cast( sum(placeholder_int_value)::bigint as bit(32) ) ) > 0 as dim_is_monthly_active, -- sums the bits from datelist
	bit_count(cast('11111110000000000000000000000000' as bit(32)) -- cast only 1's in the 7 first days and use an 'AND' operator to compare it to the datelist >> 1 AND 1 = 1, 1 AND 0 = 0, 0 AND 0 = 0
		& cast( sum(placeholder_int_value)::bigint as bit(32) ) ) > 0 as dim_is_weekly_active
from
	placeholder_ints
group by
	user_id