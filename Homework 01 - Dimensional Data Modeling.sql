-- Creating a type to handle the films struct
create type films as (
	film text,
	votes integer,
	rating real,
	film_id text
);

-- Creating a type to enumerate the acceptable values for the column quality_class
create type quality_class as enum( 'star', 'good', 'average', 'bad' );

-- DDL of the table actors
create table actors (
	actor_id text,
	actor_name text,
	year integer,
	films films[],
	quality_class quality_class,
	is_active boolean,
	primary key ( actor_id, year )
);

-- Populating the table actors with a full ingestion
insert into actors
with years as (
	select *
	from generate_series(
		(select MIN(year) from actor_films),
		(select MAX(year) from actor_films)
	) as year
)
, actor_first_year as (
	select
		actorid,
		actor,
		MIN(year) as min_year
	from
		actor_films
	group by
		actorid,
		actor
)
, actor_years as (
	select 
		afy.actorid,
		afy.actor,
		y.year
	from
		actor_first_year as afy
	join
		years as y
			on afy.min_year <= y.year
)
, actors as (
	select distinct
		ay.actorid,
		ay.actor,
		ay.year,
		array_remove( -- Removing nulls from the array if they exist
			array_agg( -- Creating the array of struct
				case 
					when af.year is not null
						then row( af.film, af.votes, af.rating, af.filmid )::films
				end
			) over (partition by ay.actorid order by ay.year)
		, null) as films,
		avg(af.rating) over (partition by ay.actorid, ay.year) as avg_rating,
		count(af.rating) over (partition by ay.actorid order by ay.year) as count_rating,
		af.year is not null as is_active
	from
		actor_years as ay
	left join
		actor_films as af
			on ay.actorid = af.actorid
			and ay.year = af.year
)
select
	actorid as actor_id,
	actor as actor_name,
	year,
	films,
	case
		when first_value(avg_rating) over (partition by actorid, count_rating) > 8
			then 'star'
		when first_value(avg_rating) over (partition by actorid, count_rating) > 7
			then 'good'
		when first_value(avg_rating) over (partition by actorid, count_rating) > 6
			then 'average'
		when first_value(avg_rating) over (partition by actorid, count_rating) <= 6
			then 'bad'
	end::quality_class as quality_class,
	is_active
from
	actors
order by
	actor_id,
	year
;

-- DDL of the table actors_history_scd
create table actors_history_scd (
	actor_id text,
	actor_name text,
	quality_class quality_class,
	is_active boolean,
	start_year integer,
	end_year integer,
	current_year integer,
	primary key ( actor_id, start_year )
);

-- Populating the table actors_history_scd with a full ingestion
insert into actors_history_scd
with previous_record as (
	select
		actor_id,
		actor_name,
		year,
		quality_class,
		is_active,
		lag(quality_class, 1) over (partition by actor_id order by year) as previous_quality_class,
		lag(is_active, 1) over (partition by actor_id order by year) as previous_is_active,
		2020 as current_year -- This year would be replaced to a variable to be set in the pipeline orchestrator
	from
		actors
	where
		year <= 2020
)
, changes as (
	select
		*,
		case
			when quality_class is distinct from previous_quality_class
				then 1
			when is_active is distinct from previous_is_active
				then 1
			else 0
		end as changed
	from
		previous_record
)
, streaks as (
	select
		*,
		SUM(changed) over (partition by actor_id order by year) as streak_identifier
	from
		changes
)
select
	actor_id,
	actor_name,
	quality_class,
	is_active,
	MIN(year) as start_year,
	MAX(year) as end_year,
	2020 as current_year -- This year would be replaced to a variable to be set in the pipeline orchestrator
from
	streaks
group by
	actor_id,
	actor_name,
	quality_class,
	is_active,
	streak_identifier
order by
	actor_id,
	start_year
;

-- Creating type actors_scd_type to handle the struct in the incremental SCD table
create type actors_scd_type as (
	quality_class quality_class,
	is_active boolean,
	start_year integer,
	end_year integer
);

-- Generating the SCD table incrementing the current year
with last_year as (
	select
		*
	from
		actors_history_scd
	where
		current_year = 2020
		and end_year = 2020
)
, history as (
	select
		actor_id,
		actor_name,
		quality_class,
		is_active,
		start_year,
		end_year
	from
		actors_history_scd
	where
		current_year = 2020
		and end_year < 2020
)
, current_year as (
	select
		*
	from
		actors
	where
		year = 2021
)
, unchanged_records as (
	select
		cy.actor_id,
		cy.actor_name,
		cy.quality_class,
		cy.is_active,
		ly.start_year,
		cy.year as end_year
	from
		current_year as cy
	join
		last_year as ly
			on ly.actor_id = cy.actor_id
	where
		cy.quality_class = ly.quality_class
		and cy.is_active = ly.is_active
)
, changed_records as (
	select
		cy.actor_id,
		cy.actor_name,
		unnest(
			array[
				row( ly.quality_class, ly.is_active, ly.start_year, ly.end_year )::actors_scd_type,
				row( cy.quality_class, cy.is_active, cy.year, cy.year )::actors_scd_type
			]
		) as records
	from
		current_year as cy
	join
		last_year as ly
			on ly.actor_id = cy.actor_id
	where
		cy.quality_class is distinct from ly.quality_class
		or cy.is_active is distinct from ly.is_active
)
, unnested_changed_records as (
	select
		actor_id,
		actor_name,
		(records::actors_scd_type).*
	from
		changed_records
)
, new_records as (
	select
		cy.actor_id,
		cy.actor_name,
		cy.quality_class,
		cy.is_active,
		cy.year as start_year,
		cy.year as end_year
	from
		current_year as cy
	left join
		last_year as ly
			on cy.actor_id = ly.actor_id
	where
		ly.actor_id is null
)
, appended as (
	select * from history
	union all
	select * from unchanged_records
	union all
	select * from unnested_changed_records
	union all
	select * from new_records
)
select
	*,
	2021 as current_year
from
	appended
;