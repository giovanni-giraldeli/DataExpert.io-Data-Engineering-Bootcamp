-- Idempotency: result the same results, regardless the day you run it, how many times you run it and the hour that you run it
---- Don't use INSERT INTO (only use it with TRUNCATE coupled), since it can generate duplicates >> prefer MERGE or INSERT OVERWRITE
---- Use start_date > [date] coupled with a end_date < [date]
-- Slowly Changing Dimension (SCD): use it when you can collapse a bunch of rows >> it's not necessarily idempotent
---- Daily snapshots are idempotent, but they can be significantly bigger
---- SCD type 0 >> doesn't change
---- SCD type 1 >> only considers the latest value
---- SCD type 2 >> start_date + end_date >> is idempotent
---- SCD type 3 >> original value and current value (only 1 row) >> if changes more than once, intermediary data will be lost


-- Lab 02: Slowly Changing Dimensions (SCD)

-- Creating type from the last lab
create type season_stats as (
	season integer,
	gp integer,
	pts real,
	reb real,
	ast real
);

-- Creating type from the last lab
create type scoring_class as enum ('star', 'good', 'average', 'bad');

--------------------------------------
-- Auto backfilling table "players" --
--------------------------------------
insert into players
with years as (
	select *
	from generate_series(1996,2022) as season
)
, player_first_season as (
	select
		player_name,
		MIN(season) as first_season
	from
		player_seasons
	group by
		player_name
)
, players_and_seasons as (
	select 
		*
	from
		player_first_season as pfs
	join
		years as y
			on pfs.first_season <= y.season
)
, windowed as (
	select 
		pas.player_name,
		pas.season,
		array_remove(array_agg(
			case 
				when ps.season is not null
					then row(ps.season, ps.gp, ps.pts, ps.reb, ps.ast)::season_stats
			end
		) over (partition by pas.player_name order by pas.season),null) as season_stats
	from 
		players_and_seasons as pas
	left join
		player_seasons as ps
			on ps.player_name = pas.player_name
			and ps.season = pas.season
	order by
		pas.player_name,
		pas.season
)
, static as (
	select
		player_name,
		MAX(height) as height,
		MAX(college) as college,
		MAX(country) as country,
		MAX(draft_year) as draft_year,
		MAX(draft_round) as draft_round,
		MAX(draft_number) as draft_number
	from
		player_seasons
	group by
		player_name
)
select
	w.player_name,
	s.height,
	s.college,
	s.country,
	s.draft_year,
	s.draft_round,
	s.draft_number,
	w.season_stats,
	case
		when w.season_stats[cardinality(w.season_stats)].pts > 20 then 'star'
		when w.season_stats[cardinality(w.season_stats)].pts > 15 then 'good'
		when w.season_stats[cardinality(w.season_stats)].pts > 10 then 'average'
		else 'bad'
	end::scoring_class as scoring_class,
	w.season - w.season_stats[cardinality(w.season_stats)].season as years_since_last_season,
	w.season as current_season,
	w.season = w.season_stats[cardinality(w.season_stats)].season as is_active
from
	windowed as w
join
	static as s
		on w.player_name = s.player_name
;

---------------------
-- Players SCD DDL --
---------------------
create table players_scd (
	player_name text,
	scoring_class scoring_class,
	is_active boolean,
	start_season integer,
	end_season integer,
	current_season integer,
	primary key(player_name, start_season)
);

--------------------------------------------------
-- Players SCD data insertion >> FULL INGESTION --
--------------------------------------------------
insert into players_scd
with with_previous as (
	select 
		player_name,
		current_season,
		scoring_class,
		is_active,
		LAG(scoring_class, 1) over (partition by player_name order by current_season) as previous_scoring_class,
		LAG(is_active, 1) over (partition by player_name order by current_season) as previous_is_active
	from
		players
	where 
		current_season <= 2021
)
, indicators as (
	select
		*,
		case 
			when scoring_class <> previous_scoring_class
				then 1
			when is_active <> previous_is_active
				then 1
			else 0
		end as change_indicator
	from
		with_previous
)
, streaks as (
	select
		*,
		SUM(change_indicator) over (partition by player_name order by current_season) as streak_identifier
	from
		indicators
)
select
	player_name,
	scoring_class,
	is_active,
	MIN(current_season) as start_season,
	MAX(current_season) as end_season,
	2021 as current_season
from
	streaks
group by player_name, streak_identifier, is_active, scoring_class
order by player_name, start_season
;

-- Creating data type to handle SCD arrays
create type scd_type as (
	scoring_class scoring_class,
	is_active boolean,
	start_season integer,
	end_season integer
);

---------------------------------------------------------
-- Players SCD data insertion >> INCREMENTAL INGESTION --
---------------------------------------------------------
with last_season_scd as (
	select * from players_scd
	where current_season = 2021
	and end_season = 2021
)
, historical_scd as (
	select
		player_name,
		scoring_class,
		is_active,
		start_season,
		end_season
	from players_scd
	where current_season = 2021
	and end_season < 2021
)
, this_season_data as (
	select * from players
	where current_season = 2022
)
, unchanged_records as (
select
	ts.player_name,
	ts.scoring_class,
	ts.is_active,
	ls.start_season,
	ts.current_season as end_season
from
	this_season_data as ts
join
	last_season_scd as ls
		on ls.player_name = ts.player_name
where
	ts.scoring_class = ls.scoring_class
	and ts.is_active = ls.is_active
)
, changed_records as (
select
	ts.player_name,
	unnest(
		array[
			row(
				ls.scoring_class,
				ls.is_active,
				ls.start_season,
				ls.end_season
			)::scd_type,
			row(
				ts.scoring_class,
				ts.is_active,
				ts.current_season,
				ts.current_season
			)::scd_type
		]
	) as records
from
	this_season_data as ts
left join
	last_season_scd as ls
		on ls.player_name = ts.player_name
where
	(ts.scoring_class is distinct from ls.scoring_class 
	or ts.is_active is distinct from ls.is_active)
)
, unnested_changed_records as (
	select 
		player_name,
		(records::scd_type).*
	from
		changed_records
)
, new_records as (
	select
		ts.player_name,
		ts.scoring_class,
		ts.is_active,
		ts.current_season as start_season,
		ts.current_season as end_season
	from
		this_season_data as ts
	left join
		last_season_scd as ls
			on ts.player_name = ls.player_name
	where
		ls.player_name is null
)
select * from historical_scd
union all
select * from unchanged_records
union all
select * from unnested_changed_records
union all
select * from new_records
;

select * from players_scd