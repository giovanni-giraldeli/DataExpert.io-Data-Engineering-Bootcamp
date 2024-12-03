-- Facts: atomic grain; usually it can't be reduced
---- ~10-100x the size of the dimension data
---- Can need additional context for effective analysis. E.g. region and demographics
---- Duplicates are more common here than in the dimensional data

-- Normalized Facts: don't have dimensional attributes, just IDs
---- Increases standardization
---- "The smaller the scale, the better normalization is gonna be as an option"
-- Denormalized Facts: brings some dimensional attributes >> don't need joins, but needs more storage
---- Increases querying efficiency

-- Raw Logs: SWEs >> OLTP, without quality guarantees and potentially can have duplicates
-- Fact Data: DEs >> OLAP, longer retentation, better naming for columns, quality column guarantees

-- Fact Data Models: WHAT, WHEN, WHO, WHERE and HOW fields
---- No duplicates!
---- Tip: Log the timestamps in UTC instead of specific timezones
---- Should have quality guarantees, otherwise analysts could use the log directly
---- Generally is smaller than the raw logs, since the SWEs need to map what happens specifically in the server, that doesn't have necessarily business information
---- Easier data types: strings, numbers, timestamps
---- Sometimes the solution could be adapt upstream data architecture instead of solving by creating/modifying a pipeline
---- Thrift: middle layer from SWEs environment and DEs environment that is language agnostic >> shared schema
------ Tests to guarantee that changes from upstream will be considered in the pipeline

-- Duplicates: usually there's a time window where they matter. E.g. a notification clicked in 10 minutes after and 265 days after
---- Capture with streaming data: consider retaining a day and dedup the data in it (works fine for smaller datasets)
------ Hourly Microbatch Dedup for every hour
------ FULL OUTER JOIN hours in couples consecutively to eliminate the duplicates
--------- E.g. hmd_01, hmd_02, hmd_03, hmd_04 >> hmd_01_02, hmd_03_04 >> hmd_01_04

-- Lab 04: Fact Data Modeling

create table fct_game_details (
	dim_game_date date,
	dim_season integer,
	dim_team_id integer,
	dim_player_id integer,
	dim_player_name text,
	dim_start_position text,
	dim_is_playing_at_home boolean,
	dim_did_not_play boolean,
	dim_did_not_dress boolean,
	dim_not_with_team boolean,
	m_minutes real,
	m_fgm integer,
	m_fga integer,
	m_fg3m integer,
	m_fg3a integer,
	m_fta integer,
	m_oreb integer,
	m_dreb integer,
	m_reb integer,
	m_ast integer,
	m_stl integer,
	m_blk integer,
	m_turnovers integer,
	m_pf integer,
	m_pts integer,
	m_plus_minus integer,
	primary key (dim_game_date, dim_team_id, dim_player_id)
);

insert into fct_game_details
with deduped as (
	select 
		g.game_date_est,
		g.season,
		g.home_team_id,
		g.visitor_team_id,
		gd.*,
		row_number() over (partition by gd.game_id, gd.team_id, gd.player_id order by g.game_date_est) as row_num
	from
		game_details as gd
	join
		games as g
			on gd.game_id = g.game_id
)
select
	game_date_est as dim_game_date,
	season as dim_season,
	team_id as dim_team_id,
	player_id as dim_player_id,
	player_name as dim_player_name,
	start_position as dim_start_position,
	team_id = home_team_id as dim_is_playing_at_home,
	coalesce(position('DNP' in comment) > 0, FALSE) as dim_did_not_play,
	coalesce(position('DND' in comment) > 0, FALSE) as dim_did_not_dress,
	coalesce(position('NWT' in comment) > 0, FALSE) as dim_not_with_team,
	cast(split_part(min, ':', 1) as real) + cast(split_part(min, ':', 2) as real) / 60 as minutes,
	fgm as m_fgm,
	fga as m_fga,
	fg3m as m_fg3m,
	fg3a as m_fg3a,
	ftm as m_ftm,
	fta as m_fta,
	oreb as m_oreb,
	dreb as m_dreb,
	reb as m_reb,
	ast as m_ast,
	stl as m_stl,
	blk as m_blk,
	"TO" as m_turnovers,
	pf as m_pf,
	plus_minus as m_plus_minus
from
	deduped
where
	row_num = 1
;

select
	dim_player_name,
	count(1) as num_games,
	count(case when dim_not_with_team then 1 end) as bailed_num,
	count(case when dim_not_with_team then 1 end) * 1.0 / count(1) as bailed_pct
from
	fct_game_details as fct
group by 1
order by 4 desc