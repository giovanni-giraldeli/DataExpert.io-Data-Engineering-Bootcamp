-- Deduplicating game_details table
with game_details_dedup as (
select
	gd.*,
	row_number() over (partition by gd.game_id, gd.team_id, gd.player_id, g.game_date_est) as rn
from
	game_details as gd
join
	games as g
		on gd.game_id = g.game_id
-- Joining to retrieve the dame date, since a player can match against the same multiple time, but in a different date
-- If a game is stopped due to some event and returns in another day, this date may make difference if the game_id remains the same in this case
)
select
	*
from
	game_details_dedup
where
	rn = 1
;