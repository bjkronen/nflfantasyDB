import nfldb
import dataset
import sqlalchemy

def weekly_player_stats(year):
	for x in range(1, 18):
		q = nfldb.Query(db)
		q.game(season_year=year, season_type='Regular', week=x)

		print
		print "week", x

		for pp in q.sort('passing_yds').as_aggregate():
			fp = pp.passing_yds * .04 + pp.passing_cmp * .25 + pp.passing_tds * 4 + pp.rushing_yds * .1 + pp.rushing_tds * 6 + pp.receiving_yds * .1 + pp.receiving_rec * .25 + pp.receiving_tds * 6 + pp.fumbles_tot * -1 + pp.fumbles_lost * -2 + pp.passing_int * -1 + pp.kickret_yds * .04 + pp.kickret_tds * 6 + pp.puntret_yds * .04 + pp.puntret_tds * 6  + pp.rushing_twoptm * 2 + pp.passing_twoptm * 2 + pp.receiving_twoptm * 2

			print pp.player, " ", year, " ", x, " ", fp
			data = dict(player_id=pp.player_id, player=str(pp.player), season_year=year, position=pp.player.position, week_number=x, passing_yards=pp.passing_yds, passing_attempts=pp.passing_att, passing_completions=pp.passing_cmp, passing_tds=pp.passing_tds, rushing_yards=pp.rushing_yds, rushing_tds=pp.rushing_tds, receiving_yards=pp.receiving_yds, receiving_completions=pp.receiving_rec, receiving_targets=pp.receiving_tar, receiving_tds=pp.receiving_tds, fumbles=pp.fumbles_tot, fumbles_lost=pp.fumbles_lost, interceptions=pp.passing_int, kick_return_yds=pp.kickret_yds, kick_return_tds=pp.kickret_tds, punt_return_yds=pp.puntret_yds, punt_return_tds=pp.puntret_tds, rushing_two_point_conversion=pp.rushing_twoptm, passing_two_point_conversion=pp.passing_twoptm, recieving_two_point_conversion=pp.receiving_twoptm, fantasy_points=fp)
			weekly_table.upsert(data, ['player_id', 'season_year', 'week_number'])

def yearly_player_stats(year):
	q = nfldb.Query(db)
	q.game(season_year=year, season_type='Regular')


	for pp in q.sort('passing_yds').as_aggregate():
		fp = pp.passing_yds * .04 + pp.passing_cmp * .25 + pp.passing_tds * 4 + pp.rushing_yds * .1 + pp.rushing_tds * 6 + pp.receiving_yds * .1 + pp.receiving_rec * .25 + pp.receiving_tds * 6 + pp.fumbles_tot * -1 + pp.fumbles_lost * -2 + pp.passing_int * -1 + pp.kickret_yds * .04 + pp.kickret_tds * 6 + pp.puntret_yds * .04 + pp.puntret_tds * 6  + pp.rushing_twoptm * 2 + pp.passing_twoptm * 2 + pp.receiving_twoptm * 2 + pp.fumbles_rec_tds * 6
		
		player_weeks_results = weekly_table.find(player_id=pp.player_id, season_year=year)
		
		weeks_played = 0
		for weekly_row in player_weeks_results:
			weeks_played+=1

		ppw = 0
		if weeks_played > 0:
			ppw = fp / weeks_played

		data = dict(player_id=pp.player_id, player=str(pp.player), season_year=year, position=pp.player.position, fantasy_points_per_week=ppw, games_played=weeks_played,  passing_yards=pp.passing_yds, passing_attempts=pp.passing_att, passing_completions=pp.passing_cmp, passing_tds=pp.passing_tds, rushing_yards=pp.rushing_yds, rushing_tds=pp.rushing_tds, receiving_yards=pp.receiving_yds, receiving_completions=pp.receiving_rec, receiving_targets=pp.receiving_tar, receiving_tds=pp.receiving_tds, fumbles=pp.fumbles_tot, fumbles_lost=pp.fumbles_lost, interceptions=pp.passing_int, kick_return_yds=pp.kickret_yds, kick_return_tds=pp.kickret_tds, punt_return_yds=pp.puntret_yds, punt_return_tds=pp.puntret_tds, rushing_two_point_conversion=pp.rushing_twoptm, passing_two_point_conversion=pp.passing_twoptm, recieving_two_point_conversion=pp.receiving_twoptm, fantasy_points=fp)
		yearly_table.upsert(data, ['player_id', 'season_year'])



db = nfldb.connect()

db3 = dataset.connect('postgresql://fantasyDB:fantasyDB@localhost:5432/fantasyDB')

weekly_table = db3['weekly_stats']
weekly_table.drop() 

weekly_table = db3.create_table('weekly_stats')

weekly_table.create_column('player_id', sqlalchemy.String)
weekly_table.create_column("season_year", sqlalchemy.Integer)
weekly_table.create_column('week_number', sqlalchemy.Integer)
weekly_table.create_column('position', sqlalchemy.String)
weekly_table.create_column('player', sqlalchemy.String)
weekly_table.create_column('passing_yards', sqlalchemy.Integer) 
weekly_table.create_column('passing_completions', sqlalchemy.Integer)
weekly_table.create_column('passing_attempts', sqlalchemy.Integer)
weekly_table.create_column('passing_tds', sqlalchemy.Integer)
weekly_table.create_column('rushing_yards', sqlalchemy.Integer)
weekly_table.create_column('rushing_tds', sqlalchemy.Integer)
weekly_table.create_column('receiving_yards', sqlalchemy.Integer)
weekly_table.create_column('receiving_completions', sqlalchemy.Integer)
weekly_table.create_column('receiving_targets', sqlalchemy.Integer)
weekly_table.create_column('receiving_tds', sqlalchemy.Integer)
weekly_table.create_column('fumbles', sqlalchemy.Integer)
weekly_table.create_column('fumbles_lost', sqlalchemy.Integer)
weekly_table.create_column('interceptions', sqlalchemy.Integer)
weekly_table.create_column('kick_return_yds', sqlalchemy.Integer)
weekly_table.create_column('kick_return_tds', sqlalchemy.Integer)
weekly_table.create_column('punt_return_yds', sqlalchemy.Integer)
weekly_table.create_column('punt_return_tds', sqlalchemy.Integer)
weekly_table.create_column('rushing_two_point_conversion', sqlalchemy.Integer)
weekly_table.create_column('passing_two_point_conversion', sqlalchemy.Integer)
weekly_table.create_column('receivng_two_point_conversion', sqlalchemy.Integer)
weekly_table.create_column('fantasy_points', sqlalchemy.Integer)

weekly_table.create_index(['player_id', 'season_year', 'week_number'])
weekly_table.create_index(['player_id', 'season_year'])
weekly_table.create_index(['position', 'season_year'])


year = 2009
while True:
	weekly_player_stats(year)

	year += 1
	if year > 2014:
		break 

yearly_table = db3['yearly_stats']
yearly_table.drop() 

yearly_table = db3.create_table('yearly_stats')

yearly_table.create_column('player', sqlalchemy.String)
yearly_table.create_column('player_id', sqlalchemy.String)
yearly_table.create_column('season_year', sqlalchemy.Integer)
yearly_table.create_column('position', sqlalchemy.String)
yearly_table.create_column('passing_yards', sqlalchemy.Integer) 
yearly_table.create_column('passing_completions', sqlalchemy.Integer)
yearly_table.create_column('passing_attempts', sqlalchemy.Integer)
yearly_table.create_column('passing_tds', sqlalchemy.Integer)
yearly_table.create_column('rushing_yards', sqlalchemy.Integer)
yearly_table.create_column('rushing_tds', sqlalchemy.Integer)
yearly_table.create_column('receiving_yards', sqlalchemy.Integer)
yearly_table.create_column('receiving_completions', sqlalchemy.Integer)
yearly_table.create_column('receiving_targets', sqlalchemy.Integer)
yearly_table.create_column('receiving_tds', sqlalchemy.Integer)
yearly_table.create_column('fumbles', sqlalchemy.Integer)
yearly_table.create_column('fumbles_lost', sqlalchemy.Integer)
yearly_table.create_column('interceptions', sqlalchemy.Integer)
yearly_table.create_column('kick_return_yds', sqlalchemy.Integer)
yearly_table.create_column('kick_return_tds', sqlalchemy.Integer)
yearly_table.create_column('punt_return_yds', sqlalchemy.Integer)
yearly_table.create_column('punt_return_tds', sqlalchemy.Integer)
yearly_table.create_column('rushing_two_point_conversion', sqlalchemy.Integer)
yearly_table.create_column('passing_two_point_conversion', sqlalchemy.Integer)
yearly_table.create_column('receivng_two_point_conversion', sqlalchemy.Integer)
yearly_table.create_column('fantasy_points', sqlalchemy.Integer)
yearly_table.create_column('fantasy_points_per_week', sqlalchemy.DECIMAL)
yearly_table.create_column('weekly_points_above_replacement', sqlalchemy.DECIMAL)
yearly_table.create_column('weekly_points_above_average_starter', sqlalchemy.DECIMAL)
yearly_table.create_column('games_played', sqlalchemy.Integer)



yearly_table.create_index(['player_id', 'season_year'])
yearly_table.create_index(['position', 'season_year'])

year = 2009
while True:
	yearly_player_stats(year)

	year += 1
	if year > 2014:
		break 


