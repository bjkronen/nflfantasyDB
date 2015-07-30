import nfldb
import dataset

db = nfldb.connect()
q = nfldb.Query(db)


db3 = dataset.connect('postgresql://fantasyDB:fantasyDB@localhost:5432/fantasyDB')

weekly_table = db3['weekly_stats']
yearly_table = db3['yearly_stats']

#results = weekly_table.find(season_year=2013, order_by='-fantasy_points')
results = yearly_table.find(season_year=2013, order_by='-fantasy_points_per_week')


x=0 
for row in results:
	print row['player'], " ", row['player_id'], " ", row['position'], " ", row['season_year'], " ", row['fantasy_points']
	x+=1
	if x > 250:
		break

q.game( season_year=2013, season_type='Regular')

for pp in q.sort('passing_yds').limit(25).as_aggregate():
    print pp.player, pp.passing_yds