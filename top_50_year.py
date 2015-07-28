import nfldb
import dataset
import sqlalchemy

db = nfldb.connect()

db2 = dataset.connect()
db3 = dataset.connect('postgresql://krondb6:prism2@localhost:5432/krondb6')

weekly_table = db3['weekly_stats']
yearly_table = db3['yearly_stats']

results = yearly_table.find(position=['WR', 'RB', 'TE'], order_by='-fantasy_points_per_week')
x=0 
for row in results:
	ppw = str(row['fantasy_points_per_week'])
	print "{:<30}".format(row['player']), " ", row['position'], " ", row['games_played'], " ",  "{0:.2f}".format(row['fantasy_points_per_week']), " ", row['fantasy_points']#, " recv yards ", row['receiving_yards'], " recv tds ", row['receiving_tds'], " receptions ", row['receiving_completions'], " rush yds ", row['rushing_yards'], " rush tds ", row['rushing_tds']
	if x==50:
		break
	x+=1

