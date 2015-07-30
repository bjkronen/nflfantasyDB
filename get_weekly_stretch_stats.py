import nfldb
import dataset
import sqlalchemy


db = nfldb.connect()
db2 = dataset.connect()
db3 = dataset.connect('postgresql://fantasyDB:fantasyDB@localhost:5432/fantasyDB')

weekly_table = db3['weekly_stats']
yearly_table = db3['yearly_stats']
stretch_table = db3['stretch_stats']


weekly_stretch = 4

pos = 'TE'
pos = raw_input("Enter a Postion (leave blank for All) : ")
season_year = raw_input("Enter a Year: ")


x=0

if pos=='':
	print pos
	results = stretch_table.find(season_year=season_year, order_by='-fantasy_points')
else:
	results = stretch_table.find(position=pos, season_year=season_year, order_by='-fantasy_points')

for row in results:
	print "{:<35}".format(row['player']), " ", row['player_id'], " ", row['season_year'], " ", row['position'], " ", "{:<2}".format(row['week1']), " ", "{:<2}".format(row['week2']), " ", "{:<2}".format(row['week3']), " ", "{:<2}".format(row['week4']), " ", "{0:.2f}".format(row['fantasy_points_per_week']), " ", row['fantasy_points']#, " recv yards ", row['receiving_yards'], " recv tds ", row['receiving_tds'], " receptions ", row['receiving_completions'], " rush yds ", row['rushing_yards'], " rush tds ", row['rushing_tds']
	x+=1
	if x > 50:
		break
