import nfldb
import dataset
import sqlalchemy

db = nfldb.connect()

db2 = dataset.connect()
db3 = dataset.connect('postgresql://krondb6:prism2@localhost:5432/krondb6')

weekly_table = db3['weekly_stats']
yearly_table = db3['yearly_stats']


stretch_table = db3['stretch_stats']

stretch_table.create_column('player_id', sqlalchemy.String)
stretch_table.create_column('player', sqlalchemy.String)
stretch_table.create_column('week1', sqlalchemy.Integer)
stretch_table.create_column('week2', sqlalchemy.Integer)
stretch_table.create_column('week3', sqlalchemy.Integer)
stretch_table.create_column('week4', sqlalchemy.Integer)
stretch_table.create_column('position', sqlalchemy.String)
stretch_table.create_column('fantasy_points', sqlalchemy.Integer)
stretch_table.create_column('fantasy_points_per_week', sqlalchemy.DECIMAL)

stretch_table.create_index('player_id')
stretch_table.create_index('player_id', 'week1')
stretch_table.create_index('position')

weekly_stretch = 4

results = yearly_table.find(order_by='-fantasy_points_per_week')


x=0 
for row in results:
	player_weeks_results = weekly_table.find(player_id=row['player_id'])
	
	#weeks_played = yearly_table.count(player_id=row['player_id'])

	records=[]
	weeks_processed = weekly_stretch
	weeks_played = row['games_played']
	i=0 
	if weeks_played >= weekly_stretch:
		for weekly_row in player_weeks_results:
			records.append(weekly_row)
			i+=1

		i=0
		while True:
			row1 = records[i]
			row2 = records[i+1]
			row3 = records[i+2]
			row4 = records[i+3]
			#print i
			
			total_points = row1['fantasy_points'] + row2['fantasy_points'] + row3['fantasy_points'] + row4['fantasy_points']
			ppw = total_points / weekly_stretch

			data = dict(player_id=row1['player_id'], player=row1['player'], position=row1['position'], week1=row1['week_number'], week2=row2['week_number'],week3=row3['week_number'],week4=row4['week_number'], fantasy_points=total_points, fantasy_points_per_week=ppw)
			stretch_table.upsert(data, ['player_id', 'week1'])

			weeks_processed+=1
			i+=1

			if weeks_processed>weeks_played:
				break


	#print "{:<30}".format(row['player']), " ", row['position'], " ", row['games_played'], " ",  "{0:.2f}".format(row['fantasy_points_per_week']), " ", row['fantasy_points']#, " recv yards ", row['receiving_yards'], " recv tds ", row['receiving_tds'], " receptions ", row['receiving_completions'], " rush yds ", row['rushing_yards'], " rush tds ", row['rushing_tds']
	#if x==75:
	#	break
	#x+=1

x=0
results = stretch_table.find(position=['TE', 'WR'], order_by='-fantasy_points')
for row in results:
	print "{:<30}".format(row['player']), " ", row['position'], " ", "{:<2}".format(row['week1']), " ", "{:<2}".format(row['week2']), " ", "{:<2}".format(row['week3']), " ", "{:<2}".format(row['week4']), " ", "{0:.2f}".format(row['fantasy_points_per_week']), " ", row['fantasy_points']#, " recv yards ", row['receiving_yards'], " recv tds ", row['receiving_tds'], " receptions ", row['receiving_completions'], " rush yds ", row['rushing_yards'], " rush tds ", row['rushing_tds']
	x+=1
	if x > 250:
		break
