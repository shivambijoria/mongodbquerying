from pymongo import MongoClient
client = MongoClient('localhost:27017')
db = client.database1
db.country.drop()
db.players.drop()
db.match_results.drop()
db.player_cards.drop()
db.player_assists_goals.drop()
db.world_cup_history.drop()
db.stadium.drop()
db.playersmodified.drop()
db.countrystructure.drop()


import csv
f = open('Country.csv')
csv_f = csv.reader(f.read().splitlines())
for row in csv_f:
  db.country.insert({"Country_Name" : row[0].split("'")[1] ,"Population" : float(row[1]),"No_of_Worldcup_won":int(row[2]),"Manager":row[3].split("'")[1],"Capital":row[4].split("'")[1]})
  
  
f = open('Players.csv')
csv_f = csv.reader(f.read().splitlines())
for row in csv_f:
  db.players.insert({"Player_id" : int(row[0]) ,"Name" : (row[1]).split("'")[1],"Fname":(row[2]).split("'")[1],"Lname":row[3].split("'")[1],"DOB":row[4].split("'")[1],"Country":row[5].split("'")[1],"Height":int(row[6])
  ,"Club":row[7].split("'")[1],"Position":row[8].split("'")[1],"Caps_for_country":int(row[9]),"Is_captain":row[10]  })
  
  
f = open('Match_results.csv')
csv_f = csv.reader(f.read().splitlines())
for row in csv_f:
  db.match_results.insert({"Match_id":int(row[0]),"Date":row[1].split("'")[1],"Start_time":row[2].split("'")[1],"Team1":(row[3]).split("'")[1]
  ,"Team2":(row[4]).split("'")[1],"Team1_score":int(row[5]),"Team2_score":int(row[6]),"Stadium":row[7].split("'")[1],"Host_city":row[8].split("'")[1]})
  

f = open('Player_Cards.csv')
csv_f = csv.reader(f.read().splitlines())
for row in csv_f:
  db.player_cards.insert({"Player_id":int(row[0]),"No_of_Yellow_cards":int(row[1]),"No_of_Red_cards":int(row[2])})
  
f = open('Player_Assists_Goals.csv')
csv_f = csv.reader(f.read().splitlines())
for row in csv_f:
  db.player_assists_goals.insert({"Player_id":int(row[0]),"No_of_Matches":int(row[1]),"Goals":int(row[2]),"Assists":int(row[3]),"Minutes_Played":int(row[4])})
  
  
f = open('Worldcup_History.csv')
csv_f = csv.reader(f.read().splitlines())
for row in csv_f:
  db.world_cup_history.insert({"Year":int(row[0]),"Host":row[1].split("'")[1],"Winner":row[2].split("'")[1]})
  
#basic tables created


f = open('Match_results.csv')
csv_f = csv.reader(f.read().splitlines())
for row in csv_f:
	count1=db.stadium.find( { "Stadium":row[7].split("'")[1] } ).count()
	if count1==0:
		db.stadium.insert({"Stadium" : row[7].split("'")[1],"city" : row[8].split("'")[1]})

		
		
f = open('Match_results.csv')
csv_f = csv.reader(f.read().splitlines())
for row in csv_f:
	db.stadium.update({"Stadium":row[7].split("'")[1]},{ "$push": { "Match":{"Team1":row[3].split("'")[1],"Team2":row[4].split("'")[1],"Team1Score":int(row[5]) ,"Team2Score":int(row[6]),"Date":row[1].split("'")[1] }} })

#stadium datastructure created

#country datastructure being created
f = open('Players.csv')
csv_f = csv.reader(f.read().splitlines())
for row in csv_f:
  db.playersmodified.insert({"Player_id" : int(row[0]) ,"Name" : (row[1]).split("'")[1],"Fname":(row[2]).split("'")[1],"Lname":row[3].split("'")[1],"DOB":row[4].split("'")[1],"Country":row[5].split("'")[1],"Height":int(row[6])
  ,"Club":row[7].split("'")[1],"Position":row[8].split("'")[1],"Caps_for_country":int(row[9]),"Is_captain":row[10],"No_of_Yellow_cards":0,"No_of_Red_cards":0
, "Goals":0,"Assists":0 })
  
  

f = open('Player_Cards.csv')
csv_f = csv.reader(f.read().splitlines())
for row in csv_f:
  db.playersmodified.update({"Player_id":int(row[0])},{ "$set":   {"No_of_Yellow_cards":int(row[1]),"No_of_Red_cards":int(row[2])}   }   )
  
f = open('Player_Assists_Goals.csv')
csv_f = csv.reader(f.read().splitlines())
for row in csv_f:
  db.playersmodified.update({"Player_id":int(row[0])},{ "$set":   {"Goals":int(row[2]),"Assists":int(row[3]) }	})


#############################################  
f = open('Country.csv')
csv_f = csv.reader(f.read().splitlines())
for row in csv_f:
  db.countrystructure.insert({"CName" : row[0].split("'")[1] ,"Population" : float(row[1]),"Manager":row[3].split("'")[1],"Capital":row[4].split("'")[1]})

f = open('Worldcup_History.csv')
csv_f = csv.reader(f.read().splitlines())
for row in csv_f:
	db.countrystructure.update({"CName":row[2].split("'")[1]},{ "$push": { "WorldCupWonHistory":{"Year":int(row[0]),"Host":row[1].split("'")[1] }} })

#############################################
f = open('Players.csv')
csv_f = csv.reader(f.read().splitlines())
for row in csv_f:
	cur = db.playersmodified.find({'Player_id': int(row[0])}, {'Lname':1, '_id':0})
	st= str(cur[0]).split("'")
	Lname = str(st[3])
	
	cur = db.playersmodified.find({'Player_id': int(row[0])}, {'Fname':1, '_id':0})
	st= str(cur[0]).split("'")
	Fname = str(st[3])
	
	cur = db.playersmodified.find({'Player_id': int(row[0])}, {'Height':1, '_id':0})
	st= str(cur[0]).split("'")
	temp=str(st[2])
	Height =int(temp[2:-1]) 
	
	cur = db.playersmodified.find({'Player_id': int(row[0])}, {'DOB':1, '_id':0})
	st= str(cur[0]).split("'")
	DOB = str(st[3])
	
	cur = db.playersmodified.find({'Player_id': int(row[0])}, {'Is_captain':1, '_id':0})
	st= str(cur[0]).split("'")
	Is_captain = str(st[3])
	
	cur = db.playersmodified.find({'Player_id': int(row[0])}, {'Position':1, '_id':0})
	st= str(cur[0]).split("'")
	Position = str(st[3])
	
	cur = db.playersmodified.find({'Player_id': int(row[0])}, {'No_of_Yellow_cards':1, '_id':0})
	st= str(cur[0]).split("'")
	temp=str(st[2])
	No_of_Yellow_cards =int(temp[2:-1]) 
	
	
	cur = db.playersmodified.find({'Player_id': int(row[0])}, {'No_of_Red_cards':1, '_id':0})
	st= str(cur[0]).split("'")
	temp=str(st[2])
	No_of_Red_cards =int(temp[2:-1]) 
	
	cur = db.playersmodified.find({'Player_id': int(row[0])}, {'Goals':1, '_id':0})
	st= str(cur[0]).split("'")
	temp=str(st[2])
	Goals =int(temp[2:-1]) 
	
	cur = db.playersmodified.find({'Player_id': int(row[0])}, {'Assists':1, '_id':0})
	st= str(cur[0]).split("'")
	temp=str(st[2])
	Assists =int(temp[2:-1]) 
	
	cur = db.playersmodified.find({'Player_id': int(row[0])}, {'Country':1, '_id':0})
	st= str(cur[0]).split("'")
	Country = str(st[3])
	
	db.countrystructure.update({"CName":Country},{ "$push": { "players":{
	"Lname":Lname,
	"Fname":Fname,
	"Height":Height,
	"DOB":DOB,
	"is_captain":Is_captain,
	"Position":Position,
	"no_Yellow_cards":No_of_Yellow_cards,
	"no_Red_cards":No_of_Red_cards,
	"no_Goals":Goals,
	"no_Assists":Assists
	}} })

#country structure created