import pymongo
from pymongo import MongoClient
import sys
from geopy.geocoders import GoogleV3

geolocator = GoogleV3()

reload(sys)
sys.setdefaultencoding('utf8')

client = MongoClient()
db = client["wsf_2016"]

output = "/home/jarr850/output/d_intermediaries_summary/intermediaries-08_07_2016_31_08_2016_all_w_locations"
o = open(output, 'w')

ids_file = "/home/jarr850/output/d_intermediaries_summary/intermediaries-08_07_2016_31_08_2016_ids"
f = open(ids_file, 'r')

degrees = []
degree_file = "/home/jarr850/output/d_intermediaries_summary/intermediaries-08_07_2016_31_08_2016_indegree"
f2 = open(degree_file, 'r')
for d in f2:
	degrees.append(str(d).strip())

part = []
part_file = "/home/jarr850/output/d_intermediaries_summary/intermediaries-08_07_2016_31_08_2016_participation"
f3 = open(part_file, 'r')
for p in f3:
	part.append(str(p).strip())

i = 0
for id_i in f:
	id = str(id_i).strip()
	result = db.nodes.find_one(
		{"twitterID": id}
	)
	if result is not None:
		location =  result["location"]

		if len(location) > 0 and id not in ("240471494"):
			print "%s | %s" % (id, location.encode('utf8'))
			geo = None
			try:
				geo = geolocator.geocode(location)
			except ValueError as error_message:	
				print("Error: geocode failed with message %s" % error_message)

			if geo is not None:
				address, (latitude, longitude) = geo
				locationUTF8 = location.encode('utf8').replace(',',' ');
				addressUTF8 =  address.encode('utf8').replace(',',' ');

				part_i = part[i]	
				degree_i = degrees[i]
					
				newData = "%s , %s , %s, %s, %s, %s, %s" % (locationUTF8, addressUTF8, latitude, longitude, id, part_i, degree_i)  	
				print newData
				o.write(newData + '\n')
				
	i = i + 1

o.close()
