import pymongo
import sys 
from pymongo import MongoClient

from master.database.mongoDB import MongoDB

reload(sys)  
sys.setdefaultencoding('utf8')

class Linkages:

    def __init__(self, CONF, EGO_ID, TIMES):
	client = MongoClient()
	self.db = client[MongoDB.REMOTE_DATABASE_NAME]
	self.CONF = CONF + "/" + "linkages"
	self.EGO_ID = EGO_ID;
	self.TIMES = TIMES;

    def find(self):
	i = 0
	total = 0

	output = self.CONF
	fo = open(output, "w")
	while (i < len(self.TIMES)):
		timeTotal = 0
		time1 = self.TIMES[i]
		i = i + 1

		inEdgesT1 = self.db['edges' + time1].find(
			{"targetId": self.EGO_ID}
		)

		inEdgesT1S = set()
		for edge in inEdgesT1:
			inEdgesT1S.add(edge["sourceId"])

		for edge in inEdgesT1S:
			edgeLinkages = self.db['edges' + time1].find(
				{"targetId": edge}
			).count()
			print ("Count for %s: %s" % (edge, edgeLinkages))
			timeTotal = timeTotal + edgeLinkages

		total = total + timeTotal

		fo.write("Time %s: %s \n" % (time1, timeTotal))
		print ("Time %s: %s" % (time1, timeTotal))

	fo.write("Total: %s" % total)
	print ("Total: %s" % total)
	fo.close()
