import pymongo
from pymongo import MongoClient

from master.database.mongoDB import MongoDB

class Triadics:

    def __init__(self, EGO_ID, TIMES):
        client = MongoClient()
        self.db = client[MongoDB.REMOTE_DATABASE_NAME]

        self.EGO_ID = EGO_ID;
        self.TIMES = TIMES;

    def find(self):
        i = 0
        stats = {}
        while (i < len(self.TIMES) - 1):
            time1 = self.TIMES[i]
            time2 = self.TIMES[i+1]
            i += 1

            print "Analyzing times: %s - %s" % (time1, time2)

            inEdgesT1 = self.db['edges' + time1].find(
                {"targetId": self.EGO_ID}
            )

            inEdgesT1S = set()
            for edge in inEdgesT1:
                inEdgesT1S.add(edge["sourceId"])

            inEdgesT2 = self.db['edges' + time2].find(
                {"targetId": self.EGO_ID}
            )
            inEdgesT2S = set()
            for edge in inEdgesT2:
                inEdgesT2S.add(edge["sourceId"])

            diffs = inEdgesT2S.difference(inEdgesT1S)
            print "Differences: %s" % diffs

            total = len(diffs)
            existed = 0.0
            for diff in diffs:
                previousC = self.db['edges' + time1].find(
                    {"$or": [{"targetId": diff}, {"sourceId": diff}]}
                )
                if(previousC.count() > 0):
                    existed += 1
                for previous in previousC:
                    print "source: %s / target: %s:" %\
                        (previous["sourceId"], previous["targetId"])

            stats[time2] = [total, existed,
                float("{0:.2f}".format(existed/total)) ]

        for key, value in stats.iteritems():
            print "Time %s stats: %s" % (key, value)

    def compareTimes(self):
        i = 0
        stats = {}
        time1 = self.TIMES[0]
        time2 = self.TIMES[1]

        print "Analyzing times: %s - %s" % (time1, time2)

        # Get followers in T1
        inEdgesT1 = self.db['edges' + time1].find(
            {"targetId": self.EGO_ID}
        )
        # Remove duplicates
        inEdgesT1S = set()
        for edge in inEdgesT1:
            inEdgesT1S.add(edge["sourceId"])

        # Get followers in T2
        inEdgesT2 = self.db['edges' + time2].find(
            {"targetId": self.EGO_ID}
        )
        inEdgesT2S = set()
        for edge in inEdgesT2:
            inEdgesT2S.add(edge["sourceId"])

        # Compute differences (Followers in T2, but not in T1)
        diffs = inEdgesT2S.difference(inEdgesT1S)
        print "Differences: %s" % diffs

        nett1 = []
        nett2 = []
        for diff in diffs:
            nett2.append([diff, self.EGO_ID])

            print "Analyzing new follower: %s" % diff
            previousC = self.db['edges' + time1].find({"sourceId": diff})
            for previous in previousC:
                print "Previous tie: %s, %s" % (diff, previous["targetId"])
                if previous["targetId"] in inEdgesT1S:
                        nett1.append([previous["targetId"], self.EGO_ID])
                        nett1.append([previous["sourceId"], previous["targetId"]])



        print "Network 1:"
        for node in nett1:
            print "%s,%s" % (node[0], node[1])

        nett2.extend(nett1)
        print "Network 2:"
        for node in nett2:
            print "%s,%s" % (node[0], node[1])
