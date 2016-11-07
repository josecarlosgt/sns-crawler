import pymongo
from pymongo import MongoClient

from master.database.mongoDB import MongoDB

class Triadics:

    def __init__(self, CONF, EGO_ID, TIMES):
        client = MongoClient()
        self.db = client[MongoDB.REMOTE_DATABASE_NAME]
        self.CONF = CONF + "/" + "intermediaries-"
        self.EGO_ID = EGO_ID;
        self.TIMES = TIMES;

    def find(self):
        i = 0
        stats = {}
        inter = {}
        interTimes = {}
        interInDegree = {}

        while (i < len(self.TIMES) - 1):
            time1 = self.TIMES[i]
            time2 = self.TIMES[i+1]
            i += 1

            output = self.CONF + time1 + "_" + time2
            fo = open(output, "w")

            print "TIMES: %s - %s" % (time1, time2)

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
            print "DIFFERENCES: %s" % diffs

            total = len(diffs)
            existed = 0
            for diff in diffs:
                previousC = self.db['edges' + time1].find(
                    # {"$or": [{"targetId": diff}, {"sourceId": diff}]}
                    {"sourceId": diff}
                )
                if(previousC.count() > 0):
                    existed += 1
                    print "NEW FOLLOWER: %s existent" % diff
                    for previous in previousC:
                        egoFollowers = self.db['edges' + time1].find(
                            {"targetId": self.EGO_ID, "sourceId": previous["targetId"]}
                        )
                        egoFollowersC = []
                        for egoFollower in egoFollowers:
                            egoFollowersC.append(egoFollower["sourceId"])
                        egoFollowersNodes = self.db['nodes' + time1].find(
                            {"twitterID":{"$in":egoFollowersC}})

                        for egoFollowerNode in egoFollowersNodes:
                            if inter.has_key(egoFollowerNode["twitterID"]):
                                inter[egoFollowerNode["twitterID"]] = \
                                    inter[egoFollowerNode["twitterID"]] + 1
                            else:
                                inter[egoFollowerNode["twitterID"]] = 1

                            if not interTimes.has_key(egoFollowerNode["twitterID"]):
                                interTimes[egoFollowerNode["twitterID"]] = set()
                            interTimes[egoFollowerNode["twitterID"]].add(time1)

                            interInDegree[egoFollowerNode["twitterID"]] = egoFollowerNode["followers_count"]

                            participation = inter[egoFollowerNode["twitterID"]]
                            print "\tIntermediary: %s (Indegree: %s) (Participation: %s)" % (
                                egoFollowerNode["twitterID"],
                                egoFollowerNode["followers_count"],
                                participation
                                )
                            if len(interTimes[egoFollowerNode["twitterID"]]) > 1:
                                print "\t\t %s" % interTimes[egoFollowerNode["twitterID"]]

                            fo.write("%s\n" % egoFollowerNode["followers_count"])
                else:
                    print "NEW FOLLOWER: %s non-existent" % diff

            fo.write("ex %s\n" % existed)
            fo.write("ne %s\n" % (total - existed))
            fo.write("total %s\n" % total)

            fo.close()

        timeKey = self.TIMES[0] + "_" + self.TIMES[-1]
        output_p = self.CONF + timeKey + "_participation"
        output_d = self.CONF + timeKey + "_indegree"
        fo_p = open(output_p, "w")
        fo_d = open(output_d, "w")
        for id, participation in inter.items():
            if(participation >= 2):
                indegree = interInDegree[id]
                fo_p.write("%s\n" % participation)
                fo_d.write("%s\n" % indegree)

        fo_p.close()
        fo_d.close()
