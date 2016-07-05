import threading
import httplib
import socket
from httplib import HTTPException
from bs4 import BeautifulSoup
import json

from profile_parser.userInfo import UserInfo
from configKeys import ConfigKeys
from database.mongoDB import MongoDB

class SubMasterThread (threading.Thread):
    # Constants
    CLASS_NAME="SubThread"

    def __init__(self,
        type,
        node, level,
        logger, db, ip,
        config):

        threading.Thread.__init__(self)

        self.type = type
        self.node = node
        self.level = level
        self.logger = logger
        self.db = db
        self.ip = ip
        self.config = config

    def processEdges(self, nodesCont, direction, level, nodeId):
        self.logger.info("Processing %s edges for node %s" % (direction, nodeId))

        # Parse output from html parser
        snames = []
        if(nodesCont is not None):
            nodes = nodesCont.next

            snames = set([sname.strip() \
                for sname in nodes.splitlines() if len(sname.strip()) > 0])
            invalids = self.INVALID_IDS + [nodeId]
            for invalid in invalids:
                try:
                    snames.remove(invalid)
                    #self.logger.\
                    #    info("Invalid node %s eliminated" % invalid)
                except KeyError:
                    pass
                    # self.logger.\
                    #    info("Invalid node %s doesn't exist" % invalid)

            for sname in snames:
                # Add edges to network
                if(direction == self.IN_EDGES_KEY):
                    self.db.insertEdge(sname, nodeId)
                elif(direction == self.OUT_EDGES_KEY):
                    self.db.insertEdge(nodeId, sname)
                # Update BFS queue with out-edges
                self.db.updateBFSQ(sname, level)

    def collectEdges(self, level, nodeId, direction, userInfo):
        self.logger.info("Collecting %s edges for node %s" % (direction, nodeId))

        data=""
        nodeData = None
        try:
            conn = httplib.HTTPConnection(self.ip)
            url = "/connections?user=%s&sample_size=%s&direction=%s&user_info=%s" %\
                (nodeId, self.SAMPLE_SIZE, direction, userInfo)
            self.logger.info("Connecting to: %s%s" % (self.ip, url))
            conn.request("GET", url)
            r = conn.getresponse()
            data = r.read()
            conn.close()
        except socket.error:
            self.logger.\
                error("Connection refused when collecting edges for node %s" %\
                    nodeId)
            return nodeData
        except HTTPException:
            self.logger.\
                error("HTTP error when collecting edges for node %s" % nodeId)
            return nodeData

        soup = BeautifulSoup(data, 'html.parser')
        if(userInfo == 1):
            userData = soup.find("user_info")
            if(userData is not None):
                nodeData = self.processNode(userData, nodeId)


                self.db.insertNode(u, nodeId)


        edges = soup.find(direction)
        self.processEdges(edges, direction, level, nodeId)

        # Remove node from BFS queue
        self.db.dequeBFSQ(nodeId)

        return nodeData

    '''
    def processProfile(self, info, nodeId):
        u = UserInfo(info,
            self.ip,
            Logger.clone(self.logger, UserInfo.CLASS_NAME))
        u.parse()

        return u
    '''

    def processProfile(self, profile):
        for attribute in self.config[ConfigKeys.SNS][ConfigKeys.PROFILE_ATTRIBUTES]:
            if attribute not in profile:
                profile[attribute] = ""
                if(attribute.endswith("_count")):
                    profile[attribute] = UserInfo.getNumber(profile[attribute])

        return profile

    def collectProfile(self):
        self.logger.info("Collecting profiles for level %s" % (self.level))

        # Iterate over: requests number * request size (on each ip)
        reqCount = 0
        while reqCount >= 0 and\
            reqCount < self.config[ConfigKeys.MULTITHREADING][ConfigKeys.PROFILES_WINDOW_REQUESTS_NUMBER]:
            nodesIds = []
            nodesSNs = []
            profCount = 0
            while profCount >= 0 and\
                profCount < self.config[ConfigKeys.MULTITHREADING][ConfigKeys.PROFILES_REQUEST_SIZE]:
                    if(len(self.node) > 0):
                        node = self.node.pop()
                        if(MongoDB.TWITTER_ID_KEY in node):
                            nodesIds.append(node[MongoDB.TWITTER_ID_KEY])
                        elif(MongoDB.TWITTER_SNAME_ID_KEY in node):
                            nodesSNs.append(node[MongoDB.TWITTER_SNAME_ID_KEY])

                    else:
                        profCount = -2
                        reqCount = -2

                    profCount += 1

            data=""
            try:
                conn = httplib.HTTPConnection(self.ip)
                nodesIdsStr = ','.join([str(n) for n in nodesIds])
                nodesSNsStr = ','.join([str(n) for n in nodesSNs])
                appLabel = self.config[ConfigKeys.APP_LABELS][self.ip]
                url = "/api-client/profiles?app_label=%s&ids=%s&snames=%s" %\
                    (appLabel, nodesIdsStr, nodesSNsStr)
                self.logger.info("Connecting to: %s%s" % (self.ip, url))
                conn.request("GET", url)
                r = conn.getresponse()
                data = r.read()
                conn.close()
            except socket.error:
                self.logger.\
                    error("Socket error when collecting profiles at ip %s" % self.ip)
                return False
            except HTTPException:
                self.logger.\
                    error("HTTP error when collecting profiles at ip %s" % self.ip)
                return False

            try:
                profiles = json.loads(data)
                for profile in profiles:
                    pprofile = self.processProfile(profile)
                    self.logger.info("RESPONSE RECEIVED: %s" % pprofile)
                    if(pprofile["friends_count"] <= self.config[ConfigKeys.SNS][ConfigKeys.MAX_DEGREE] and\
                        pprofile["followers_count"] <= self.config[ConfigKeys.SNS][ConfigKeys.MAX_DEGREE]):
                        self.db.insertNode(pprofile, self.ip)
                        self.db.updateBFSQ(pprofile)
                    else:
                        self.logger.info("Profile %s exceeded degree limit: %s,%s" %\
                            (pprofile[MongoDB.TWITTER_SNAME_ID_KEY],
                                pprofile["friends_count"],
                                pprofile["followers_count"]))
            except ValueError:
                self.logger.\
                    error("Not valid JSON response received: **%s**" % data)

            reqCount += 1

        return True

    def run(self):
        if(self.type == ConfigKeys.PROFILE_KEY):
            self.collectProfile()

        elif(self.type == ConfigKeys.IN_EDGES_KEY):
            pass
        elif(self.type == ConfigKeys.OUT_EDGES_KEY):
            pass
        # nodeData = self.collectEdges(self.level, self.nodeId, self.OUT_EDGES_KEY, 1)
        # if(nodeData is not None):
        #    if(nodeData.getFollowers() <= self.MAX_INDEGREE):
        #        nodeData = self.collectEdges(self.level, self.nodeId, self.IN_EDGES_KEY, 0)
        #    else:
        #        self.logger.info("Skipping %s edges for node %s. Maximum is reached (%s)." % (self.IN_EDGES_KEY, self.nodeId, self.MAX_INDEGREE))
        #else:
        #    self.logger.info("Skipping %s edges for node %s. No profile found." % (self.IN_EDGES_KEY, self.nodeId))
