import threading
import httplib
import socket
from httplib import HTTPException
from bs4 import BeautifulSoup
import json

from profile_parser.userInfo import UserInfo
from configKeys import ConfigKeys
from database.mongoDB import MongoDB

class SubMasterThread(threading.Thread):
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

    def processEdges(self, edges, responseIdentifier):
        self.logger.info("Processing <%s> edges for node: %s" %\
            (self.type, self.node))

        invalids = self.config[ConfigKeys.SNS][ConfigKeys.INVALID_IDS]
        invalids.append(self.node[MongoDB.TWITTER_ID_KEY])
        invalids.append(self.node[MongoDB.TWITTER_SNAME_ID_KEY])
        ''' Remove accounts that should not part of the observations that come
        in the results of the web crawler, such the origin of the connections
        (self.node) and other identifiers specified in the configuration such
        as the account used to authenticate with the Twitter website
        '''
        for invalid in invalids:
            try:
                edges.remove(invalid)
                self.logger.info("Removing: <%s> edge for node: %s / %s" %\
                    (invalid,
                    self.node[MongoDB.TWITTER_ID_KEY],
                    self.node[MongoDB.TWITTER_SNAME_ID_KEY]))
            except ValueError:
                pass

        # Add edges to network
        for edge in edges:
            # For in-edges (followers), the edge is an id
            if(self.type == ConfigKeys.IN_EDGES_KEY):
                self.db.insertEdge(edge, self.node[MongoDB.TWITTER_ID_KEY])

            # For out-edges (following), the edge is a screen name
            elif(self.type == ConfigKeys.OUT_EDGES_KEY):
                self.db.insertEdge(self.node[MongoDB.TWITTER_ID_KEY], edge)

            # Add edge as node to be visited at the bfs queue
            self.db.enqueBFSQ({responseIdentifier: edge}, self.level)

    def collectEdges(self, pathIdentifier, requestIdentifier, responseIdentifier):
        self.logger.info("Collecting <%s> edges for node: %s" % (self.type, self.node))

        data=""
        try:
            conn = httplib.HTTPConnection(self.ip)
            url = "/%s/%s?app_label=%s&cursor_count=%s&%s" %\
                (pathIdentifier,
                self.type,
                self.config[ConfigKeys.APP_LABELS][self.ip],
                self.config[ConfigKeys.SNS][ConfigKeys.CURSOR_COUNT],
                requestIdentifier)

            self.logger.info("Connecting to: %s%s" % (self.ip, url))
            conn.request("GET", url)
            r = conn.getresponse()
            data = r.read()
            conn.close()
        except socket.error:
            self.logger.\
                error("Socket error when collecting edges for node: %s" %\
                    self.node)
            return False
        except HTTPException:
            self.logger.\
                error("HTTP error when collecting edges for node: %s" %\
                    self.node)
            return False

        #try:
        #    edges = json.loads(data)
        edgesStr = data.strip("[").strip("]").replace('"', "")
        edges = edgesStr.split(",")

        self.processEdges(edges, responseIdentifier)

        #except ValueError:
        #    self.logger.\
        #        error("Not valid JSON response received: **%s**" % data)
        #    return False

        return True

    '''
    def processProfile(self, info, nodeId):
        u = UserInfo(info,
            self.ip,
            Logger.clone(self.logger, UserInfo.CLASS_NAME))
        u.parse()

        return u
    '''

    def processJSONProfile(self, profile):
        newProfile = {}

        for attribute in self.config[ConfigKeys.SNS][ConfigKeys.PROFILE_ATTRIBUTES]:
            if attribute in profile:
                if(attribute.endswith("_count")):
                    newProfile[attribute] = UserInfo.getNumber(str(profile[attribute]))
                else:
                    newProfile[attribute] = profile[attribute]
            else:
                newProfile[attribute] = ""

        return newProfile

    def collectProfile(self):
        self.logger.info("Collecting profiles for level <%s>" % (self.level))

        # Iterate over: requests number * request size (for this ip)
        # One HTTP connection per each request
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
                    error("Socket error when collecting profiles")
                return False
            except HTTPException:
                self.logger.\
                    error("HTTP error when collecting profiles")
                return False

            try:
                profiles = json.loads(data)
                for profile in profiles:
                    pprofile = self.processJSONProfile(profile)
                    self.logger.info("PROFILE PARSED: %s" % pprofile)

                    if(pprofile["friends_count"] <= self.config[ConfigKeys.SNS][ConfigKeys.MAX_DEGREE] and\
                        pprofile["followers_count"] <= self.config[ConfigKeys.SNS][ConfigKeys.MAX_DEGREE]):
                        self.db.insertNode(pprofile, self.ip)
                        self.db.updateBFSQ(pprofile)
                    else:
                        self.logger.info("Profile <%s> exceeded degree limit: %s,%s" %\
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
            result = self.collectEdges("api-client",
                "user_id=" + self.node[MongoDB.TWITTER_ID_KEY],
                MongoDB.TWITTER_ID_KEY)

            if(result):
                # Remove node from BFS queue
                self.db.deque4FollowingBFSQ(self.node[MongoDB.TWITTER_ID_KEY])

        elif(self.type == ConfigKeys.OUT_EDGES_KEY):
            result = self.collectEdges("website-crawler",
                "screen_name=" + self.node[MongoDB.TWITTER_SNAME_ID_KEY],
                MongoDB.TWITTER_SNAME_ID_KEY)

            if(result):
                # Remove node from BFS queue
                self.db.deque4FollowersBFSQ(self.node[MongoDB.TWITTER_ID_KEY])
