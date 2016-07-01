import threading
import httplib
import socket
from httplib import HTTPException
from bs4 import BeautifulSoup

from logger import Logger
from parser.userInfo import UserInfo

class MasterThread (threading.Thread):
    # Constants
    CLASS_NAME="Thread"

    IN_EDGES_KEY="followers"
    OUT_EDGES_KEY="following"

    def __init__(self,
        nodeId, level,
        logger, db, ip,
        INVALID_IDS, SAMPLE_SIZE, MAX_INDEGREE):

        threading.Thread.__init__(self)

        self.nodeId = nodeId
        self.level = level
        self.logger = logger
        self.db = db
        self.ip = ip
        self.INVALID_IDS = INVALID_IDS
        self.SAMPLE_SIZE = SAMPLE_SIZE
        self.MAX_INDEGREE = MAX_INDEGREE

    def processNode(self, info, nodeId):
        u = UserInfo(info,
            self.ip,
            Logger.clone(self.logger, UserInfo.CLASS_NAME))
        u.parse()
        self.db.insertNode(u, nodeId)

        return u

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

        edges = soup.find(direction)
        self.processEdges(edges, direction, level, nodeId)

        # Remove node from BFS queue
        self.db.removeBFSQ(nodeId)

        return nodeData

    def run(self):
        self.logger.info("Processing node: %s[L%s] - IP: %s" %\
            (self.nodeId, self.level, self.ip))
        nodeData = self.collectEdges(self.level, self.nodeId, self.OUT_EDGES_KEY, 1)
        if(nodeData is not None):
            if(nodeData.getFollowers() <= self.MAX_INDEGREE):
                nodeData = self.collectEdges(self.level, self.nodeId, self.IN_EDGES_KEY, 0)
            else:
                self.logger.info("Skipping %s edges for node %s. Maximum is reached (%s)." % (self.IN_EDGES_KEY, self.nodeId, self.MAX_INDEGREE))
        else:
            self.logger.info("Skipping %s edges for node %s. No profile found." % (self.IN_EDGES_KEY, self.nodeId))
