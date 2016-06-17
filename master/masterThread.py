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
        INVALID_IDS, SAMPLE_SIZE):

        threading.Thread.__init__(self)

        self.nodeId = nodeId
        self.level = level
        self.logger = logger
        self.db = db
        self.ip = ip
        self.INVALID_IDS = INVALID_IDS
        self.SAMPLE_SIZE = SAMPLE_SIZE

    def processNode(self, info, nodeId):
        u = UserInfo(info,
            Logger.clone(self.logger, UserInfo.CLASS_NAME))
        u.parse()
        self.db.insertNode(u, nodeId)

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
        try:
            conn = httplib.HTTPConnection(self.ip)
            url = "/%s?sample_size=%s&direction=%s&user_info=%s" %\
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
            return
        except HTTPException:
            self.logger.\
                error("HTTP error when collecting edges for node %s" % nodeId)
            return

        soup = BeautifulSoup(data, 'html.parser')

        if(userInfo == 1):
            userInfo = soup.find("user_info")
            if(userInfo is not None):
                self.processNode(userInfo, nodeId)

        edges = soup.find(direction)
        self.processEdges(edges, direction, level, nodeId)

        # Remove node from BFS queue
        self.db.removeBFSQ(nodeId)

    def run(self):
        self.logger.info("Processing node: %s[L%s] - IP: %s" %\
            (self.nodeId, self.level, self.ip))
        self.collectEdges(self.level, self.nodeId, self.IN_EDGES_KEY, 1)
        self.collectEdges(self.level, self.nodeId, self.OUT_EDGES_KEY, 0)
