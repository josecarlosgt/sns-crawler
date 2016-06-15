import subprocess
import httplib
import socket
from pprint import pprint
from httplib import HTTPException
from bs4 import BeautifulSoup
import logging

from database.mongoDB import MongoDB
from parser.userInfo import UserInfo
from logger import Logger
from pymongo.errors import ServerSelectionTimeoutError

class Master:
    # Constants
    CLASS_NAME="Master"
    IN_EDGES_KEY="followers"
    OUT_EDGES_KEY="following"

    def __init__(self, timeId, config):
        enableStdout = config["MasterLogging"]["enableStdout"]
        logFile = config["MasterLogging"]["logFile"]
        with open(logFile, 'w'): pass

        logging.basicConfig(filename=logFile, level=logging.INFO)
        logging.basicConfig(filename=logFile, level=logging.ERROR)
        self.logger = Logger(enableStdout, self.CLASS_NAME, timeId)

        self.logger.info("Running crawler")
        self.logger.info("Checking configuration")
        for section, sValue in config.iteritems():
            self.logger.info(section)
            for option, value in sValue.iteritems():
                self.logger.info("\t %s: %s" % (option, value))
            #self.logger.info("\t %s" % config.items(section))

        self.EGO_ID = config["SNS"]["egoId"]
        self.INVALID_IDS = config["SNS"]["invalidIds"]
        self.SAMPLE_SIZE = config["SNS"]["sampleSize"]
        self.TOP_LEVEL = config["BFS"]["topLevel"]

        self.db = MongoDB(timeId,
            Logger.clone(self.logger, MongoDB.CLASS_NAME))

    def processNode(self, info, item):
        u = UserInfo(info,
            Logger.clone(self.logger, UserInfo.CLASS_NAME))
        u.parse()
        self.db.insertNode(u, item)

    def processEdges(self, nodesCont, direction, level, item):
        # Parse output from html parser
        snames = []
        if(nodesCont is not None):
            nodes = nodesCont.next

            snames = set([sname.strip() \
                for sname in nodes.splitlines() if len(sname.strip()) > 0])
            invalids = self.INVALID_IDS + [item]
            for invalid in invalids:
                try:
                    snames.remove(invalid)
                    self.logger.\
                        info("Invalid item %s eliminated" % invalid)
                except KeyError:
                    self.logger.\
                        info("Invalid item %s doesn't exist" % invalid)

            for sname in snames:
                # Add edges to network
                if(direction == self.IN_EDGES_KEY):
                    self.db.insertEdge(sname, item)
                elif(direction == self.OUT_EDGES_KEY):
                    self.db.insertEdge(item, sname)
                # Update BFS queue with out-edges
                self.db.updateBFSQ(sname, level)

    def collectEdges(self, level, item):
        data=""
        try:
            conn = httplib.HTTPConnection("127.0.0.1:3000")
            conn.request("GET", "/%s?sample_size=%s" % (item, self.SAMPLE_SIZE))
            r = conn.getresponse()
            data = r.read()
            conn.close()
        except socket.error:
            self.logger.\
                error("Connection refused when collecting edges for item %s"
                    % item)
            return
        except HTTPException:
            self.logger.\
                error("HTTP error when collecting edges for item %s" % item)
            return

        soup = BeautifulSoup(data, 'html.parser')
        userInfo = soup.find("user_info")
        if(userInfo is not None):
            self.processNode(userInfo, item)

        inEdges = soup.find(self.IN_EDGES_KEY)
        outEdges = soup.find(self.OUT_EDGES_KEY)

        self.processEdges(inEdges, self.IN_EDGES_KEY, level, item)
        self.processEdges(outEdges, self.OUT_EDGES_KEY, level, item)

        # Remove item from BFS queue
        self.db.removeBFSQ(item)

    def breadth_first_search(self, level):
        self.logger.info("Building network level %i" % level)

        # Retrieve items from level
        items = self.db.retrieveBFSQ(level - 1)
        for itemO in items:
            item = itemO["_id"]
            self.logger.info("Processing item: %s" % item)
            self.collectEdges(level, item)

        # Proceed with next level
        if(level < self.TOP_LEVEL):
            next_level = level + 1
            self.breadth_first_search(next_level)

    def start(self):
        LEVEL_0 = 0

        try:
            self.db.createEdgesIndex()

            # Disable this when resuming a failed operation
            self.db.clearBFSQ()

            # Add seed node to the bfs_queue
            self.db.updateBFSQ(self.EGO_ID, LEVEL_0)

            # Start bfs
            self.breadth_first_search(LEVEL_0 + 1)
        except ServerSelectionTimeoutError:
            self.logger.error("Connection to database failed")
