import logging
import datetime
import httplib
import socket
from httplib import HTTPException
from pymongo.errors import ServerSelectionTimeoutError

from logger import Logger
from database.mongoDB import MongoDB
from configKeys import ConfigKeys
from readProfiles import ReadProfiles

class Master:
    # Constants
    CLASS_NAME="Master"

    def __init__(self, config):
        self.config = config

        today = datetime.date.today()
        timeId = today.strftime('%d_%m_%Y')

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

        self.db = MongoDB(timeId,
            Logger.clone(self.logger, MongoDB.CLASS_NAME))

    def read_profiles(self, level):
        readProfiles = ReadProfiles(
            level,
            Logger.clone(
                self.logger, ReadProfiles.CLASS_NAME),
                self.db,
                self.config
        )
        readProfiles.start()

    def create_connections(self, level):
        self.logger.info("CREATING MASTER CONNECTION THREADS for level %i" %\
            level)
        '''
        threadFollowing = MasterFollowingThread(
            level,
            Logger.clone(
                self.logger, MasterFollowingThread.CLASS_NAME),
                self.db,
                self.ipsPool,
                self.WINDOW_FOLLOWERS_REQUESTS_SIZE,
                self.THREADS_INTERVAL_TIME,
                self.INVALID_IDS,
                self.SAMPLE_SIZE,
                self.MAX_DEGREE
        )
        threadFollowers = MasterFollowersThread(
            level,
            Logger.clone(
                self.logger, MasterFollowersThread.CLASS_NAME),
                self.db,
                self.ipsPool,
                self.WINDOW_FOLLOWERS_REQUESTS_SIZE,
                self.WINDOW_TIME,
                self.INVALID_IDS,
                self.SAMPLE_SIZE,
                self.MAX_DEGREE
        )

        threadFollowing.start()
        threadFollowers.start()

        threadFollowing.join()
        threadFollowers.join()
        '''
    def breadth_first_search(self, level):
        # Stage 1: Read profiles
        self.read_profiles(level);

        # Stage 2: Create connections
        self.create_connections(level);

        # Proceed with next level
        if(level < self.config[ConfigKeys.BFS][ConfigKeys.FINAL_LEVEL]):
            next_level = level + 1
            self.breadth_first_search(next_level)

    def start(self):
        LEVEL_0 = 0

        # Clearing logs in slave instances
        for ip in self.config[ConfigKeys.MULTITHREADING][ConfigKeys.IPS_POOL]:
            try:
                conn = httplib.HTTPConnection(ip)
                url = "/clear-log"
                self.logger.info("Connecting to: %s%s" % (ip, url))
                conn.request("GET", url)
                r = conn.getresponse()
                data = r.read()
                self.logger.info("Logs cleared in %s%s: %s" % (ip, url, data))
                conn.close()
            except socket.error:
                self.logger.\
                    error("Connection refused while clearing log for instance %s" % ip)
            except HTTPException:
                self.logger.\
                    error("HTTP error while clearing log for instance %s" % ip)

        try:
            self.db.clearBFSQ()

            self.db.createBFSQIndex()
            self.db.createEdgesIndex()
            self.db.createNodesIndex()
            self.db.createNodesValidatorIndex()

            # Add seed node to the bfs_queue
            self.db.enqueBFSQ({MongoDB.TWITTER_SNAME_ID_KEY:\
                self.config[ConfigKeys.SNS][ConfigKeys.EGO_ID]},
                LEVEL_0)

            # Start bfs
            self.breadth_first_search(LEVEL_0 + 1)
        except ServerSelectionTimeoutError:
            self.logger.error("Connection to database failed")
