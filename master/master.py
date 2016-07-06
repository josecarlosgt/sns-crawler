import logging
import datetime
import httplib
import socket
from httplib import HTTPException
from pymongo.errors import ServerSelectionTimeoutError

from pymongo import MongoClient
import os
import time

from logger import Logger
from database.mongoDB import MongoDB
from configKeys import ConfigKeys
from readProfiles import ReadProfiles
from masterFollowers import MasterFollowers
from masterFollowing import MasterFollowing

class Master:
    # Constants
    CLASS_NAME="Master"

    def __init__(self, config):
        self.config = config

        today = datetime.date.today()
        self.timeId = today.strftime('%d_%m_%Y')

        enableStdout = config["MasterLogging"]["enableStdout"]
        logFile = config["MasterLogging"]["logFile"]
        with open(logFile, 'w'): pass

        logging.basicConfig(filename=logFile, level=logging.INFO)
        logging.basicConfig(filename=logFile, level=logging.ERROR)
        self.logger = Logger(enableStdout, self.CLASS_NAME, self.timeId)
        self.logger.info("Running crawler")

        self.logger.info("Checking configuration")
        for section, sValue in config.iteritems():
            self.logger.info(section)
            for option, value in sValue.iteritems():
                self.logger.info("\t %s: %s" % (option, value))

        self.mainDB = MongoDB(self.timeId,
            Logger.clone(self.logger, MongoDB.CLASS_NAME))

    def read_profiles(self, level):
        readProfiles = ReadProfiles(
            level,
            Logger.clone(
                self.logger, ReadProfiles.CLASS_NAME),
                self.mainDB,
                self.config
        )
        readProfiles.start()

    def create_connections(self, level):
        self.logger.info("CREATING MASTER CONNECTION THREADS for level %i" %\
            level)

    	SLEEP = 10
        MP = "MP"
    	isParent = True
        cID = ""

    	p = self.mainDB.db[MP].p
    	p.drop();
        p.insert_one({"_id": ConfigKeys.IN_EDGES_KEY})
        p.insert_one({"_id": ConfigKeys.OUT_EDGES_KEY})

    	newpid1 = os.fork()
    	# We are the child
    	if newpid1 == 0:
            isParent = False
            cID = ConfigKeys.OUT_EDGES_KEY
            db1 = MongoDB(self.timeId,
                Logger.clone(self.logger, MongoDB.CLASS_NAME + "[%s]" % cID))
            MasterFollowing(
                level,
                Logger.clone(
                    self.logger, MasterFollowing.CLASS_NAME),
                db1,
                self.config
            ).start()
            p = db1.db[MP].p;
            p.remove({"_id": cID})
            # We are the parent
        else:
            newpid2 = os.fork()
            # We are the child
            if newpid2 == 0:
                isParent = False
                cID = ConfigKeys.IN_EDGES_KEY
                db2 = MongoDB(self.timeId,
                    Logger.clone(self.logger, MongoDB.CLASS_NAME + "[%s]" % cID))
                MasterFollowers(
                    level,
                    Logger.clone(
                        self.logger, MasterFollowers.CLASS_NAME),
                        db2,
                    self.config).start()
                p = db2.db[MP].p;
                p.remove({"_id": cID})

    	if not isParent:
            print "CHILD PROCESS FINISHED"
            os._exit(0)
    	else:
    		wait = True
    		while wait:
    			ps = p.find({})
    			wait = False if ps.count() == 0 else True
    			if wait:
    				print "MAIN PROCESS WAITING (%i)" % ps.count()
    				time.sleep(SLEEP)

    		print "MAIN PROCESS RESUMED"

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
            self.mainDB.clearBFSQ()

            self.mainDB.createBFSQIndex()
            self.mainDB.createEdgesIndex()
            self.mainDB.createNodesIndex()
            self.mainDB.createNodesValidatorIndex()

            # Add seed node to the bfs_queue
            self.mainDB.enqueBFSQ({MongoDB.TWITTER_SNAME_ID_KEY:\
                self.config[ConfigKeys.SNS][ConfigKeys.EGO_ID]},
                LEVEL_0)

            # Start bfs
            self.breadth_first_search(LEVEL_0 + 1)
        except ServerSelectionTimeoutError:
            self.logger.error("Connection to database failed")
