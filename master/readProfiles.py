import copy
import time

from logger import Logger
from database.mongoDB import MongoDB
from configKeys import ConfigKeys
from subMasterThread import SubMasterThread

class ReadProfiles:
    # Constants
    CLASS_NAME="ReadProfiles"

    def __init__(self,
        level,
        logger, db,
        config):

        self.level = level
        self.logger = logger
        self.db = db
        self.config = config

    def start(self):
        self.logger.info("CREATING THREADS for level <%i>" %\
            self.level)

        # Retrieve nodes from current level
        nodes = self.db.retrieveBFSQ(self.level - 1,
            self.config[ConfigKeys.BFS][ConfigKeys.LEVEL_SIZE], False)
        threads = []
        hasMoreNodes = False
        nodesCount = nodes.count()
        if(nodesCount > 0):
            hasMoreNodes = True
        else:
            self.logger.info("BFSQ is empty at level <%i>" %\
                self.level)
        ipsPool = copy.copy(self.config[ConfigKeys.MULTITHREADING][ConfigKeys.IPS_POOL])
        while(hasMoreNodes):
            i = 1
            # Create a thread per available ip
            ipsCount = 0
            while ipsCount >= 0 and ipsCount < len(ipsPool):
                ip = ipsPool.pop(0)
                ipsPool.append(ip)

                # Iterate over: requests number * request size (on each ip)
                nodesA = []
                reqCount = 0
                while reqCount >= 0 and\
                    reqCount < self.config[ConfigKeys.MULTITHREADING][ConfigKeys.PROFILES_WINDOW_REQUESTS_NUMBER]:
                    profCount = 0
                    while profCount >= 0 and\
                        profCount < self.config[ConfigKeys.MULTITHREADING][ConfigKeys.PROFILES_REQUEST_SIZE]:

                        try:
                            node = nodes.next()
                            nodesA.append(node)
                        except StopIteration:
                            self.logger.info("ALL NODES <%i> IN BFSQ RETRIEVED (level %i)" %\
                                (nodesCount, self.level))
                            hasMoreNodes = False
                            profCount = -2
                            reqCount = -2
                            ipsCount = -2

                        profCount += 1
                    reqCount += 1

                thread = SubMasterThread(
                    ConfigKeys.PROFILE_KEY,
                    nodesA,
                    self.level,
                    Logger.clone(
                        self.logger, SubMasterThread.CLASS_NAME + "-PROFILES-" + ip),
                    self.db,
                    ip,
                    self.config
                )
                threads.append(thread)
                thread.start()
                ipsCount += 1

            # Wait until all threads finish to continue with next level
            self.logger.info("WAITING FOR <%ith> POOL OF <%i> THREADS (level %i)" %\
                (i, len(threads), self.level))
            for t in threads: t.join()

            self.logger.info("ALL <%ith> POOL OF <%i> THREADS FINISHED (level %i)" %\
                (i, len(threads), self.level))
            i = i + 1
            threads = []

            if(hasMoreNodes and nodes.alive):
                self.logger.info("Sleeping for <%s> min." %\
                    self.config[ConfigKeys.MULTITHREADING][ConfigKeys.WINDOW_TIME])
                time.sleep(
                    self.config[ConfigKeys.MULTITHREADING][ConfigKeys.WINDOW_TIME] * 60)

        self.logger.info("LEVEL <%i> COMPLETED" % self.level)
