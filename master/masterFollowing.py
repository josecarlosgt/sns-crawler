import copy
import time

from database.mongoDB import MongoDB
from logger import Logger
from configKeys import ConfigKeys
from subMasterThread import SubMasterThread

class MasterFollowing():
    # Constants
    CLASS_NAME="MasterFollowing"

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

        # Retrieve nodes from previous level
        nodes = self.db.retrieve4FollowingBFSQ(self.level - 1,
            self.config[ConfigKeys.BFS][ConfigKeys.LEVEL_SIZE])
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
            for count in range(0, self.config[ConfigKeys.MULTITHREADING][ConfigKeys.THREADS_POOL_SIZE]):
                try:
                    node = nodes.next()
                    ip = ipsPool.pop(0)
                    ipsPool.append(ip)
                    thread = SubMasterThread(
                        ConfigKeys.OUT_EDGES_KEY,
                        node,
                        self.level,
                        Logger.clone(
                            self.logger, SubMasterThread.CLASS_NAME + "-FOLLOWING-" + ip),
                        self.db,
                        ip,
                        self.config
                    )
                    threads.append(thread)
                    thread.start()
                    time.sleep(self.config[ConfigKeys.MULTITHREADING][ConfigKeys.THREADS_INTERVAL_TIME])
                except StopIteration:
                    hasMoreNodes = False
                    self.logger.info("ALL NODES IN BFSQ RETRIEVED (level %i)" %\
                        (self.level))
                    break

            # Wait until all threads finish to continue with next level
            self.logger.info("WAITING FOR <%ith> POOL OF <%i> THREADS (level %i)" %\
                (i, len(threads), self.level))
            for t in threads: t.join()

            self.logger.info("ALL %ith POOL OF <%i> THREADS FINISHED (level %i)" %\
                (i, len(threads), self.level))
            i = i + 1
            threads = []

        self.logger.info("LEVEL <%i> COMPLETED" % self.level)
