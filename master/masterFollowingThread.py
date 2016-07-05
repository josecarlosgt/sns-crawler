import threading

from database.mongoDB import MongoDB
from logger import Logger
from master import Master
from submMasterThread import SubMasterThread

class MasterFollowingThread:
    # Constants
    CLASS_NAME="MasterFollowingThread"

    def __init__(self,
        level,
        logger, db,
        ipsPool, THREADS_POOL_SIZE, THREADS_INTERVAL_TIME,
        INVALID_IDS, SAMPLE_SIZE, MAX_DEGREE):

        threading.Thread.__init__(self)

        self.level = level
        self.logger = logger
        self.db = db
        self.ipsPool = ipsPool
        self.THREADS_POOL_SIZE = THREADS_POOL_SIZE
        self.THREADS_INTERVAL_TIME = THREADS_INTERVAL_TIME
        self.INVALID_IDS = INVALID_IDS
        self.SAMPLE_SIZE = SAMPLE_SIZE
        self.MAX_DEGREE = MAX_DEGREE

    def start(self):
        self.logger.info("CREATING THREADS for level %i" %\
            level)

        # Retrieve nodes from current level
        nodes = self.db.retrieveBFSQ(level - 1, self.BFSQ_LEVEL_SIZE, True)
        threads = []
        hasMoreNodes = True
        while(hasMoreNodes):
            i = 1
            for count in range(0, self.THREADS_POOL_SIZE):
                try:
                    node = nodes.next()
                    ip = self.ipsPool.pop(0)
                    self.ipsPool.append(ip)
                    nodeId = node["_id"]
                    thread = SubMasterThread(
                        Master.OUT_EDGES_KEY
                        nodeId,
                        level,
                        Logger.clone(
                            self.logger, SubMasterThread.CLASS_NAME + "-" + nodeId),
                        self.db,
                        ip,
                        self.INVALID_IDS,
                        self.SAMPLE_SIZE
                    )
                    threads.append(thread)
                    thread.start()
                    time.sleep(self.THREADS_INTERVAL_TIME)
                except StopIteration:
                    hasMoreNodes = False
                    self.logger.info("ALL NODES IN BFSQ RETRIEVED (level %i)" %\
                        (level))
                    break

            # Wait until all threads finish to continue with next level
            self.logger.info("WAITING FOR %ith POOL OF %i THREADS (level %i)" %\
                (i, len(threads), level))
            for t in threads: t.join()
            self.logger.info("ALL %ith POOL OF %i THREADS FINISHED (level %i)" %\
                (i, len(threads), level))
            i = i + 1
            threads = []

        self.logger.info("LEVEL %i COMPLETED" % level)
