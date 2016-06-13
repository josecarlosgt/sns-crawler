import subprocess

import master.mongodb as mongoU
from master.parser.userInfo import UserInfo

# Constants

class Master:

    IN_EDGES_KEY="followers"
    OUT_EDGES_KEY="following"
    PARSER="/Users/josecarlos/research/rep/sns-crawler/slave/"

    def processNode(self, infoFile, item):
        with open(infoFile, "r") as html_doc:
            u = UserInfo(html_doc)
            u.parse()
            print "Parsed information for node: %s" % item
            u.printAttr()
            mongoU.insertNode(self.db, u, self.TIME_ID, item)

    def processEdges(self, level, direction, item):
        extractInfo = 1 if direction == self.IN_EDGES_KEY else 0

        # Call html parser: direction-edges
        print "%s %s %s %s" % (self.PARSER, direction, item, self.SAMPLE)
        subprocess.check_output("/Users/josecarlos/research/rep/sns-crawler/slave/parse_pages.sh %s %s %s %s" % (
            direction,
            item,
            extractInfo,
            self.SAMPLE), shell = True)
        output="%s/%s.%s" % (self.HTML_PARSER_OUTPUT_DIR, direction, item)

        if(extractInfo == 1):
            info="%s/info.%s" % (self.HTML_PARSER_OUTPUT_DIR, item)
            self.processNode(info, self.TIME_ID, item)

        # Parse output from html parser
        snames = []
        with open(output, "r") as outputF:
            snames = set([sname.strip("\n") for sname in outputF if sname != "\n"])
        invalids = self.INVALID_IDS + [item]
        for invalid in invalids:
            try:
                snames.remove(invalid)
                print "*** Invalid item %s eliminated" % invalid
            except KeyError:
                print "*** Invalid item %s doesn't exist" % invalid

        for sname in snames:
            # Add edges to network
            if(direction == self.IN_EDGES_KEY):
                print "Adding edge to network: (%s, %s)" % (sname, item)
                mongoU.insertEdge(self.db, sname, item, self.TIME_ID)
            elif(direction == self.OUT_EDGES_KEY):
                print "Adding edge to network: (%s, %s)" % (item, sname)
                mongoU.insertEdge(self.db, item, sname, self.TIME_ID)
            # Update BFS queue with out-edges
            mongoU.updateBFSQ(self.db, sname, level)

        # Remove item from BFS queue
        mongoU.removeBFSQ(self.db, item)

    def breadth_first_search(self, level):
        print "Building network level %i" % level

        # Retrieve items from level
        items = mongoU.retrieveBFSQ(level - 1)
        for itemO in items:
            item = itemO["_id"]
            print "Processing in-edges for item: %s" % item
            self.processEdges(level, self.IN_EDGES_KEY, item)

            print "Processing out-edges for item: %s" % item
            self.processEdges(level, self.OUT_EDGES_KEY, item)

        # Proceed with next level
        if(level < self.TOP_LEVEL):
            next_level = level + 1
            print (raw_input("Continue with level: %i ?" % next_level))
            self.breadth_first_search(next_level)

    def start(self):
        LEVEL_0 = 0

        # Disable this when resuming a failed operation
        # mongoU.clearBFSQ(db)

        # Add seed node to the bfs_queue
        mongoU.updateBFSQ(db, EGO_ID, LEVEL_0)

        # Start bfs
        self.breadth_first_search(LEVEL_0 + 1)

    def __init__(self, timeId, config):
        self.timeId = timeId
        self.EGO_ID = config.get("SNS", "EgoId")
        self.INVALID_IDS = config.get("SNS", "InvalidIds").split(",")
        self.SAMPLE_SIZE = config.get("SNS", "SampleSize")
        self.TOP_LEVEL = config.getint("Parser", "TopLevel")
        self.PARSER_OUTPUT = config.get("Parser", "ParserOutput")

        self.db = mongoU.initDB();
