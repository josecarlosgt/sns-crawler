import json

from profile_parser.userInfo import UserInfo
from configKeys import ConfigKeys
from database.mongoDB import MongoDB

class Processor():

    @staticmethod
    def processEdges(config,
        logger, db,
        node, level,
        type, edges, responseIdentifier):
        logger.info("Processing <%s> edges for node: %s" %\
            (type, node))

        invalids = config[ConfigKeys.SNS][ConfigKeys.INVALID_IDS]
        invalids.append(node[MongoDB.TWITTER_ID_KEY])
        invalids.append(node[MongoDB.TWITTER_SNAME_ID_KEY])
        ''' Remove accounts that should not part of the observations that come
        in the results of the web crawler, such the origin of the connections
        (self.node) and other identifiers specified in the configuration such
        as the account used to authenticate with the Twitter website
        '''
        for invalid in invalids:
            try:
                edges.remove(invalid)
                logger.info("Removing: <%s> edge for node: %s / %s" %\
                    (invalid,
                    node[MongoDB.TWITTER_ID_KEY],
                    node[MongoDB.TWITTER_SNAME_ID_KEY]))
            except ValueError:
                pass
            except KeyError:
                pass

        # Add edges to network
        for edge in edges:
            # For in-edges (followers), the edge is an id
            if(type == ConfigKeys.IN_EDGES_KEY):
                db.insertEdge(edge, node[MongoDB.TWITTER_ID_KEY])

            # For out-edges (following), the edge is a screen name
            elif(type == ConfigKeys.OUT_EDGES_KEY):
                db.insertEdge(node[MongoDB.TWITTER_ID_KEY], edge)

            # Add edge as node to be visited at the bfs queue
            db.enqueBFSQ({responseIdentifier: edge}, level)

    '''
    def processProfile(self, info, nodeId):
        u = UserInfo(info,
            self.ip,
            Logger.clone(self.logger, UserInfo.CLASS_NAME))
        u.parse()

        return u
    '''

    @staticmethod
    def processJSONProfile(config, profile):
        newProfile = {}

        for attribute in config[ConfigKeys.SNS][ConfigKeys.PROFILE_ATTRIBUTES]:
            if attribute in profile:
                if(attribute.endswith("_count")):
                    newProfile[attribute] = UserInfo.getNumber(str(profile[attribute]))
                else:
                    newProfile[attribute] = profile[attribute]
            else:
                newProfile[attribute] = ""

        return newProfile

    '''
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
    '''
