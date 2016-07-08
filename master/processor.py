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
    def processJSONProfile(config, db, logger, profile0):
        profile = {}

        for attribute in config[ConfigKeys.SNS][ConfigKeys.PROFILE_ATTRIBUTES]:
            if attribute in profile0:
                if(attribute.endswith("_count")):
                    profile[attribute] = UserInfo.getNumber(str(profile0[attribute]))
                else:
                    profile[attribute] = profile0[attribute]
            else:
                profile[attribute] = ""

        logger.info("PROFILE PARSED: %s" % profile)

        valid = True
        if(profile["friends_count"] <= config[ConfigKeys.SNS][ConfigKeys.MAX_DEGREE] and\
            profile["followers_count"] <= config[ConfigKeys.SNS][ConfigKeys.MAX_DEGREE]):

            if profile["protected"] == False:
                db.updateBFSQ(profile)
            else:
                logger.info("Profile is protected %s / %s" %\
                    (profile["id_str"], profile["screen_name"]))
                valid = False

        else:
            logger.info("Profile <%s> exceeded degree limit: %s,%s" %\
                (profile[MongoDB.TWITTER_SNAME_ID_KEY],
                    profile["friends_count"],
                    profile["followers_count"]))
            valid = False

        # Insert node anyway to keep record of every element in the network
        db.insertNode(profile)

        if not valid:
            # Invalid profiles should not be analyzed later
            db.removeBFSQ(profile)
