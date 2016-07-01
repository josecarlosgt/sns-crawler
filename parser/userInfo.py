from bs4 import BeautifulSoup

class UserInfo:
    # Constants
    CLASS_NAME="UserInfo"

    def __init__(self, soup, ip, logger):
        self.soup = soup
        self.ip = ip
        self.logger = logger

    def getContent(self, element):
        content = ""
        if(element is not None):
            content = element.next
            content = content.strip()

        return content

    def getStat(self, classStat):
        stat = ""
        statCont = self.soup.find("li",  { "class" : classStat })
        if(statCont is not None):
            statCont2 = statCont.find("span", {"class" : "ProfileNav-value"})
            stat = self.getContent(statCont2)

        return stat

    @staticmethod
    def getNumber(text):
        text = text.replace(',', '').lower()
        value = 0
        if "k" in text:
            value = float(text.replace('k', '')) * 1000
        elif "m" in text:
            value =  float(text.replace('m', '')) * 1000000
        else:
            try:
                value = float(text)
            except ValueError:
                pass

        return int(value)

    def parse(self):
        # Number of tweets, followers, followees & favourites
        tweets = self.getStat("ProfileNav-item--tweets")
        self.tweets = self.getNumber(tweets)

        following = self.getStat("ProfileNav-item--following")
        self.following = self.getNumber(following)

        followers = self.getStat("ProfileNav-item--followers")
        self.followers = self.getNumber(followers)

        favourites = self.getStat("ProfileNav-item--favorites")
        self.favourites = self.getNumber(favourites)

        # user name & id
        nameCont = self.soup.find("a",  { "class" : "ProfileHeaderCard-nameLink" })
        self.name = self.getContent(nameCont)

        idCont = self.soup.find("a",  { "class" : "ProfileHeaderCard-screennameLink" })
        self.id = ""
        if(idCont is not None):
            idCont2 = idCont.find("span",  { "class" : "u-linkComplex-target" })
            self.id = self.getContent(idCont2)

        # Image URL
        imageCont = self.soup.find("img",  { "class" : "ProfileAvatar-image" })
        self.image = ""
        if(imageCont is not None):
            self.image = imageCont.get("src")

        # Description: NOT PARSED CORRECTLY, PROBABLY ENCODING ISSUES
        #descCont = soup.find("p",  { "class" : "ProfileHeaderCard-bio" })
        #self.desc = getContent(descCont)
        #print self.desc

        # Website
        webSiteCont = self.soup.find("span",  { "class" : "ProfileHeaderCard-urlText" })
        self.webSite = ""
        if(webSiteCont is not None):
            webSiteCont2 = webSiteCont.find("a",  { "class" : "u-textUserColor" })
            self.webSite = self.getContent(webSiteCont2)

        # Location
        locationCont = self.soup.find("span",  { "class" : "ProfileHeaderCard-locationText" })
        self.location = ""
        if(locationCont is not None):
            locationCont2 = locationCont.find("a")
            self.location = self.getContent(locationCont2)

        # Protected timeline
        protectedCont = self.soup.find("div",  { "class" : "ProtectedTimeline" })
        self.private = False
        if(protectedCont is not None):
            protectedCont2 = protectedCont.find("h2",  { "class" : "ProtectedTimeline-heading" })
            if(protectedCont2 is not None):
                self.private = True

        self.printAttr()

    def getFollowers(self):
        return self.followers

    def printAttr(self):
        self.logger.info("Parsed information for node: %s" % self.id)
        self.logger.info("\t# Tweets: %s" % self.tweets)
        self.logger.info("\t# Follwing: %s" % self.following)
        self.logger.info("\t# Followers: %s" % self.followers)
        self.logger.info("\t# Favourites: %s" % self.favourites)
        self.logger.info("\tName: %s" % self.name)
        self.logger.info("\tId: %s" % self.id)
        self.logger.info("\tImage: %s" % self.image)
        self.logger.info("\tWebsite: %s" % self.webSite)
        self.logger.info("\tLocation: %s" % self.location)
        self.logger.info("\Private: %s" % self.private)
