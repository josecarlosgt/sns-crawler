from bs4 import BeautifulSoup

class UserInfo:
    # Constants
    CLASS_NAME="UserInfo"

    def __init__(self, soup, logger):
        self.soup = soup
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

    def parse(self):
        # Number of tweets, followers, followees & favourites
        self.tweets = self.getStat("ProfileNav-item--tweets")

        self.following = self.getStat("ProfileNav-item--following")

        self.followers = self.getStat("ProfileNav-item--followers")

        self.favourites = self.getStat("ProfileNav-item--favorites")

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
        self.location = self.getContent(locationCont)

        self.printAttr()

    def printAttr(self):
        self.logger.info("Parsed information for node: %s" % self.id)
        self.logger.info("# Tweets: %s" % self.tweets)
        self.logger.info("# Follwing: %s" % self.following)
        self.logger.info("# Followers: %s" % self.followers)
        self.logger.info("# Favourites: %s" % self.favourites)
        self.logger.info("Name: %s" % self.name)
        self.logger.info("Id: %s" % self.id)
        self.logger.info("Image: %s" % self.image)
        self.logger.info("Website: %s" % self.webSite)
        self.logger.info("Location: %s" % self.location)
