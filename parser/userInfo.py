from bs4 import BeautifulSoup

class UserInfo:

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

    def __init__(self, html):
        self.soup = BeautifulSoup(html, 'html.parser')

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

    def printAttr(self):
        print "# Tweets: %s" % self.tweets
        print "# Follwing: %s" % self.following
        print "# Followers: %s" % self.followers
        print "# Favourites: %s" % self.favourites
        print "Name: %s" % self.name
        print "Id: %s" % self.id
        print "Image: %s" % self.image
        print "Website: %s" % self.webSite
        print "Location: %s" % self.location
