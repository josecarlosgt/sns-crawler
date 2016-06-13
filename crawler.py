import datetime
import ConfigParser

from master.master import Master

today = datetime.date.today()
ftoday = today.strftime('%d_%m_%Y')
print "Running parser with time: %s " % ftoday

config = ConfigParser.RawConfigParser()
config.read('./configuration.cfg')

print "-- Checking configuration --"
print "-- SNS Section"
print "Invalid IDs: %s" % (config.get("SNS", "InvalidIds").split(","))
print "Ego ID: %s" % (config.get("SNS", "EgoId"))
print "Sample Size: %s" % (config.get("SNS", "SampleSize"))
print ""
print "-- Parser Section"
print "Top Level: %i" % (config.getint("Parser", "TopLevel"))
print "Parser Output: %s" % (config.get("Parser", "ParserOutput"))

master = Master(ftoday, config)
master.start()
