# pip install geopy

from geopy.geocoders import GoogleV3
import sys
from os import listdir

reload(sys)
sys.setdefaultencoding('utf8')

# This script geocodes the locations obtained from new followers

# Global Variables

locations = "/home/jarr850/output/d_followers_location"
geocodes = "/home/jarr850/output/d_followers_geocode"

# Geocoding

geolocator = GoogleV3()

for location in listdir(locations):
	print location
	f_geocodes = "%s/%s" % (geocodes, location.replace("locations", "geocodes"));
	f_allgeo = "%s/%s" % (geocodes, location.replace("locations", "allinfo"));
	fo_geocodes = open(f_geocodes, "w")
	fo_allgeo = open(f_allgeo, "w")

	# Get locations
	f_locations = [line.strip() for line in open("%s/%s" % (locations,location), 'r')] 
	# print f_locations

	for f_location in f_locations:
		print f_location
		#geo = geolocator.geocode(location)
		#if geo is not None:
		#	address, (latitude, longitude) = geo
		#	locationUTF8 = location.encode('utf8');
		#	addressUTF8 =  address.encode('utf8');
		#	result =  "RAW LOCATION: %s | Parsed location: %s | (lat, long): @[%s,%s]" % (locationUTF8, addressUTF8, latitude, longitude)
		#	print result;
		#	fo.write(result + '\n');
		fo_geocodes.write(f_location + '\n');
		fo_allgeo.write(f_location + '\n');

	fo_geocodes.close()
	fo_allgeo.close()

