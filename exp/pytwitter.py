from requests_oauthlib import OAuth1
import requests
import json

consumerKey = "HYRDwyXR0lInQE5GtMGbVCRtf"
consumerSecret = "GqXn4vZ0UWc6P286hxhxUpSglteskQ1WEuyaHP5QnzSyjPLUMQ"
accessToken = "4569200370-SJmvtt7H1qLrc7EfzbPxWta9UAPYqKiX9k0quCo"
accessTokenSecret = "S0OAVja67nx3xXVs5KSXJNfENpoWovqBUDG6wBXL874s4"

oauth = OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret)

# url="https://api.twitter.com/1.1/followers/ids.json?stringify_ids=true&user_id=3821978235"
# resp = requests.get(url, auth=oauth)

# data = resp.content.decode('utf-8')
# print data

url="https://api.twitter.com/1.1//users/lookup.json"
resp = requests.post(url, auth=oauth, data ={'screen_name': 'xjosecarlosgt05'})
print resp

respO = resp.json()

if(type(respO) is list):
	print "PROFILES RECEIVED"

print respO

print resp.headers

print "Limit: " + resp.headers['x-rate-limit-limit']
print "Limit remaining: " + resp.headers['x-rate-limit-remaining']
