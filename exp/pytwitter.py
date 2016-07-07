from requests_oauthlib import OAuth1
import requests
import json

consumerKey = "HYRDwyXR0lInQE5GtMGbVCRtf"
consumerSecret = "GqXn4vZ0UWc6P286hxhxUpSglteskQ1WEuyaHP5QnzSyjPLUMQ"
accessToken = "4569200370-SJmvtt7H1qLrc7EfzbPxWta9UAPYqKiX9k0quCo"
accessTokenSecret = "S0OAVja67nx3xXVs5KSXJNfENpoWovqBUDG6wBXL874s4"

oauth = OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret)

url="https://api.twitter.com/1.1/followers/ids.json?stringify_ids=true&user_id=3821978235"
resp = requests.get(url, auth=oauth)
print resp

data = resp.content.decode('utf-8')
print data

dataO = json.loads(data)
print dataO

#print "id: " + str(dataO[0]["id"])

print resp.headers

print "Limit: " + resp.headers['x-rate-limit-limit']
print "Limit remaining: " + resp.headers['x-rate-limit-remaining']
