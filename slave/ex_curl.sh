CURRENT_SNAME=$1; shift
CONN=$1; shift
REQ_TYPE=$1; shift

resp=""

if [ "$REQ_TYPE" = "main" ]; then

resp=$(curl --silent "https://twitter.com/$CURRENT_SNAME/$CONN/" -H 'accept-encoding: gzip, deflate, sdch' -H 'accept-language: en-US,en;q=0.8,es;q=0.6' -H 'upgrade-insecure-requests: 1' -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36' -H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'cache-control: max-age=0' -H 'authority: twitter.com' -H 'cookie: guest_id=v1%3A144806328621796486; pid="v3:1448590025945159770821821"; eu_cn=1; kdt=jeMBELVYc1oFmz19jMK66tpT8MTYocAKQxcYHTZT; remember_checked_on=1; auth_token=570ce31add43b2e7bf51646c66bfefbb97e70bcb; lang=en; _ga=GA1.2.1615090613.1451279788; _gat=1; _twitter_sess=BAh7CSIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7ADoPY3JlYXRlZF9hdGwrCJnpaTxUAToMY3NyZl9p%250AZCIlOGRkZDMzMTMwYWQ0ZGEwODViYTc4ZDMxNmNiZTM0MDg6B2lkIiVkNmM3%250AMjA1OTQ3MzhlODU3YzBhNDQ4OTdiOTFkZGE4NQ%253D%253D--8d947442d2cf5cd597fe71b563cf27c2621590e9; ua="f5,m2,m5,rweb,msw"' -H 'referer: https://twitter.com/fsm2016wsf' --compressed)

elif [ "$REQ_TYPE" = "xhr" ]; then

MAX=$1

resp=$(curl --silent "https://twitter.com/$CURRENT_SNAME/$CONN/users?include_available_features=1&include_entities=1&max_position=$MAX&reset_error_state=false" -H 'accept-encoding: gzip, deflate, sdch' -H 'x-requested-with: XMLHttpRequest' -H 'accept-language: en-US,en;q=0.8,es;q=0.6' -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36' -H 'accept: application/json, text/javascript, */*; q=0.01' -H 'referer: https://twitter.com/fsm2016wsf/following' -H 'authority: twitter.com' -H 'cookie: guest_id=v1%3A144806328621796486; pid="v3:1448590025945159770821821"; eu_cn=1; kdt=jeMBELVYc1oFmz19jMK66tpT8MTYocAKQxcYHTZT; remember_checked_on=1; auth_token=570ce31add43b2e7bf51646c66bfefbb97e70bcb; lang=en; _gat=1; _ga=GA1.2.1615090613.1451279788; _twitter_sess=BAh7CSIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7ADoPY3JlYXRlZF9hdGwrCJnpaTxUAToMY3NyZl9p%250AZCIlOGRkZDMzMTMwYWQ0ZGEwODViYTc4ZDMxNmNiZTM0MDg6B2lkIiVkNmM3%250AMjA1OTQ3MzhlODU3YzBhNDQ4OTdiOTFkZGE4NQ%253D%253D--8d947442d2cf5cd597fe71b563cf27c2621590e9; ua="f5,m2,m5,rweb,msw"' --compressed)

# resp=$(curl --silent "https://twitter.com/$CURRENT_SNAME/followers/users?include_available_features=1&include_entities=1&max_position=$MAX&reset_error_state=false" -H 'accept-encoding: gzip, deflat    e, sdch' -H 'x-requested-with: XMLHttpRequest' -H 'accept-language: en-US,en;q=0.8,es;q=0.6' -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chr    ome/48.0.2564.97 Safari/537.36' -H 'accept: application/json, text/javascript, */*; q=0.01' -H "referer: https://twitter.com/$CURRENT_SNAME/followers" -H 'authority: twitter.com' -H 'cookie: guest_id    =v1%3A144806328621796486; pid="v3:1448590025945159770821821"; eu_cn=1; kdt=jeMBELVYc1oFmz19jMK66tpT8MTYocAKQxcYHTZT; remember_checked_on=1; auth_token=3FBE70D14D78E511A426864765FDE5767BC9FD3D; extern    al_referer="padhuUp37zjqW+wk19u8bBEwWGXaoQSZCOdOE7m99pwqs6HZbbafDA==|0"; lang=en; _ga=GA1.2.1615090613.1451279788; _twitter_sess=BAh7CSIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFza    HsABjoKQHVzZWR7ADoPY3JlYXRlZF9hdGwrCHgsxaNSAToMY3NyZl9p%250AZCIlYzUyYzc4ZDNjNmE5NzU4YTg0MzFjMzJkNTM4OWFhNWI6B2lkIiUxNWZk%250ANmI2OGNlNDlmODIzZWQ1NDMyMDhjZDgzNTk1Nw%253D%253D--d49ae07acbb14ead6d9e4629    e80a54e24e9e2b88; ua="f5,m2,m5,rweb,msw"' --compressed)

fi

echo $resp
