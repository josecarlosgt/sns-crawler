var OAuth = require('oauth').OAuth;

//exports.TwitterAPIClient = Twitter;

function APIClient(config) {
  this.consumerKey = config.consumerKey;
  this.consumerSecret = config.consumerSecret;
  this.accessToken = config.accessToken;
  this.accessTokenSecret = config.accessTokenSecret;
  this.baseUrl = 'https://api.twitter.com/1.1';
  this.oauth = new OAuth(
      'https://api.twitter.com/oauth/request_token',
      'https://api.twitter.com/oauth/access_token',
      this.consumerKey,
      this.consumerSecret,
      '1.0',
      null,
      'HMAC-SHA1'
  );
}

APIClient.prototype.test = function () {
  var msg = "TESTING Twitter API client\n";
  msg = msg + "Authentication: "+ this.oauth;

  return msg;
}

APIClient.prototype.getFollowers = function (screenName, success, error) {
  var path = '/followers/ids.json?count=5000&stringify_ids=true&screen_name=' + screenName;
  var url = this.baseUrl + path;
  this.doRequest(url, success, error);
};

APIClient.prototype.doRequest = function (url, success, error) {
  this.oauth.get(url, this.accessToken, this.accessTokenSecret, function (err, body, response) {
    console.log(err);
    console.log(response.statusCode);

    if (!err && response.statusCode == 200) {
      limits = {
        "x-rate-limit-limit": response.headers['x-rate-limit-limit'],
        "x-rate-limit-remaining": response.headers['x-rate-limit-remaining'],
        "x-rate-limit-reset": response.headers['x-rate-limit-reset'],
      };
      success(body, limits);
    } else {
      error(err, response, body);
    }
  });
};
