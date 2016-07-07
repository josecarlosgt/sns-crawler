var OAuth = require('oauth').OAuth;

function TwitterAPIClient(config) {
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

TwitterAPIClient.prototype.test = function () {
  var msg = "TESTING Twitter API client\n";
  msg = msg + "Authentication: "+ this.oauth + "\n";
  msg = msg + "Response: "+ this.res + "\n";

  return msg;
}

TwitterAPIClient.prototype.getProfiles = function (snames, ids, success, error) {
  var path = "/users/lookup.json?screen_name=" + snames + "&user_id=" + ids;
  var url = this.baseUrl + path;
  this.doRequest(url, success, error);
};

TwitterAPIClient.prototype.doRequest = function (url, success, error) {
  this.oauth.get(url, this.accessToken, this.accessTokenSecret, function (err, body, response) {
    if (!err && response.statusCode == 200) {
      limits = {
        "limit": response.headers['x-rate-limit-limit'],
        "limitRemaining": response.headers['x-rate-limit-remaining'],
        "x-rate-limit-reset": response.headers['x-rate-limit-reset'],
      };
      success(body, limits);
    } else {
      error(err, response, body);
    }
  });
};

module.exports = TwitterAPIClient;
