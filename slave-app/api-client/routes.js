var express = require('express');
var OAuth = require('oauth').OAuth;
var TwitterAPIClient = require('./TwitterAPIClient');
var util = require('util');

var router = express.Router();

router.get('/followers', function(req, res) {
  appLabel = req.query.app_label;
  screenName = req.query.screen_name;
  var config = GLOBALS.config.TwitterApplicationsLabels[appLabel];
  var client = new TwitterAPIClient(config);

  var error = function (err, response, body) {
    console.log('ERROR [%s]', err);
  };
  var success = function (data, limits) {
    console.log('Data [%s]', data);
    console.log('Data [%s]', util.inspect(limits, false, null));

    res.write(data);
    if(limits.limitRemaining > 10) {
      client.getFollowers(screenName, success, error);
      //res.write(util.inspect(limits, false, null));
    } else {
      res.end();
    }
  };
  client.getFollowers(snames, ids, success, error);
});

router.get('/profiles', function(req, res) {
  GLOBALS.logger.log("URL Received: " + req.originalUrl )
  appLabel = req.query.app_label;
  snames = req.query.snames;
  ids = req.query.ids;
  var config = GLOBALS.config.TwitterApplicationsLabels[appLabel];
  var client = new TwitterAPIClient(config);

  var error = function (err, response, body) {
    GLOBALS.logger.log('ERROR [%s]', err);
  };
  var success = function (data, limits) {
    res.send(data);
  };

  client.getProfiles(snames, ids, success, error);
});

module.exports = router;
