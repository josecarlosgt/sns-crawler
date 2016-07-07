var express = require('express');
var OAuth = require('oauth').OAuth;
var TwitterAPIClient = require('./TwitterAPIClient');
var util = require('util');

var router = express.Router();

router.get('/profiles', function(req, res) {
  GLOBALS.logger.log("URL Received: " + req.originalUrl )

  appLabel = req.query.app_label;
  snames = req.query.snames;
  ids = req.query.ids;

  var config = GLOBALS.config.TwitterApplicationsLabels[appLabel];
  var client = new TwitterAPIClient(config);

  var error = function (err, response, body) {
    GLOBALS.logger.error('ERROR [%s]', err);
  };
  var success = function (data, limits) {
    res.send(data);
  };

  client.getProfiles(snames, ids, success, error);
});

module.exports = router;
