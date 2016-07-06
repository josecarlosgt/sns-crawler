var express = require('express');
var OAuth = require('oauth').OAuth;
var TwitterAPIClient = require('./TwitterAPIClient');
var util = require('util');

var router = express.Router();

router.get('/followers', function(req, res) {
  GLOBALS.logger.log("URL Received: " + req.originalUrl )

  var appLabel = req.query.app_label;
  var userId = req.query.user_id;
  var cursorCount = req.query.cursor_count;

  var count = GLOBALS.config.Multithreading.FollowersRequestSize;
  var config = GLOBALS.config.TwitterApplicationsLabels[appLabel];
  var client = new TwitterAPIClient(config);
  var i = 0;

  var error = function (err, response, body) {
    GLOBALS.logger.error('ERROR [%s]', err);
  };
  var success = function (dataStr, limits) {
    var data = JSON.parse(dataStr);
    GLOBALS.logger.log('Response from API: ' + util.inspect(data, false, null));
    GLOBALS.logger.log('Limits in response: ' + util.inspect(limits, false, null));

    var ids = JSON.stringify(data.ids);
    //GLOBALS.logger.log('***' + (typeof data) + '***');
    //ids = "xxx*";
    res.write(ids);

    var cursor = data.next_cursor;
    i = i + 1;
    if(limits.limitRemaining > 0 && cursor != 0 &&
      (i < cursorCount || cursorCount < 0)) {
      client.getFollowers(userId, count, cursor, success, error);

    } else {
      GLOBALS.logger.log('END OF REQUEST REACHED - Iterations: ' + i +
        ' / Limit remaining: ' + limits.limitRemaining +
        ' / Cursor: ' + cursor +
        ' / Cursor count: ' + cursorCount);

      res.end();
    }
  };

  client.getFollowers(userId, count, -1, success, error);
  //data = {"ids":["1020325136","2329102256","4745404890","598715896"],"next_cursor":0,"next_cursor_str":"0","previous_cursor":0,"previous_cursor_str":"0"}
  //res.write();
  //res.end();
});

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
