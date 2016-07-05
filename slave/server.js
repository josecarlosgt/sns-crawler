var http = require('http');
var fs = require('fs');
var url = require('url');
var express = require('express');
var exec = require('child_process').exec;
var Console = require('console').Console;
var util = require('util');

var TwitterAPIClient = require('./twitter-api-client').APIClient;
var TwitterWebSiteCrawler = require('./twitter-api-client').WebSiteCrawler;

// Global settings
var GLOBALS = {
  API: "A",
  WEBSITE: "W",
  config: JSON.parse(fs.readFileSync('../configuration.json.base', 'utf8'))
}
GLOBALS.port = GLOBALS.config.SlaveWebService.port;

// Logging settings
var createLogger = function(){
  GLOBALS.output = fs.createWriteStream(GLOBALS.config.SlaveLogging.webServerLogFile);
  GLOBALS.errorOutput = fs.createWriteStream(GLOBALS.config.SlaveLogging.webServerLogFile);
  GLOBALS.logger = new Console(GLOBALS.output, GLOBALS.errorOutput);
}
createLogger();

// Create the web server
var app = express();

app.get('/connections', function(req, res) {
  method = req.query.method;
  if(method == GLOBALS.API) {
    appLabel = req.query.app_label;
    appConfig = GLOBALS.config.TwitterApplicationsLabels[appLabel];
    console.log(TwitterAPIClient);

    var client = new TwitterAPIClient(appConfig);
    console.log(client);
    //console.log(client.test());

  } else if (method == GLOBALS.WEBSITE) {
    var parser = GLOBALS.config.SlaveWebService.parser;
    var crawler = new TwitterWebSiteCrawler(parser);
    console.log(crawler.test());


  }
  /*
  var error = function (err, response, body) {
      console.log('ERROR [%s]', err);
  };
  var success = function (data, limits) {
      console.log('Data [%s]', data);
      console.log('Data [%s]', util.inspect(limits, false, null));
  };

  twitter.getFollowers("josecarlosgt05", success, error);
  //console.log(twitter.test());
  */
});

app.get('/clear-log', function (req, res) {
  var crawlerLogFile = GLOBALS.config.SlaveLogging.crawlerLogFile;
  fs.writeFileSync(crawlerLogFile, "");

  GLOBALS.output.end()
  GLOBALS.errorOutput.end()
  fs.writeFileSync(GLOBALS.config.SlaveLogging.webServerLogFile, "");
  createLogger();

  res.send("OK");
});

app.get('/connectionsx', function (req, res) {
   GLOBALS.logger.log("Request for " + req.originalUrl + " received");
   errorMsg = "";

   user = req.query.user;
   if(user == undefined) {
     errorMsg = "Empty user in request. ";
   }
   sampleSize = req.query.sample_size
   if(sampleSize == undefined) {
     sampleSize = -1;
   }
   direction = req.query.direction
   if(direction == undefined) {
      errorMsg = errorMsg + "Empty direction in request. ";
    }
    userInfo = req.query.user_info
    if(userInfo == undefined) {
      errorMsg = errorMsg + "Empty user_info in request. ";
    }
    if(errorMsg == "") {
      GLOBALS.logger.log("Processing request for user: <" + user + "> " +
        "sample size: <" + sampleSize + "> " +
        "direction: <" + direction + "> " +
        "userInfo: <" + userInfo + ">");

      parserCall = GLOBALS.parser + " " + direction  + " " +
        user + " " + userInfo + " " + sampleSize;
      maxBuffer = 1024*1024;
      GLOBALS.logger.log("EXECUTING PARSER: " + parserCall);
      exec(parserCall, {maxBuffer: maxBuffer }, (error, stdout, stderr) => {
        if (error) {
          GLOBALS.logger.error("exec error: ${error}");
          res.status(500).send(error);
        } else {
          res.send(stdout);
        }
      });
   } else {
     GLOBALS.logger.log(errorMsg);
     res.status(500).send(errorMsg);
   }
});

var server = app.listen(GLOBALS.port, function () {
  var host = server.address().address
  var port = server.address().port

  GLOBALS.logger.log("Server running at http://%s:%s", host, port)
});
server.timeout = 1000 * 3600;
