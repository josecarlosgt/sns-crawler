var http = require('http');
var fs = require('fs');
var url = require('url');
var express = require('express');
var exec = require('child_process').exec;
var Console = require('console').Console;

// Global Settings

var config = JSON.parse(fs.readFileSync('../configuration.json.base', 'utf8'));
var logFile = config.SlaveLogging.webServerLogFile;

// Logging

const output = fs.createWriteStream(logFile);
const errorOutput = fs.createWriteStream(logFile);
const logger = new Console(output, errorOutput);

// Constants

const port = 3000;
const PARSER = "./parse_pages.sh";

// Create a server
var app = express();
app.get('/*', function (req, res) {
   logger.log("Request for " + req.originalUrl + " received");

   user = req.path.replace('/','');
   errorMsg = "";
   if(user == "") {
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
      logger.log("Processing request for user: <" + user + "> " +
        "sample size: <" + sampleSize + "> " +
        "direction: <" + direction + "> " +
        "userInfo: <" + userInfo + ">");

      parserCall = PARSER + " " + direction  + " " +
        user + " " + userInfo + " " + sampleSize;
      maxBuffer = 1024*1024;
      logger.log("EXECUTING PARSER: " + parserCall);
      exec(parserCall, {maxBuffer: maxBuffer }, (error, stdout, stderr) => {
        if (error) {
          logger.error("exec error: ${error}");
          res.status(500).send(error);
        } else {
          res.send(stdout);
        }
      });
   } else {
     logger.log(errorMsg);
     res.status(500).send(errorMsg);
   }
});

var server = app.listen(port, function () {
  var host = server.address().address
  var port = server.address().port

  logger.log("Server running at http://%s:%s", host, port)
});
server.timeout = 1000 * 3600;
