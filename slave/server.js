var http = require('http');
var fs = require('fs');
var url = require('url');
var express = require('express');
var exec = require('child_process').exec;
var Console = require('console').Console;

// Global Settings

var GLOBALS = {
  config: JSON.parse(fs.readFileSync('../configuration.json.base', 'utf8')),
  port: 3000,
  parser: "./parse_pages.sh"
}

var createLog = function(){
  GLOBALS.output = fs.createWriteStream(GLOBALS.config.SlaveLogging.webServerLogFile);
  GLOBALS.errorOutput = fs.createWriteStream(GLOBALS.config.SlaveLogging.webServerLogFile);
  GLOBALS.logger = new Console(GLOBALS.output, GLOBALS.errorOutput);
}
createLog();

// Create a server
var app = express();
app.get('/clear-log', function (req, res) {
  var crawlerLogFile = GLOBALS.config.SlaveLogging.crawlerLogFile;
  fs.writeFileSync(crawlerLogFile, "");

  GLOBALS.output.end()
  GLOBALS.errorOutput.end()
  fs.writeFileSync(GLOBALS.config.SlaveLogging.webServerLogFile, "");
  createLog();

  res.send("OK");
});

app.get('/connections', function (req, res) {
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
