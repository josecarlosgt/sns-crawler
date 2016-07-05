var express = require('express');
var fs = require('fs');

var api_client = require('./api-client/routes');
var website_crawler = require('./website-crawler/routes');

var app = express();

// Clear log: Avoid log rotation for the moment
app.get('/clear-log', function (req, res) {
  var crawlerLogFile = GLOBALS.config.SlaveLogging.crawlerLogFile;
  fs.writeFileSync(crawlerLogFile, "");

  GLOBALS.output.end()
  GLOBALS.errorOutput.end()
  fs.writeFileSync(GLOBALS.config.SlaveLogging.webServerLogFile, "");
  GLOBALS.createLogger();

  res.send("OK");
});

app.use('/api-client', api_client);
app.use('/website-crawler', website_crawler);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

// error handler
app.use(function(err, req, res, next) {
    res.status(err.status || 500);
    res.send(err.message);
});

module.exports = app;
