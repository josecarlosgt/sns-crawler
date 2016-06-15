var http = require('http');
var fs = require('fs');
var url = require('url');
const exec = require('child_process').exec;
const Console = require('console').Console;

// Global Settings

var config = JSON.parse(fs.readFileSync('../configuration.json', 'utf8'));
var logFile = config.SlaveLogging.webServerLogFile;

var date = new Date();
logFile = logFile +
  date.getDate() + '_' +
  date.getMonth() + '_' +
  date.getFullYear()

// Logging

const output = fs.createWriteStream(logFile);
const errorOutput = fs.createWriteStream(logFile);
const logger = new Console(output, errorOutput);

// Constants

const port = 3000;
const IN_EDGES_KEY = "followers";
const OUT_EDGES_KEY = "following";
const PARSER = "./parse_pages.sh";

var callback = function(error, stdout, response) {
  if (error) {
    logger.error(`exec error: ${error}`);
  }
  response.write(stdout);
}

// Create a server
http.createServer( function (request, response) {
   // Parse the request containing file name
   var urlParts = url.parse(request.url, true);

   // Print the name of the file for which request is made.
   logger.log("Request for " + urlParts.pathname + " received.");
   user = urlParts.pathname.substr(1);
   if(user == "") {
     logger.log("Empty user in request");
     response.writeHead(404, {'Content-Type': 'text/html'});

   } else {
    sampleSize = urlParts.query.sample_size
    if(sampleSize == undefined) {
     sampleSize = -1;
    }
    logger.log("Processing request for user: <" + user + "> sample size: <" +
      sampleSize + ">");

    // 1 -> Include user information
    inEdges = PARSER + " " + IN_EDGES_KEY  + " " + user + " 1 " + sampleSize;
    outEdges = PARSER + " " + OUT_EDGES_KEY + " " + user + " 0 " + sampleSize;
    maxBuffer = 1024*1024;
    response.writeHead(200, {'Content-Type': 'text/html'});
    exec(inEdges, {maxBuffer: maxBuffer }, (error, stdout, stderr) => {
        callback(error, stdout, response);
        exec(outEdges, {maxBuffer: maxBuffer }, (error, stdout, stderr) => {
            callback(error, stdout, response);
            response.end();
        });
    });
   }
}).listen(port);

// logger will print the message
logger.log('Server running at localhost on port: ' + port);
