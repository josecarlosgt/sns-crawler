var exec = require('child_process').exec;
var express = require('express');
var router = express.Router();

/* GET users listing. */
router.get('/following', function(req, res) {
  GLOBALS.logger.log("Request for " + req.originalUrl + " received");
  errorMsg = "";

  screenName = req.query.screen_name;
  if(screenName == undefined) {
    errorMsg = "Empty screen name in request. ";
  }
  cursorCount = req.query.cursor_count
  if(cursorCount == undefined) {
    cursorCount = -1;
  }
  if(errorMsg == "") {
    GLOBALS.logger.log("Processing request for user: <" + screenName + "> " +
      "cursor count: <" + cursorCount + "> ");

     parserCall = GLOBALS.config.SlaveWebService.parser + " following " +
       screenName + " " + cursorCount;
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

module.exports = router;
