import copy
import logging

class Logger:

    def clone(logger, name):
        newLogger = copy.copy(logger)
        newLogger.name = name

        return newLogger

    def __init__(self, enableStdout, name, timeId):
        self.enableStdout = enableStdout
        self.name = name
        self.timeId = timeId

    def buildMessage(self, message):
        return "%s-%s> %s" % (self.name, self.timeId, message)

    def error(self, message):
        logLine = self.buildMessage(message)
        if self.enableStdout: print "ERROR: " + logLine
        logging.error(logLine)

    def info(self, message):
        logLine = self.buildMessage(message)
        if self.enableStdout: print logLine
        logging.info(logLine)
