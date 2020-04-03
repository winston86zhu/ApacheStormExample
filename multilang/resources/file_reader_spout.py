# import os
# from os.path import join
from time import sleep

# from streamparse import Spout
import storm


class FileReaderSpout(storm.Spout):

    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        self._complete = False

        storm.logInfo("Spout instance starting...")

        # TODO:
        # Task: Initialize the file reader
        self._path = conf['input']
        self._file_reader = open(self._path, 'r')
        # End

    def nextTuple(self):
        # TODO:
        # Task 1: read the next line and emit a tuple for it
        # Task 2: don't forget to sleep for 1 second when the file is entirely read to prevent a busy-loop
        lines = self._file_reader.readlines()
        for line in lines:
            storm.logInfo("Reading line %s " %line)
            storm.emit([line])
        # line = self._file_reader.readline()
        # while line:
        #     storm.logInfo("Reading line %s " % line)
        #     storm.emit([line])
        #     line = self._file_reader.readline()
        sleep(1)
        # End


# Start the spout when it's invoked
FileReaderSpout().run()
