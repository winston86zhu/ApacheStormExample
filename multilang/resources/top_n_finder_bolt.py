import heapq
from collections import Counter

import storm


class TopNFinderBolt(storm.BasicBolt):
    # Initialize this instance
    def initialize(self, conf, context):
        self._conf = conf
        self._context = context

        storm.logInfo("Top-N bolt instance starting...")

        # TODO:
        # Task: set N
        self._nvalue = conf['topValue']
        # End
        self._countmap = {}
        self._max = 0
        self._min = 0

        # Hint: Add necessary instance variables and classes if needed

    def process(self, tup):
        '''
        TODO:
        Task: keep track of the top N words
        Hint: implement efficient algorithm so that it won't be shutdown before task finished
              the algorithm we used when we developed the auto-grader is maintaining a N size min-heap
        '''
        word = tup.values[0]
        count = int(tup.values[1])
        storm.logInfo("word %s " %word)
        storm.logInfo("count %s " %count)
        if len(self._countmap) >= self._nvalue:
            if count >= self._min:
                # if word not in self._countmap:
                #     self._countmap[word] = count
                # else:
                #     self._countmap[word] += count
                minkey = min(self._countmap, key=self._countmap.get)
                del self._countmap[minkey]
                self._countmap[word] = count
                self._min = count
                if count >= self._max:
                    self._max = count
        else:
            self._countmap[word] = count
            if count <= self._min:
                self._min = count
            elif count >= self._max:
                self._max = count
        if len(self._countmap) >= self._nvalue:
            final_string = ""
            for key in self._countmap:
                final_string += " "
                final_string += (key + ",")
            final_string = final_string.strip()[:-1]
            storm.logInfo("Top N result %s" %final_string)
            storm.emit(["top-N", final_string])




        # End


# Start the bolt when it's invoked
TopNFinderBolt().run()
