import storm


class NormalizerBolt(storm.BasicBolt):

    def initialize(self, conf, context):
        self._conf = conf
        self._context = context

        self._common_words = [
            "the", "be", "a", "an", "and", "of", "to", "in", "am", "is", "are",
            "at", "not", "that", "have", "i", "it", "for", "on", "with", "he",
            "she", "as", "you", "do", "this", "but", "his", "by", "from",
            "they", "we", "her", "or", "will", "my", "one", "all", "s", "if",
            "any", "our", "may", "your", "these", "d", " ", "me", "so", "what",
            "him", "their"
        ]

        storm.logInfo("Normalizer bolt instance starting...")

    def process(self, tup):
        # TODO:
        # Task 1: make the words all lower case
        storm.logInfo("Received tuple %s" % tup)
        line = tup.values[0]
        line = line.lower()
        # Task 2: remove the common words
        if line not in self._common_words and line != " ":
            storm.logInfo("Normalized word %s" % line)
            storm.emit([line])
        # End


# Start the bolt when it's invoked
NormalizerBolt().run()
