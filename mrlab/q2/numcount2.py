from mrjob.job import MRJob

class WordCount(MRJob):
    def mapper(self, key, line):
        words = line.split()
        for w in words:
            if len(w) > 1: # Check if word has more than one character
                yield (len(w), 1)
            else:
                continue
    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    WordCount.run()
