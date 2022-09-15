from mrjob.job import MRJob   # MRJob version

# Change the class name!!
class WordCount(MRJob):  #MRJob version
    def mapper(self, key, line):
        words = line.split()
        for w in words:
            yield (len(w), 1) # Yield length of word and 1 instead
    def reducer(self, key, values):
        yield (key, sum(values)) # No change

if __name__ == '__main__':
    WordCount.run()   # MRJob version
