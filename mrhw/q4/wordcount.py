from mrjob.job import MRJob   # MRJob version

class WordCount(MRJob):  #MRJob version
    def mapper_init(self):
        self.cache = {}

    def mapper(self, key, line):
        words = line.split()
        for word in words: # Increment word if in cache, else add to dict
            if word in self.cache:
                self.cache[word] += 1
            else:
                self.cache[word] = 1

        if len(self.cache) == 100: # Yield all in cache, clear cache
            for word in self.cache:
                yield word, self.cache[word]
            self.cache.clear()
        else:
            pass

    def mapper_final(self): # Yield all in cache if not empty
        if len(self.cache) > 0:
            for word in self.cache:
                yield word, self.cache[word]
        self.cache.clear()
    
    def reducer_init(self):
        self.cache = {}

    def reducer(self, key, values): # Add all words to cache
        if key in self.cache:
            self.cache += sum(values)
        else:
            self.cache[key] = sum(values)
    
    def reducer_final(self): 
        freq = max(self.cache, key=self.cache.get) # Get most common word in reducer
        for word in self.cache: # Yield all words in cache
            yield word, self.cache[word]
        yield "MostFrequent", freq # Yield most frequent word

if __name__ == '__main__':
    WordCount.run()   # MRJob version
