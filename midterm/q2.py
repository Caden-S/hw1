from mrjob.job import MRJob   # MRJob version

class EffMidtermFlight(MRJob):  #MRJob version
    def mapper_init(self):
        self.cache = {}

    def mapper(self, key, line):
        flight = line.split(",")
        if flight[0] == "ITIN_ID":
            pass
        else:
            total = float(flight[-1]) # Convert number of passengers to float
            if flight[1] == "2021":
                diff = total
            else:
                diff = -total
            if flight[3] in self.cache: # Change if in cache, else add to cache
                self.cache[flight[3]] = (self.cache[flight[3]][0] + total, self.cache[flight[3]][1] + diff) 
            else:
                self.cache[flight[3]] = (total, diff)
        
        if len(self.cache) == 100:
            for airport in self.cache:
                total = self.cache[airport][0]
                diff = self.cache[airport][1]
                yield (airport, (total, diff))
            self.cache.clear()
        else:
            pass
 
    def mapper_final(self): # Yield cache if not empty
        if len(self.cache) > 0:
            for airport in self.cache: 
                total = self.cache[airport][0]
                diff = self.cache[airport][1]
                yield(airport, (total, diff))
        self.cache.clear()

    def reducer(self, key, values):
        x, y = 0, 0
        for tup in values: # Sum the incoming and outgoing values 
            x += tup[0]   
            y += tup[1]
        yield(key, (x,y))  # Yield as tuple

if __name__ == '__main__':
    EffMidtermFlight.run()   # MRJob version
