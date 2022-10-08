from mrjob.job import MRJob   # MRJob version

class GoodFlight(MRJob):  #MRJob version
    def mapper_init(self):
        self.cache = {}

    def mapper(self, key, line):
        flight = line.split(",")
        if flight[0] == "ITIN_ID": # Skip first line
            pass
        else:
            num = float(flight[-1])
            if flight[3] in self.cache: # Inrecment departure if in the dict, else add to dict
                num_arrive = self.cache[flight[3]][0]
                num_depart = self.cache[flight[3]][1]
                tup = (num_arrive, num_depart + num)
                self.cache[flight[3]] = tup
            else:
                self.cache[flight[3]] = (0, num)
            if flight[5] in self.cache: # Increment arrival if in dict, else add to dict
                num_arrive = self.cache[flight[5]][0]
                num_depart = self.cache[flight[5]][1]
                tup = (num_arrive + num, num_depart)
                self.cache[flight[5]] = tup
            else:
                self.cache[flight[5]] = (num, 0)
            
            if len(self.cache) == 100: # If cache full, yield all in cache
                for airport in self.cache:
                    yield airport, self.cache[airport]
                self.cache.clear()
            else:
                pass

    def mapper_final(self):
        if len(self.cache) > 0: # Yield all in cache if cache not empty and < 100
            for airport in self.cache:
                yield airport, self.cache[airport]
        self.cache.clear()

    def reducer(self, key, values):
        a, d = 0, 0
        for tup in values: # Increment a and d and format output
            a += tup[0]
            d += tup[1]
        yield(key, ("(arrive: {}, leave: {})".format(a, d))) # No change

if __name__ == '__main__':
    GoodFlight.run()   # MRJob version
