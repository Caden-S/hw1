from mrjob.job import MRJob   # MRJob version

class MidtermFlight(MRJob):  #MRJob version
    def mapper_init(self):
        self.cache = {}

    def mapper(self, key, line):
        flight = line.split(",")
        if flight[0] == "ITIN_ID":
            pass
        else:
            total = float(flight[7]) # Convert number of passengers to float
            pair = (flight[3], flight[6])
            total_2021 = 0
            total_2022 = 0

            if flight[1] == "2021":
                total_2021 = total
            else:
                total_2022 = total

            if pair in self.cache:
                self.cache[pair] = (self.cache[pair][0] + total_2021, self.cache[pair][1] + total_2022)
            else: 
                self.cache[pair] = (total_2021, total_2022)

        if len(self.cache) > 500:
            for key in self.cache:
                yield key, (self.cache[key][0], self.cache[key][1])
            self.cache.clear()
        else:
            pass
    
    def mapper_final(self):
        if len(self.cache) > 0:
            for key in self.cache:
                yield key, (self.cache[key][0], self.cache[key][1])
        self.cache.clear()
    
    def reducer(self, key, values):
        x, y = 0, 0
        for tup in values: # Sum the incoming and outgoing values (for some reason
            x += tup[0]    # list comprehension did not work)
            y += tup[1]
        yield(key, (x,y))  # Yield as tuple

if __name__ == '__main__':
    MidtermFlight.run()   # MRJob version
