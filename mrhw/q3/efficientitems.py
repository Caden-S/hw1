from mrjob.job import MRJob   # MRJob version

class EfficientItems(MRJob):  #MRJob version
    def mapper_init(self):
        self.cache = {}

    def mapper(self, key, line):
        item_line = line.split("\t")
        country = item_line[-1]
        item = item_line[1]

        if item == "StockCode": # Skip first line
            pass
        else:
            num = float(item_line[3])
            if item in self.cache: # Set key to item and value to list of countries
                 self.cache[item] = (self.cache[item][0] + num, self.cache[item][1] + [country]) 
            else:
                self.cache[item] = (num, [country])

        if len(self.cache) == 100: # If cache length == 100, yield all and clear
            num = self.cache[item][0]
            countries = self.cache[item][1]
            for item in self.cache:
                yield item, (num, countries)
            self.cache.clear()
        else:
            pass

    def mapper_final(self): # If cache not empty, yield all and clear
        if len(self.cache) > 0:
            for item in self.cache:
                num = self.cache[item][0]
                countries = self.cache[item][1]
                yield item, (num, countries)
        self.cache.clear()

    def reducer(self, key, values):
        num = 0
        countries = {}
        for tup in values: # Loop through all keys, add first value to num for item quanitity
            num += tup[0]
            for country in tup[1]: # Loop through list of countries, add to dict
                if country in countries:
                    countries[country] += 1
                else:
                    countries[country] = 1
        pop = max(countries, key=countries.get) # Get key of max of dictionary
        num_countries = len(countries) # Equal to number of unique countries
        yield(key, ("(amount: {}, numcountries: {}, mostpopular: {})".format(num, num_countries, pop))) # No change

if __name__ == '__main__':
    EfficientItems.run()   # MRJob version
