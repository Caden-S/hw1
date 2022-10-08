from mrjob.job import MRJob   # MRJob version

class Items(MRJob):  #MRJob version
    def mapper(self, key, line):
        item_line = line.split("\t")
        country = item_line[-1]
        item = item_line[1]
        if item == "StockCode": # Skip first line
            pass
        else:
            num = float(item_line[3]) 
            yield item, (num, country) # Yield the item with a tuple of the quantity and the country

    def reducer(self, key, values):
        num = 0
        countries = {}
        for tup in values:
            num += tup[0]
            if tup[1] in countries: # Increment the count for the country in the dictionary
                countries[tup[1]] += 1
            else:
                countries[tup[1]] = 1
        pop = max(countries, key=countries.get) # Gets the key which has the largest value
        num_countries = len(countries) # Equal to number of unique countries per item
        yield(key, ("(amount: {}, numcountries: {}, mostpopular: {})".format(num, num_countries, pop))) # No change

if __name__ == '__main__':
    Items.run()   # MRJob version
