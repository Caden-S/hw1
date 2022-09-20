from mrjob.job import MRJob   # MRJob version

class CountryCost(MRJob):  #MRJob version
    def mapper(self, key, line):
        purchase = line.split("\t")
        if purchase[3] == "Quantity":
            pass
        else:
            yield (purchase[-1], (float(purchase[3][0:]) * float(purchase[5][0:]))) # Yield Country and quantity * price

    def reducer(self, key, values):
        yield (key, round(sum(values), 2)) # Round output to 2 decimal places

if __name__ == '__main__':
    CountryCost.run()   # MRJob version
