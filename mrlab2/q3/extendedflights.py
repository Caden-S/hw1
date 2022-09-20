from mrjob.job import MRJob   # MRJob version

class Flight(MRJob):  #MRJob version
    def mapper(self, key, line):
        flight = line.split(",")
        if flight[0] == "ITIN_ID":
            pass
        else:
            num = float(flight[-1]) # Convert number of passengers to float
            yield(flight[3], float(flight[-1])) # Yield both state and airport the same number of passengers
            yield(flight[4], float(flight[-1]))

    def reducer(self, key, values):
        yield(key, sum(values)) # No change
if __name__ == '__main__':
    Flight.run()   # MRJob version
