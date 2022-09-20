from mrjob.job import MRJob   # MRJob version

class Flight(MRJob):  #MRJob version
    def mapper(self, key, line):
        flight = line.split(",")
        if flight[0] == "ITIN_ID":
            pass
        else:
            num = float(flight[-1])
            yield(flight[3], float(flight[-1]))
            yield(flight[4], float(flight[-1]))

    def reducer(self, key, values):
        yield(key, sum(values))
if __name__ == '__main__':
    Flight.run()   # MRJob version
