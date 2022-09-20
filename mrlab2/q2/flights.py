from mrjob.job import MRJob   # MRJob version

class Flight(MRJob):  #MRJob version
    def mapper(self, key, line):
        flight = line.split(",")
        if flight[0] == "ITIN_ID":
            pass
        else:
            num = float(flight[-1])
            yield(flight[3], [0,num])
            yield(flight[5], [num,0])

    def reducer(self, key, values):
        x, y = 0, 0
        for tup in values:
            x += tup[0]
            y += tup[1]
        yield(key, (x,y))
if __name__ == '__main__':
    Flight.run()   # MRJob version
