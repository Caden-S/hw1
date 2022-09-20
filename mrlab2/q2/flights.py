from mrjob.job import MRJob   # MRJob version

class Flight(MRJob):  #MRJob version
    def mapper(self, key, line):
        flight = line.split(",")
        if flight[0] == "ITIN_ID":
            pass
        else:
            num = float(flight[-1]) # Convert number of passengers to float
            yield(flight[3], (0,num)) # Yield a tuple for both the origin and destination
            yield(flight[5], (num,0))

    def reducer(self, key, values):
        x, y = 0, 0
        for tup in values: # Sum the incoming and outgoing values (for some reason
            x += tup[0]    # list comprehension did not work)
            y += tup[1]
        yield(key, (x,y))  # Yield as tuple

if __name__ == '__main__':
    Flight.run()   # MRJob version
