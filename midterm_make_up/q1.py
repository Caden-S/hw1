from mrjob.job import MRJob   # MRJob version

class MidtermFlight(MRJob):  #MRJob version
    def mapper(self, key, line):
        flight = line.split(",")
        if flight[0] == "ITIN_ID":
            pass
        else:
            total = float(flight[7]) # Convert number of passengers to float
            total_2021 = 0
            total_2022 = 0
            if flight[1] == "2021":
                total_2021 = total
            else:
                total_2022 = total
            yield((flight[3], flight[6]), (total_2021, total_2022)) # Yield a tuple for both the origin and destination
    
    def reducer(self, key, values):
        x, y = 0, 0
        for tup in values: # Sum the incoming and outgoing values (for some reason
            x += tup[0]    # list comprehension did not work)
            y += tup[1]
        yield(key, (x,y))  # Yield as tuple

if __name__ == '__main__':
    MidtermFlight.run()   # MRJob version
