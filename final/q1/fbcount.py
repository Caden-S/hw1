from mrjob.job import MRJob

class PBCount(MRJob):
  def mapper_init(self):
    self.cache = {}

  def mapper(self, key, line):
    (left, right) = line.split(" ")
    if left in self.cache:
      if int(right) > 500:
        self.cache[left] = self.cache[left] + 1
    else:
        self.cache[left] = 1
    
    if len(self.cache) > 100:
      for node in self.cache:
        yield (node, self.cache[node])
      self.cache.clear()
    else:
      pass
  
  def mapper_final(self):
    if len(self.cache) > 0:
      for node in self.cache:
        yield (node, self.cache[node])
    self.cache.clear()

  def reducer(self, key, values):
    left = key
    myval = sum(values)
    if myval > 2:
      yield (left, myval+1)

if __name__ == '__main__':
  PBCount.run()
