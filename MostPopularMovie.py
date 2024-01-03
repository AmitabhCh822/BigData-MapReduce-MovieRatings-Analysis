from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWordFrequencyCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_get_ratings),
            MRStep(reducer = self.reducer_find_max)
        ]

    def mapper(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1
        
    def reducer_get_ratings(self, key, values):
        yield None, (sum(values), key)

    def reducer_find_max(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MRWordFrequencyCount.run()
