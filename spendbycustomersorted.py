from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWordFrequencyCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_order,
                   reducer=self.reducer_count_order),
            MRStep(mapper=self.mapper_make_counts_key,
                   reducer = self.reducer_output)
        ]


    def mapper_get_order(self, key, line):
        (customerID, itemID, orderAmount) = line.split(',')
        yield customerID, float(orderAmount)
        
    def reducer_count_order(self, customerID, values):
        yield customerID, sum(values)

    def mapper_make_counts_key(self, customerID, count):
        yield '%08.02f'%float(count), customerID

    def reducer_output(self, count, customers):
         for customerID in customers:
            yield count, customerID


if __name__ == '__main__':
    MRWordFrequencyCount.run()
