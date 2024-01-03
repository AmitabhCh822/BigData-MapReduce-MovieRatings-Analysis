####Amitabh Chakravorty
####Dr. Treu
####Lab 9: Spend by Customer
####Date:11/29/2021

from mrjob.job import MRJob

class UserMovieCounter(MRJob):
    def mapper(self, key, line):
        (customerID, itemID, orderAmount) = line.split(',')
        yield customerID, float(orderAmount)
        
    def reducer(self, customerID, values):
        yield customerID, sum(values)
        
if __name__ == '__main__':
    UserMovieCounter.run()
    