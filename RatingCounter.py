####Amitabh Chakravorty
####Dr. Treu
####In-class: Rating Counter
####Date:11/29/2021

from mrjob.job import MRJob

class UserMovieCounter(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1
        
    def reducer(self, rating, count):
        yield rating, len(list(count))
        
if __name__ == '__main__':
    UserMovieCounter.run()
    