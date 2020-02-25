from mrjob.job import MRJob 
 
class MRUserMovieReviews(MRJob): 
 
    def mapper(self, _, line):         
        (userid, movieid, rating, timestamp) = line.split(",")         
		yield userid,movieid 
 
    def reducer(self, userid, movies):         
		yield userid, len(list(dict.fromkeys(movies))) 
 
if __name__ == '__main__':     
	MRUserMovieReviews.run() 
 
# NOTE: dict.fromkeys(movies) will remove any duplicates if user has reviewed same movie more than once in the data. 