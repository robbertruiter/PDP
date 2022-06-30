from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieRatings(MRJob):
    # Define steps for MRJob
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies, combiner=self.combiner_movies, reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sort_by_rating)
        ]

    # For each line of the file, return the movieID
    def mapper_get_movies(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1
    
    # Combine the rating and movieID
    def combiner_movies(self, rating, counts):
        yield rating, sum(counts)

    # Reduce the list by counting the ratings per movieID
    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), key)

    # Sort the movieID by rating high->low
    def reducer_sort_by_rating(self, _, rating_counts):
        for count, key in sorted(rating_counts, reverse=True):
            yield (key, int(count))

if __name__ == '__main__':
    MovieRatings.run()