# import more_itertools
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEX = re.compile(r"[\w]+")

class MRMaxFreq(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper_post,
                   reducer=self.reducer_post)
        ]

    def mapper(self, _, line):
        for word in WORD_REGEX.findall(line):
            yield (word.lower(), 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))


    # keys: None, values: (word, word_count)
    def mapper_post(self, word, word_count):
        yield (None, (word, word_count))

    # sort list of (word, word_count) by word_count
    def reducer_post(self, _, word_count_pairs):
        
        from operator import itemgetter
        yield(max(word_count_pairs,key=itemgetter(1)))
        # yield (word_count_pairs)
        #for key, val in more_itertools.pairwise(word_count_pairs):
            #print(key, val)
        #print(str(word_count_pairs))

if __name__ == "__main__":
    MRMaxFreq().run()
