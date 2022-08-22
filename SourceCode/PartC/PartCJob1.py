from mrjob.job import MRJob

import re


class PartCJob1(MRJob):

    def mapper(self, _, lines):

        try:

            field=lines.split(",")

            if (len(field)==9):

                miner = field[2] # fetch the miner value 

                size = float(field[4])  # fetch the size value 

                yield(miner, size) 

        except:

            pass


    def combiner(self, key, value):

        yield(key, sum(value))


    def reducer(self, key, value):

        yield(key, sum(value))


if __name__ == '__main__':

    PartCJob1.run()
