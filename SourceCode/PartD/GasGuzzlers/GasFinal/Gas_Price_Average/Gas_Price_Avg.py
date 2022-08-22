from mrjob.job import MRJob

import time


class Gas_Price_Avg(MRJob):


    def mapper(self,_,lines):

        try:

            field = lines.split(',')

            if (len(field) == 7):

                timestamp_extraction = int(field[6])

                gas_price = float(field[5])

                day = time.strftime("%d",time.gmtime(timestamp_extraction)) # Extract day

                month = time.strftime("%m",time.gmtime(timestamp_extraction)) # Extract month

                year = time.strftime("%Y",time.gmtime(timestamp_extraction)) #Extract year

                attr = (month, year)

                gas_key = (1,gas_price)

                yield(attr, gas_key)


        except:

            pass


    def combiner(self, trans_keys, values): 

        count = 0

        total = 0

        for each_value in values:

            count+=each_value[1]

            total+=each_value[0]

        yield(trans_keys, (total,count))



    def reducer(self, trans_keys, values): 

        count = 0

        total = 0

        for each_value in values:

            count+=each_value[1]

            total+=each_value[0]

        yield(trans_keys, (count/total))



if __name__=='__main__':

    Gas_Price_Avg.run()
