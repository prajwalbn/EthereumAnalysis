from mrjob.job import MRJob

import time

import datetime



#Task 1 of coursework to identify the transaction count of ethereum from start to end



class Task1B(MRJob):

    def mapper(self, _, lines):

        try:

            field = lines.split(",")    # Dataset is seperated by commas, hence using comma as split delimit

            if (len(field) == 7):       #check if the particluar line has all 7 attributes, else consider that as malformed(reference from Lab 3)

                timestamp_extraction = int(field[6]) # / 1000 # Extract Day, Month and Year from timestamp to determine transaction of every month and year (refernce from Lab 3)

                transaction_value = float(field[3])  # Extract the transaction value 

                day = time.strftime("%d",time.gmtime(timestamp_extraction)) # Extract day

                month = time.strftime("%m",time.gmtime(timestamp_extraction)) # Extract month

                year = time.strftime("%Y",time.gmtime(timestamp_extraction)) #Extract year

                attr = (month, year) 

                trans_key = (1, transaction_value)

                yield(attr, trans_key)

        except:

            pass





    # Computing average value of transactions - logic reference used from Lab 3

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





if __name__ == '__main__':

    Task1B.run()





        


