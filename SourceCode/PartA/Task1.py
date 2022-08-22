from mrjob.job import MRJob

import time

import datetime



#Task 1 of coursework to identify the transaction count of ethereum from start to end



class Task1(MRJob):

    def mapper(self, _, lines):

        try:

            field = lines.split(",") # Dataset is seperated by commas, hence using comma as split delimit

            if (len(field) == 7): #check if the particluar line has all 7 attributes, else consider that as malformed(reference from Lab 3)

                timestamp_extraction = int(field[6]) # / 1000 # Extract Day, Month and Year from timestamp to determine transaction of every month and year (refernce from Lab 3)

                day = time.strftime("%d",time.gmtime(timestamp_extraction)) # Extract day

                month = time.strftime("%m",time.gmtime(timestamp_extraction)) # Extract month

                year = time.strftime("%Y",time.gmtime(timestamp_extraction)) #Extract year

                attr = (month, year)

                yield(attr, 1)

        except:

            pass



    def combiner(self, attrr, no_of_trans):

        yield(attrr, sum(no_of_trans))



    def reducer(self, attrr, no_of_trans):

        yield(attrr, sum(no_of_trans))



if __name__ == '__main__':

    Task1.run()





        


