from mrjob.job import MRJob
import time

class Gas_Top10(MRJob):

    def mapper(self,_,lines):
        try:
            field = lines.split(',')
            timestamp_extraction = int(field[2][:-1])
            #gas_price = float(field[5]) #Extracts gas price to calculate Average
            #day = time.strftime("%d",time.gmtime(timestamp_extraction)) # Extract day
            month_year = time.strftime("%m-%y",time.gmtime(timestamp_extraction)) # Extract month
            #year = time.strftime("%Y",time.gmtime(timestamp_extraction)) #Extract year
            address = field[0]
            yield((address,month_year),(int(field[1][3:]),1))



        except:
            pass

    def combiner(self,key,val):
        count = 0
        total = 0
        for v in val:
            count+= v[0]
            total= v[1]

        yield (key,(count,total))

    def reducer(self,key,val):
        count = 0
        total = 0
        for v in val:
            count += v[0]
            total = v[1]

        yield (key, (count,total))

if __name__=='__main__':
    Gas_Top10.run()
