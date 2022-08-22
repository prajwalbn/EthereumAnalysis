import pyspark
import time
sc = pyspark.SparkContext()

def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False

        int(fields[4])
        int(fields[6])
        return True

    except:
        return False

trans = sc.textFile('/data/ethereum/transactions')
trans_filter = trans.filter(is_good_line)
transaction = trans_filter.map(lambda t: (t.split(',')[2], (int(t.split(',')[4]),int(t.split(',')[6])))).persist()
#transactions = transaction.reduceByKey(lambda a,b: (a+b))

top10 = sc.textFile('/user/pbn01/partb.txt')
top10_join = top10.map(lambda f: (f.split('-')[0], int(f.split('-')[1])))

join_it = transaction.join(top10_join)

#time_change = join_it.map(lambda tc: (tc[0], (time.strftime('%m-%Y',time.gmtime(tc[1][0])), tc[1][0])))

#luc_scam = time_change.reduceByKey(lambda a,b: (a+b)).sortByKey()

join_it.saveAsTextFile('/user/pbn01/gastop10')
