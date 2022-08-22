import pyspark
import time
sc = pyspark.SparkContext()

def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False

        int(fields[3])
        int(fields[6])
        return True
    except:
        return False

trans = sc.textFile('/data/ethereum/transactions/')
trans_filter = trans.filter(is_good_line)
transactions = trans_filter.map(lambda t: (t.split(',')[2], (int(t.split(',')[6]), int(t.split(',')[3])))).persist()

scam = sc.textFile('/user/pbn01/scams.csv')
scams = scam.map(lambda s: (s.split(',')[1], s.split(',')[6]))

join_it = transactions.join(scams)

scam_cat = join_it.map(lambda c: (c[1][1], c[1][0][1]))

luc_scam = scam_cat.reduceByKey(lambda a,b: (a+b)).sortByKey()

luc_scam.saveAsTextFile('lucrative_scam1.txt')
