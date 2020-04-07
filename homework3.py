import sys
import os
from pyspark import SparkContext
sc = SparkContext()

import csv




def mapper1(partitionId,records):
    if partitionId==0:
        next(records)
    import csv
    reader=csv.reader(records)
    for r in reader:
        if len(r)==18 and r[17]!='N/A':
            yield ((r[1].lower(),int(r[0][:4])),1)

def mapper2(partitionId,records):
    if partitionId==0:
        next(records)
    import csv
    reader=csv.reader(records)
    for r in reader:
        if len(r)==18 and r[17]!='N/A':
            yield ((r[1].lower(),int(r[0][:4])),set([r[7]]))
            
def mapper3(partitionId,records):
    if partitionId==0:
        next(records)
    import csv
    reader=csv.reader(records)
    for r in reader:
        if len(r)==18 and r[17]!='N/A':
            yield ((r[1].lower(),int(r[0][:4]),r[7]),set([r[17]]))

Complaint_data = sys.argv[1]
complain = sc.textFile(Complaint_data, use_unicode=False).cache()
            
if __name__ == '__main__':

    total=complain.mapPartitionsWithIndex(mapper1).reduceByKey(lambda x,y:x+y)

    company=complain.mapPartitionsWithIndex(mapper2)\
          .reduceByKey(lambda x,y: x | y ) \
          .map(lambda x: (x[0],len(x[1])))

    max_comp=complain.mapPartitionsWithIndex(mapper3)\
    .reduceByKey(lambda x,y: x | y)\
    .map(lambda x:(x[0][:2],len(x[1])))\
    .reduceByKey(lambda x, y: max(x,y))

    ratio=max_comp.join(total)
    max_ratio=ratio.mapValues(lambda x:int(round(x[0]/x[1]*100,0)))

    final=total.join(company).join(max_ratio)
    final.map(lambda x :(x[0][0],x[0][1],x[1][0][0],x[1][0][1],x[1][1])).saveAsTextFile(sys.argv[2])




 