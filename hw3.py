from pyspark import SparkContext
def mapper1(partitionId,records):
    if partitionId==0:
        next(records)
    import csv
    reader=csv.reader(records)
    for r in reader:
        if len(r)==18 and r[17]!='N/A':
            yield ((r[1],int('20'+r[0].split('/')[2])),1)

def mapper2(partitionId,records):
    if partitionId==0:
        next(records)
    import csv
    reader=csv.reader(records)
    for r in reader:
        if len(r)==18 and r[17]!='N/A':
            yield ((r[1],int('20'+r[0].split('/')[2])),set([r[7]]))
            
def mapper3(partitionId,records):
    if partitionId==0:
        next(records)
    import csv
    reader=csv.reader(records)
    for r in reader:
        if len(r)==18 and r[17]!='N/A':
            yield ((r[1],int('20'+r[0].split('/')[2]),r[7]),set([r[17]]))



if __name__=='__main__':
    sc = SparkContext()
    complain = sc.textFile('/Users/jianangong/Downloads/complaints_sample.csv')
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

    final=total.join(max_comp).join(max_ratio)
    final1=final.map(lambda x :(x[0][0],x[0][1],x[1][0][0],x[1][0][1],x[1][1])).collect()

    res = [list(ele) for ele in final1] 
    import csv
    with open("out5.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(res)