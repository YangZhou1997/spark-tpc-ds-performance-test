from pyspark.context import SparkContext
from pyspark.sql import HiveContext
import glob
import sqlparse
import os, sys
from multiprocessing.dummy import Pool as ThreadPool
import threading

sqlpaths = glob.glob("./src/queries/*.sql")
sqlpaths.sort()

sqlidx = int(sys.argv[1])
sqlpath = sqlpaths[sqlidx]
head, tail = os.path.split(sqlpath)

sc = SparkContext(appName=tail)
hiveContext = HiveContext(sc)
df = hiveContext.sql("show databases")
df.show()
df = hiveContext.sql("use tpcds10")
df.show()

sqlstr = open(sqlpath).read()
splits = sqlparse.format(sqlstr, strip_comments=True).strip('\n;').split(';')

for i, split in enumerate(splits):
    sqlraw = split.strip('\n;').replace('limit 100', '')
    print(sqlraw)

    df = hiveContext.sql(sqlraw)
    df.show()
    print((df.count(), len(df.columns)))

    for c in df.columns:
        df = df.withColumnRenamed(c, c.replace(' ', '_').replace(',', '_').replace(';', '_').replace('{', '_').replace('}', '_')
            .replace('(', '_').replace(')', '_').replace('\n', '_').replace('\t', '_').replace('=', '_'))

    respath = 'alluxio://node0:19998/user/hive/warehouse/sqlres/' + tail.replace('sql', 'parquet')
    if i != 0:
        respath += str(i)

    df.coalesce(1).write.parquet(respath, compression='snappy', mode = 'overwrite')
    # df.write.parquet(respath, compression='snappy', mode = 'overwrite')
