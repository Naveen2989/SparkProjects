

import datetime as dt
import sys
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import*
from pyspark.sql import*
from pyspark_cassandra.conf import*
#import pyspark_cassandra.streaming
import pyspark_cassandra
from pyspark_cassandra import CassandraSparkContext, RowFormat, Row, UDT
# $example off:programmatic_schema$
"""
A simple example demonstrating Spark SQL Hive integration.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/hive.py
"""

if __name__ == "__main__":
    
    conf = SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Spark Cassandra")
    conf.set("spark.cassandra.connection.host","localhost")
#    conf.set("spark.cassandra.connection.port",'9042')
    sc = CassandraSparkContext(conf=conf)
    assert sys.version_info >= (3, 5) 
    pyspark_cassandra.monkey_patch_sc(sc)
    users = sc.cassandraTable("demo", "user" ,row_format=pyspark_cassandra.RowFormat.DICT) 
#    .select("name","favorite_food") \
#    .map(lambda r: (r["name"], 1)) \
#    .reduceByKey(lambda a, b: a + b) \
#    .collect()	

    sc.stop()
