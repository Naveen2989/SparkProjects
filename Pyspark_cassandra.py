

import datetime as dt
import sys
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import*
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
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sqlContext.read\
    .format("org.apache.spark.sql.cassandra")\
    .option("keyspace","spark_demo")\
    .option("table" ,"weather_station").load("spark_demo.weather_station").show()

    sc.stop()
