from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
import sys

_, source = sys.argv

conf = SparkConf().setAppName("spark-people")
sc = SparkContext(conf=conf)

people_rdd = sc.textFile(source)
cities_rdd = people_rdd.map(lambda line: line.split()[-1])

city_count = cities_rdd.map(lambda city: (city, 1)).reduceByKey(lambda a, b: a + b)

result = city_count.collect()

print(f"\n{result}\n")


sc.stop()
