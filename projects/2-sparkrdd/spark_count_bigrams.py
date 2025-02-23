from pyspark import SparkContext, SparkConf
import sys

_, source = sys.argv

conf = SparkConf().setAppName("spark-count-bigrams")
sc = SparkContext(conf=conf)

lines_rdd = sc.textFile(source)
bigrams_rdd = lines_rdd.flatMap(lambda line: zip(line.split(), line.split()[1:]))
bigram_counts_rdd = bigrams_rdd.map(lambda bigram: (bigram, 1)).reduceByKey(
    lambda a, b: a + b
)

result = bigram_counts_rdd.collect()

print("\nBigrams and their counts:\n")
for bigram, count in result:
    print(f"{bigram}: {count}")

sc.stop()
