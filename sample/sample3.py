from __future__ import print_function
import sys
from random import random
from operator import add
from pyspark.sql import SparkSession
if __name__ == "__main__":
	"""
		Usage: pi [partitions]
	"""
	spark = SparkSession\
	.builder\
	.appName("PythonPi")\
	.getOrCreate();
	partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2;
	n = 100000 * partitions;
	
	def f(index):
		return n;
		
	count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add);
	print("The sum of what I got is %d" % (count));
	spark.stop();