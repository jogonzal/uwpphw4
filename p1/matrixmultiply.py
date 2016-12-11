from __future__ import print_function
import sys
from pyspark.sql import SparkSession
if __name__ == "__main__":
	"""
		Usage: matrixmultiply
	"""
	spark = SparkSession\
	.builder\
	.appName("PythonPi")\
	.getOrCreate();
	# Assumming we already have the matrix in a variable
	# Matrix is 4x4
	matrix = [	0, 1, 2, 3,
				4, 5, 6, 7,
				8, 9, 10, 11,
				12, 13, 14, 15];
	# Vector
	vector = [0, 1, 2, 3];
	
	dimension = len(vector);
	n = dimension;
	
	def f(index):
		print("Multiplying " + str(index));
		acc = 0;
		for i in range(0, dimension):
			acc += vector[i]*matrix[index * dimension + i];
		return acc;
		
	result = spark.sparkContext.parallelize(range(0, n), 1).map(f).collect();
	print("The resulting vector is " + str(result));
	spark.stop();