from __future__ import print_function
import sys
from pyspark.sql import SparkSession
if __name__ == "__main__":
	"""
		Usage: matrixmultiply INPUTFILE
	"""
	spark = SparkSession\
	.builder\
	.appName("PythonPi")\
	.getOrCreate();

	if len(sys.argv) != 2:
		raise ValueError('Usage: matrixmultiply INPUTFILE');
	
	inputFilePath = sys.argv[1];
	
	allNumbersInFile = []
	with open(inputFilePath) as f:
		for line in f:
			inner_list = [elt.strip() for elt in line.split(',')]
			for element in inner_list:
				# print("Converting..." + str(element));
				elementAsIngeger = int(element);
				allNumbersInFile.append(elementAsIngeger);
	
	# print(str(allNumbersInFile));

	dimension = allNumbersInFile[0];
	n = dimension;

	matrix = [None] * dimension * dimension;
	vector = [None] * dimension;
	for i in range(2, dimension * dimension + 2):
		matrix[i - 2] = allNumbersInFile[i];
	for i in range(0, dimension):
		vector[i] = allNumbersInFile[dimension*dimension+2+i];

	def f(index):
		print("Multiplying " + str(index));
		acc = 0;
		for i in range(0, dimension):
			acc += vector[i]*matrix[index * dimension + i];
		return acc;
		
	result = spark.sparkContext.parallelize(range(0, n), 1).map(f).collect(); # Note that there's no need to reduce here
	print("The resulting vector is " + str(result));
	thefile = open('output.txt', 'w');
	thefile.seek(0);
	for item in result:
		thefile.write("%s\n" % item);
	print("Wrote result to output.txt");
	spark.stop();