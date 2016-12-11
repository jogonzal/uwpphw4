from __future__ import print_function
import sys
from pyspark.sql import SparkSession
if __name__ == "__main__":
	"""
		Usage: bfs
	"""
	spark = SparkSession\
	.builder\
	.appName("PythonPi")\
	.getOrCreate();
	# Assumming we already have the matrix in a variable
	# Matrix is 4x4
	connectionPairs = [
				('i', 'b'), # this means node 0 and 1 are connected
				('h', 'j'),
				('g', 'b'),
				('a', 'b'),
				('f', 'h'),
				('a', 'b'),
				('a', 'c'),
				('a', 'b'),
				('e', 'b'),
				('i', 'd'),
				('a', 'f'),
				('k', 'e'),
				('k', 'z'),
				('a', 'k'),
				('k', 'g'),
				('h', 'e'),
				('k', 'i'),
				];

	def getOrCreateNode(dict, nodeId, nodeCount):
		if nodeId in dict:
			return dict[nodeId], nodeCount;
		dict[nodeId] = nodeCount;
		nodeCount+=1;
		return nodeCount-1, nodeCount;
	
	# First, obtain the array of unique nodes
	nodeDict = dict();
	connectionCount = len(connectionPairs);
	nodeCount = 0;
	transformedConnections = set();
	for i in range(0, connectionCount):
		localConnection = connectionPairs[i];
		nodeLeft, nodeCount = getOrCreateNode(nodeDict, localConnection[0], nodeCount);
		nodeRight, nodeCount = getOrCreateNode(nodeDict, localConnection[1], nodeCount);
		transformedConnections.add((nodeLeft, nodeRight));
	
	print("Nodes: " + str(nodeCount) + " Connections: " + str(connectionCount));
	print(str(nodeDict));
	print(str(transformedConnections));
	
	nodeVisited = [-1] * nodeCount;
	
	# At this point, we have an array of connections in "transformedConnections" and the array of visited nodes at "nodeVisited"

	# def f(index):
		# print("Multiplying " + str(index));
		# acc = 0;
		# for i in range(0, dimension):
			# acc += vector[i]*matrix[index * dimension + i];
		# return acc;
		
	# result = spark.sparkContext.parallelize(range(0, n), 1).map(f).collect();
	# print("The resulting vector is " + str(result));
	# spark.stop();