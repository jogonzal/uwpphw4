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
	
	# Assumming we already have the connections in a variable
	connectionPairs = [
				# ('a', 'b'),
				# ('a', 'c'),
				# ('c', 'd')
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

	Verbose = False;
	rootNode = 'd';
	
	def printVerbose(message):
		if Verbose:
			printVerbose(message);
	
	def getOrCreateNode(dict, nodeId, nodeCount):
		if nodeId in dict:
			return dict[nodeId], nodeCount;
		dict[nodeId] = nodeCount;
		nodeCount+=1;
		return nodeCount-1, nodeCount;
	
	# First, obtain the array of unique nodes
	nodeDict = dict();
	nodeCount = 0;
	transformedConnections = set();
	inputConnectionCount = len(connectionPairs);
	for i in range(0, inputConnectionCount):
		localConnection = connectionPairs[i];
		nodeLeft, nodeCount = getOrCreateNode(nodeDict, localConnection[0], nodeCount);
		nodeRight, nodeCount = getOrCreateNode(nodeDict, localConnection[1], nodeCount);
		transformedConnections.add((nodeLeft, nodeRight));
	
	connectionCount = len(transformedConnections);
	printVerbose("Nodes: " + str(nodeCount) + " Connections: " + str(connectionCount));
	printVerbose("Node dictionary: " + str(nodeDict));
	printVerbose("Transformed connections: " + str(transformedConnections));
	
	nodeVisited = [-1] * nodeCount;
	rootNodePosition = nodeDict[rootNode];
	nodeVisited[rootNodePosition] = 0;
	connectionsArray = [None] * connectionCount;
	offset = 0;
	for connection in transformedConnections:
		connectionsArray[offset] = connection;
		offset+=1;
	
	printVerbose("Visited dictionary = " + str(nodeVisited));
	
	# At this point, we have an array of connections in "transformedConnections" and the array of visited nodes at "nodeVisited"
	
	# To split the connectionCount into workerCount workers, we'll need to calculate chunk sizes
	workerCount = 10;
	genericChunkSize = connectionCount / 10;
	chunkSizes = [None] * 10;
	for i in range(0, workerCount):
		if (i == (workerCount - 1)):
			chunkSizes[i] = connectionCount - genericChunkSize * (workerCount - 1);
		else:
			chunkSizes[i] = genericChunkSize;
	
	printVerbose("Chunks: " + str(chunkSizes));


	def f(index):
		nodesFound = set();
		vertexCount = 0;
		initialOffset = index * genericChunkSize;
		endOffset = initialOffset + chunkSizes[index];
		printVerbose("Going from " + str(initialOffset) + " to " + str(endOffset) +". Current level is " + str(currentLevel) + ".");
		for i in range(initialOffset, endOffset):
			localConnection = connectionsArray[i];
			nodeLeft = localConnection[0];
			nodeRight = localConnection[1];
			if nodeVisited[nodeLeft] == currentLevel and nodeVisited[nodeRight] == -1:
				nodesFound.add(nodeRight);
				vertexCount+=1;
			if nodeVisited[nodeRight] == currentLevel and nodeVisited[nodeLeft] == -1:
				nodesFound.add(nodeLeft);
				vertexCount+=1;
		return (nodesFound, vertexCount);
		
	currentLevel = 0;
	shouldContinue = True;
	globalVertexCount = 0;
	while shouldContinue:
		result = spark.sparkContext.parallelize(range(0, workerCount), 1).map(f).collect();
		
		printVerbose("the resulting vector is " + str(result));
		
		currentLevel+=1;
		
		newNodesFound = set();
		for individualResult in result:
			individualSet = individualResult[0];
			for element in individualSet:
				newNodesFound.add(element);
			localVertexCount = individualResult[1];
			globalVertexCount+=localVertexCount;
			
		discoveredNewNodes = False;
		for newNodeFound in newNodesFound:
			if nodeVisited[newNodeFound] == -1:
				discoveredNewNodes = True;
				nodeVisited[newNodeFound] = currentLevel;
			else:
				raise ValueError('NodeVisited was about to be set where level != -1!');
		nodesLeftToDiscover = False;
		for i in range(0, nodeCount):
			if nodeVisited[i] == -1:
				nodesLeftToDiscover = True;
				break;
	
		# Continue if there are more 1's to discover and if there were some nodes visited
		shouldContinue = nodesLeftToDiscover and discoveredNewNodes;
		printVerbose("New levels are : " + str(nodeVisited));
		printVerbose("Should continue : " + str(shouldContinue));
	spark.stop();
	
	globalMaxLevel = currentLevel;
	
	print("Graph vertices: " + str(nodeCount) + " with total edges " + str(inputConnectionCount*2) + ". Reached vertices from " + str(rootNode) + " are " + str(globalVertexCount) + " and max level is " + str(globalMaxLevel));