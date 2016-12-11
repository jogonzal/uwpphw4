from __future__ import print_function
import sys
from pyspark.sql import SparkSession
if __name__ == "__main__":
	"""
		Usage: bfs inputfile rootnode
	"""
	spark = SparkSession\
	.builder\
	.appName("PythonPi")\
	.getOrCreate();

	workerCount = 6;
	
	if len(sys.argv) != 3:
		raise ValueError('Usage: bfs INPUTFILE');
	
	inputFilePath = sys.argv[1];
	rootNode = int(sys.argv[2]);

	connectionPairs = [];
	with open(inputFilePath) as f:
		for line in f:
			inner_list = [elt.strip() for elt in line.split(' ')];
			inner_list = filter(None, inner_list);
			if len(inner_list) != 2:
				print(str(inner_list));
				raise ValueError('Expecting text file with 2 elements each line. Found ' + str(len(inner_list)));
			nodeLeftAsInteger = int(inner_list[0]);
			nodeRightAsInteger = int(inner_list[1]);
			connectionPairs.append((nodeLeftAsInteger, nodeRightAsInteger));

	# For quick iteration, you can use this array
	# connectionPairs = [
				# ('a', 'b'),
				# ('a', 'c'),
				# ('c', 'd')
				# # ('i', 'b'), # this means node 0 and 1 are connected
				# # ('h', 'j'),
				# # ('g', 'b'),
				# # ('a', 'b'),
				# # ('f', 'h'),
				# # ('a', 'b'),
				# # ('a', 'c'),
				# # ('a', 'b'),
				# # ('e', 'b'),
				# # ('i', 'd'),
				# # ('a', 'f'),
				# # ('k', 'e'),
				# # ('k', 'z'),
				# # ('a', 'k'),
				# # ('k', 'g'),
				# # ('h', 'e'),
				# # ('k', 'i'),
				# ];
	# rootNode = 'a';
	
	Verbose = True;
	
	def printVerbose(message):
		if Verbose:
			print(message);
	
	def getOrCreateNode(dict, nodeId, nodeCount):
		if nodeId in dict:
			return dict[nodeId], nodeCount;
		dict[nodeId] = nodeCount;
		nodeCount+=1;
		return nodeCount-1, nodeCount;
	
	# First, obtain the array of unique nodes
	nodeDict = dict();
	nodeCount = 0;
	transformedConnections = [];
	inputConnectionCount = len(connectionPairs);
	for i in range(0, inputConnectionCount):
		localConnection = connectionPairs[i];
		nodeLeft, nodeCount = getOrCreateNode(nodeDict, localConnection[0], nodeCount);
		nodeRight, nodeCount = getOrCreateNode(nodeDict, localConnection[1], nodeCount);
		transformedConnections.append((nodeLeft, nodeRight));
	
	connectionCount = len(transformedConnections);
	printVerbose("Nodes: " + str(nodeCount) + " Connections: " + str(connectionCount));
	#printVerbose("Node dictionary: " + str(nodeDict));
	#printVerbose("Transformed connections: " + str(transformedConnections));
	
	nodeVisited = [-1] * nodeCount;
	rootNodePosition = nodeDict[rootNode];
	nodeVisited[rootNodePosition] = 0;
	connectionsArray = [None] * connectionCount;
	offset = 0;
	for connection in transformedConnections:
		connectionsArray[offset] = connection;
		offset+=1;
	
	#printVerbose("Visited dictionary = " + str(nodeVisited));
	
	# At this point, we have an array of connections in "transformedConnections" and the array of visited nodes at "nodeVisited"
	
	# To split the connectionCount into workerCount workers, we'll need to calculate chunk sizes
	genericChunkSize = connectionCount / workerCount;
	chunkSizes = [None] * workerCount;
	for i in range(0, workerCount):
		if (i == (workerCount - 1)):
			chunkSizes[i] = connectionCount - genericChunkSize * (workerCount - 1);
		else:
			chunkSizes[i] = genericChunkSize;
	
	#printVerbose("Chunks: " + str(chunkSizes));

	def f(index):
		nodesFound = set();
		vertexCount = 0;
		initialOffset = index * genericChunkSize;
		endOffset = initialOffset + chunkSizes[index];
		printVerbose(str(index) + "Going from " + str(initialOffset) + " to " + str(endOffset) +". Current level is " + str(currentLevel) + ".");
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
		print(str(index) + "Done!");
		return (nodesFound, vertexCount);
	
	def reducerFunction(a, b):
		newSet = a[0].union(b[0]);
		newVertexCount = a[1] + b[1];
		return (newSet, newVertexCount);
	
	currentLevel = 0;
	shouldContinue = True;
	globalVertexCount = 0;
	print("Starting loop...");
	while shouldContinue:
		result = spark.sparkContext.parallelize(range(0, workerCount), 1).map(f).reduce(reducerFunction);
		
		# printVerbose("the resulting vector is " + str(result));
		printVerbose("Now reducing...");

		currentLevel+=1;
		
		newNodesFound = result[0];
		globalVertexCount += result[1];
		
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
		#printVerbose("New levels are : " + str(nodeVisited));
		printVerbose("Should continue : " + str(shouldContinue) + ". At level " + str(currentLevel));
	spark.stop();
	
	globalMaxLevel = currentLevel;
	
	finalMessage = ("Graph vertices: " + str(nodeCount) + " with total edges " + str(inputConnectionCount*2) + ". Reached vertices from " + str(rootNode) + " are " + str(globalVertexCount) + " and max level is " + str(globalMaxLevel));
	print(finalMessage);
	thefile = open('output.txt', 'w');
	thefile.seek(0);
	thefile.write(finalMessage);
	print("Wrote result to output.txt");
	spark.stop();