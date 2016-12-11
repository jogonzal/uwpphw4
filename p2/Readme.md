For each node, initialize a integer array of the levels where it has been "touched". Only the root node has the first level set to 1, the rest will be -1.

We have the array of visited nodes, and the array of connections.

Example:

IN this example node 3 is root

Nodes:
	[0] Node 0, level -1
	[1] Node 1, level -1
	[2] Node 2, level -1
	[3] Node 3, level 0

Edges (note they are bidirectional)
	[0] 0 - 3
	[1] 1 - 2
	[2] 3 - 0
	[3] 0 - 2
	[4] 2 - 3

The algorithm is as follows.

In every Map-Reduce iteration for each process, we:
1. Iterate over the corresponding edges for the map reduce worker
2. If the node is visited, generate a list of the visited nodes. Return that list.
4. On the reduce step, distinct-merge the list that was generated
5. Mark the elements in the visited array as visited again
6. If none were visited, then back to (1). Otherwise, done.

In the example above, after the first iteration, things will look like this:

Nodes:
	[0] Node 0, level 1
	[1] Node 1, level -1
	[2] Node 2, level -1
	[3] Node 3, level 0
	
Because node 0 and 3 are connected. Then it will look like this:

Nodes:
	[0] Node 0, level 1
	[1] Node 1, level -1
	[2] Node 2, level 2
	[3] Node 3, level 0

And so on. The vertex and nodes can be counted as we increase levels in each node and are returned by each worker.