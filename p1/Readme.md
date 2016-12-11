To solve matrix-vector multiply in spark, we simply kick off N spark tasks - every one of them calculates the multiplication of one row against the entire vector.

At the end of the map stage, we reduce by aggregating data in a list.