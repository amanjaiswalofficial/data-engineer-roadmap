#Boilerplate stuff:
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

# The characters we wish to find the degree of separation between:
startCharacterID = 5306 #SpiderMan
targetCharacterID = 14  #ADAM 3,031 (who?)

# Our accumulator, used to signal when we find the target character during
# our BFS traversal.
hitCounter = sc.accumulator(0)

def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))
    color = 'WHITE'
    distance = 9999
    if (heroID == startCharacterID):
        color = 'GRAY'
        distance = 0
    return (heroID, (connections, distance, color))


def createStartingRdd():
    inputFile = sc.textFile("E:/Projects/data-engineering-learning-path/07_pyspark_demos/Marvel_Graph.txt")
    return inputFile.map(convertToBFS)

def bfsMap(node):
    characterID = node[0] # (123)
    data = node[1] # ([], 9999, WHITE)
    connections = data[0] # []
    distance = data[1] # 9999
    color = data[2] # WHITE/GRAY
    results = []
    if (color == 'GRAY'): # IF ROW's of the start character itself
        for connection in connections: # for each of the connection in the list
            newCharacterID = connection 
            newDistance = distance + 1 # increase distance
            newColor = 'GRAY' # set color to gray
            if (targetCharacterID == connection): # if we find the target character
                hitCounter.add(1) # increment accumulator
            newEntry = (newCharacterID, ([], newDistance, newColor)) # create a new object
            results.append(newEntry) # append this to the final list
        color = 'BLACK' # set color of this item to black, assuming we've processed this one
    results.append( (characterID, (connections, distance, color)) ) # append this back to the result
    return results # return

def bfsReduce(data1, data2):
    """
    Comparing 2 rows data (value)
    """
    edges1 = data1[0] # all values from list()
    edges2 = data2[0]
    distance1 = data1[1] # distance amount
    distance2 = data2[1]
    color1 = data1[2] # color
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # choosing the maximum edge length
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Preserve minimum distance
    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    # return same row like object as response in order to reduce
    return (edges, distance, color)


#Main program here:
iterationRdd = createStartingRdd()

for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration+1))

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done.
    mapped = iterationRdd.flatMap(bfsMap)

    # Note that mapped.count() action here forces the RDD to be evaluated, and
    # that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")

    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
            + " different direction(s).")
        break

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce)
