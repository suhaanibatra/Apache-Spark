from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge") #local is a Spark cluster that runs on a single machine
sc = SparkContext(conf = conf) #sc is the SparkContext object

def parseLine(line):
    fields = line.split(',') #this splits the line into fields by the comma
    age = int(fields[2]) #this converts the age to an integer
    numFriends = int(fields[3]) #this converts the number of friends to an integer
    return (age, numFriends) #this returns the age and number of friends as a tuple

lines = sc.textFile("file:///Users/suhaanibatra/Desktop/Apache-Spark-Tut/fakefriends.csv") #this creates a RDD from the fakefriends.csv file
rdd = lines.map(parseLine) #this maps each line to the parseLine function
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) #this reduces the RDD by age and sums the number of friends, first action
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1]) #this calculates the average number of friends by age
results = averagesByAge.collect() #this collects the results as a list
for result in results: #this prints the results
    print(result) 
