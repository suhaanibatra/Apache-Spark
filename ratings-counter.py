from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram") #local is a Spark cluster that runs on a single machine
sc = SparkContext(conf = conf) #sc is the SparkContext object

lines = sc.textFile("file:///Users/suhaanibatra/Desktop/Apache-Spark-Tut/ml-100k/u.data") #this creates a RDD from the u.data file
ratings = lines.map(lambda x: x.split()[2]) #this maps each line to the rating column
result = ratings.countByValue() #this counts the number of occurrences of each rating

sortedResults = collections.OrderedDict(sorted(result.items())) #this sorts the results by rating
for key, value in sortedResults.items(): #this prints the results
    print("%s %i" % (key, value))
