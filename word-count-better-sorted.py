import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///Users/suhaanibatra/Desktop/Apache-Spark-Tut/book.txt")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y) #convert each word to a key/value pair and reduce by key
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey() #sort the word counts by key, counts become the key and words become the value
results = wordCountsSorted.collect() #collect the results

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
