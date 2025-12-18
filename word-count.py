from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///Users/suhaanibatra/Desktop/Apache-Spark-Tut/book.txt")
words = input.flatMap(lambda x: x.split()) #this splits the text into words by the space
wordCounts = words.countByValue() #this counts the number of occurrences of each word

for word, count in wordCounts.items(): #this prints the results
    cleanWord = word.encode('ascii', 'ignore') #this encodes the word to ASCII
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count)) #this prints the word and the count
