from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpentByCustomerSorted")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amountSpent = float(fields[2])
    return (customerID, amountSpent)

lines = sc.textFile("file:///Users/suhaanibatra/Desktop/Apache-Spark-Tut/customer-orders.csv")
parsedLines = lines.map(parseLine)
totalByCustomer = parsedLines.reduceByKey(lambda x, y: x + y)
totalByCustomerSorted = totalByCustomer.map(lambda x: (x[1], x[0])).sortByKey()

results = totalByCustomerSorted.collect()
for result in results:
    print(f"Customer {result[1]} spent {result[0]}\n")