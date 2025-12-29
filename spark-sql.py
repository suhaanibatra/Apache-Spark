from pyspark.sql import SparkSession 
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate() #getOrCreate() creates a SparkSession if one doesn't exist, otherwise it uses the existing one -- used instead of sparkContext

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("fakefriends.csv") #this creates a RDD from the fakefriends.csv file by reading the file line by line 
people = lines.map(mapper) #dataframe is a collection of row objects with each row having a set of columns

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache() #createDataFrame() creates a DataFrame from the RDD
schemaPeople.createOrReplaceTempView("people") #createOrReplaceTempView() registers the DataFrame as a table in the database

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
  print(teen)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
