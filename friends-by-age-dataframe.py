from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAgeDataFrame").getOrCreate()

#create a DataFrame from the fakefriends-header.csv file
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///Users/suhaanibatra/Desktop/Apache-Spark-Tut/fakefriends-header.csv")

#print the schema
people.printSchema()

friendsByAge = people.select("age", "friends")


#group by age
friendsByAge.groupBy("age").avg("friends").show()

friendsByAge.groupBy("age").avg("friends").sort("age").show()

friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("average_friends")).sort("age").show()