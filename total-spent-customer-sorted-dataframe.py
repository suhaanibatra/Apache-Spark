from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

#create a SparkSession
spark = SparkSession.builder.appName("TotalSpentCustomerSortedDataFrame").getOrCreate()

#create a custom schema for the data as it does not have a header
schema = StructType([StructField("customerID", IntegerType(), True), StructField("itemID", IntegerType(), True), StructField("amountSpent", FloatType(), True)])

#read the csv file into a dataframe
df = spark.read.schema(schema).csv("file:///Users/suhaanibatra/Desktop/Apache-Spark-Tut/customer-orders.csv")

amountByCustomer = df.select("customerID", "amountSpent")
totalByCustomer = df.groupBy("customerID").agg(func.round(func.sum("amountSpent"),2).alias("totalSpent"))
totalByCustomerSorted = totalByCustomer.sort("totalSpent")

totalByCustomerSorted.show()
totalByCustomerSorted.show(totalByCustomerSorted.count())

spark.stop()