from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

#structured data file with a header 
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///Users/suhaanibatra/Desktop/Apache-Spark-Tut/fakefriends-header.csv")
    #inferSchema() infers the schema of the DataFrame based on the data
    #header() specifies that the first row of the CSV file contains the column names
print("Here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

spark.stop() #needs to be explicitly stopped to free up resources

