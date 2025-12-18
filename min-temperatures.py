from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1
    #temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0 #this converts the temperature to Fahrenheit
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///Users/suhaanibatra/Desktop/Apache-Spark-Tut/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1]) #this filters the RDD to only include the minimum temperatures
stationTemps = minTemps.map(lambda x: (x[0], x[2])) #this maps the RDD to the station ID and temperature
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y)) #this reduces the RDD by station ID and finds the minimum temperature
results = minTemps.collect(); #this collects the results as a list

for result in results:
    print(result[0] + "\t{:.2f}C".format(result[1]))
