# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import os
import sys

if sys.platform.startswith('win'):
    #where you downloaded the resource bundle
    os.chdir("C:/Users/Admin/ApacheSpark/SparkPythonDoBigDataAnalytics-Resources")
    #where you installed spark
    os.environ['SPARK_HOME'] = 'C:/spark-2.4.5-bin-hadoop2.7'

os.curdir

#create a variable for our root path
SPARK_HOME = os.environ['SPARK_HOME']

#Adding the following paths to the system path(inserting some lib's to our path)
sys.path.insert(0,os.path.join(SPARK_HOME,"python"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","pyspark.zip"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","py4j-0.10.7-src.zip"))

#Initialize SparkSession and SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext

#Create a Spark Session(starting Spark) 
#Temporary dir ie., Spark uses
SpSession = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("Glabs1") \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max","2") \
    .config("spark.sql.warehouse.dir","file:///c:/temp/spark-warehouse") \
    .getOrCreate()

#Get the Spark Context from Spark Session
SpContext = SpSession.sparkContext

#Test Spark
testData = SpContext.parallelize([3,6,4,2])


testData.count()

#Create an RDD by loading from a file
tweetsRDD = SpContext.textFile("movietweets.csv")

#show top 5 records
tweetsRDD.take(5)

#Transform Data - change to upper case 
ucRDD = tweetsRDD.map( lambda x : x.upper())
ucRDD.take(5)

#Action - Count the number of tweets
tweetsRDD.count()

#Loading and Storing data

#Load from a collection
collData = SpContext.parallelize([4,3,2,5,8])
collData.collect()

#Load the file. lazy initialization
autoData = SpContext.textFile("auto-data.csv")
autoData.cache()

#load only now
autoData.count()
autoData.first()
autoData.take(5)

for line in autoData.collect():
    print(line)

#save to local file. First collect the RDD to the master
# and then save as local file
autoDataFile = open("auto-data-saved.csv","w")   
autoDataFile.write("\n".join(autoData.collect()))  
autoDataFile.close()  


#Transformation


#Map and create a new RDD
tsvData=autoData.map(lambda x : x.replace(",","\t"))
tsvData.take(5)

#filter and create a new RDD
toyotaData=autoData.filter(lambda x: "toyota" in x)
toyotaData.count()

#FlatMap
words=toyotaData.flatMap(lambda line: line.split(","))
words.count()

words.take(20)


#Distinct
for numbData in collData.distinct().collect():
    print(numbData)
    
#Set operations
words1 = SpContext.parallelize(["hello","war","peace","world"])
words2 = SpContext.parallelize(["war","peace","universe"]) 

for unions in words1.union(words2).distinct().collect():
    print(unions)
    
for intersects in words1.intersection(words2).collect():
    print(intersects)


#Using functions for transformation
#cleanse and transform an RDD

def cleanseRDD(autoStr) :
    if isinstance(autoStr, int) :
        return autoStr
    attList=autoStr.split(",")
    #convert doors to a number str
    if attList[3] == "two" :
        attList[3]="2"
    else :
        attList[3]="4"
        
    #convert Drive to uppercase
    attList[5] = attList[5].upper()
    return ",".join(attList)

#
##    Actions
#
 
# reduce - compute the sum
collData.collect()
collData.reduce(lambda x,y: x+y)

#find the shortest line
autoData.reduce(lambda x,y: x if len(x) < len(y) else y)

#Use a function to perform reduce
def getMPG( autoStr) :
    if isinstance(autoStr, int) :
        return autoStr
    attList=autoStr.split(",")
    if attList[9].isdigit() :
        return int(attList[9])
    else:
        return 0
    
#find average MPG-City for all cars
autoData.reduce(lambda x,y : getMPG(x) +getMPG(y)) \
    / (autoData.count()-1)
    
 #
## working with key/value RDDs
#

#create a KV RDD of auto Brand and Horsepower
cylData = autoData.map( lambda x: (x.split(",")[0], \
    x.split(",")[7]))
cylData.take(5)
cylData.keys().collect()


#Remove header row
header = cylData.first()
cylHPData= cylData.filter(lambda line: line != header)
   

#Find average HP by Brand
#Add a count 1 to each record and then reduce to find totals of HP and
addOne = cylHPData.mapValues(lambda x: (x,1))
addOne.collect()

brandValues= addOne \
    .reduceByKey(lambda x,y: (int(x[0]) + int(y[0]), \
    x[1] + y[1]))
brandValues.collect()


#find average by dividing HP total by count total
brandValues.mapValues(lambda x: int(x[0])/int(x[1])). \
    collect()
    
#
##  Advanced Spark : Accumulators & Broadcast variables
#

#function that splits the line as well as counts sedans and hatchbacks 
#Speed optimization

#Initialize accumulator
sedanCount = SpContext.accumulator(0)
hatchbackCount = SpContext.accumulator(0)

#Set Broadcast variable
sedanText=SpContext.broadcast("sedan")
hatchbackText=SpContext.broadcast("hatchback")

def splitLines(line) :
    
    global sedanCount
    global hatchbackCount
    
    #Use broadcast variable to do comparison and set accumulator
    if sedanText.value in line:
        sedanCount +=1
    if hatchbackText.value in line:
        hatchbackCount +=1
        
    return line.split(",")

#do the map
splitData=autoData.map(splitLines)

#Make it execute the map (Lazy execution)
splitData.count()
print(sedanCount, hatchbackCount)

#
##  Advanced Spark : Partitions
#
collData.getNumPartitions()

#Specify no.of partitions
collData=SpContext.parallelize([3,5,4,7,4],4)
collData.cache()
collData.count()

collData.getNumPartitions()

