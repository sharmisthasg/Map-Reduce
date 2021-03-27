import sys,os
import re
from pyspark import SparkContext, SparkConf

par_dir=os.path.split(os.getcwd())[0]

sc = SparkContext("local","PySpark Word Count Exmaple")
words = sc.textFile(par_dir+"/data/demo.txt").flatMap(lambda line: re.sub(r'[^a-zA-Z0-9 ]', '', line).split(" "))

wordCounts = words.map(lambda word: (word.lower(), 1)).reduceByKey(lambda a,b:a +b)

f = open("WordCount.txt","w")
for k,v in wordCounts.collect():
	f.write(str(k)+" "+str(v))
	f.write("\n")
f.close()

