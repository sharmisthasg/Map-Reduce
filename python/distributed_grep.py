import os
from pyspark import SparkContext

sc = SparkContext("local", "Distributed Grep")

par_dir=os.path.split(os.getcwd())[0]
textFile = sc.textFile(par_dir+"/data/demo.txt")
grep_lines = textFile.filter(lambda row: "is" in row).collect()

f = open("DistributedGrep.txt","w")
for line in grep_lines:
	f.write(line)
	f.write("\n")
f.close()