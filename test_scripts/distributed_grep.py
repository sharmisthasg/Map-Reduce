import os
import re
from pyspark import SparkContext

sc = SparkContext("local", "Distributed Grep")
pattern = "map"
par_dir=os.path.split(os.getcwd())[0]
textFile = sc.textFile(par_dir+"/data/demo.txt")
grep_lines = textFile.map(lambda line: re.sub(r'[^a-zA-Z0-9 ]', '', line).lower()).filter(lambda line: "map" in line).collect()

f = open("DistributedGrep.txt","w")
for line in grep_lines:
	f.write(line)
	f.write("\n")
f.close()
