import sys,os
import re
from pyspark import SparkContext, SparkConf

par_dir=os.path.split(os.getcwd())[0]
di={}

f = open(par_dir+"/data/demo.txt","r")
for i,line in enumerate(f):
	line = line.replace("\n","")
	line = re.sub(r'[^a-zA-Z0-9 ]', '', line)
	words = line.split(" ")
	for word in words:
		word = word.lower()
		if word in di:
			if i not in di[word]:
				di[word].append(i)
		else:
			di[word]=[i]

f.close()

f = open("InvertedIndex.txt","w")
for word in di:
	f.write(word+" ")
	for i,index in enumerate(di[word]):
		f.write(str(index))
		if i!=(len(di[word])-1):
			f.write(",")
	f.write("\n")
f.close()
