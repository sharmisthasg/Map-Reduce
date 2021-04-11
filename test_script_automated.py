#!/usr/bin/env python
# coding: utf-8

# In[1]:

import sys
import os
import shutil
import glob

# In[ ]:


# In[2]:


def compare(mr_file_path, py_file_path):
    mr_file = open(mr_file_path)
    mr_output_list = []
    for line in mr_file:
        mr_output_list.append(line)
    
    py_file = open(py_file_path)
    py_output_list = []
    for line in py_file:
        py_output_list.append(line)
    
    for line in mr_output_list:
        if line not in py_output_list:
            return False
        
    for line in py_output_list:
        if line not in mr_output_list:
            return False
        
    return True

def list_of_sorted_elements(a):
    t = a.split(",")
    u = t[0].split(" ")
    for i in range(1,len(t)):
        u.append(t[i].strip())
    u = u.sort()
    return u

def compare_inverted_index(outfile_path, py_file_path):
    outfile = open(outfile_path)
    out_list = []
    for line in outfile:
        sorted = list_of_sorted_elements(line)
        out_list.append(sorted)

    pythonfile = open(py_file_path)
    python_list = []
    for line in pythonfile:
        sorted = list_of_sorted_elements(line)
        python_list.append(sorted)

    for line in out_list:
        if line not in python_list:
            return False

    for line in python_list:
        if line not in out_list:
            return False

    return True
# In[ ]:


# In[3]:


os.system("javac -cp src/ src/Main.java src/Nodes/Worker.java src/TestCases/*.java")


# In[4]:


udfList = ["WordCount", "DistributedGrep", "InvertedIndex"]


# In[5]:
N = input("Enter the number of desired mappers and reducers: ")

result = True
udf_failed = ''
for udf in udfList:
    print("-"*5 + udf + "-"*5)
    f = open("resources/config.properties","w")
    f.write("N="+str(N)+"\n")
    f.write("input_file_path=data/demo.txt\n")
    f.write("output_file_path=output/\n")
    f.write("udf_class="+udf)
    f.close()
    os.system("java -cp src/ Main")
    print("Comparing MapReduce Output File with files generated using python script")
    os.chdir("output/"+udf)
    outfilename = "outfile.txt"
    with open(outfilename, 'wb') as outfile:
        for filename in glob.glob('*.txt'):
            if filename == outfilename:
                continue
            with open(filename, 'rb') as readfile:
                shutil.copyfileobj(readfile, outfile)
    os.chdir(sys.path[0])
    if udf == "InvertedIndex":
        result = compare_inverted_index("output/"+ udf + "/outfile.txt","test_scripts/" + udf+".txt")
    else:
        result &= compare("output/"+ udf + "/outfile.txt","test_scripts/" + udf+".txt")
    if result:
        print(udf + " Comparison is Successful")
    if not result:
        udf_failed = udf
        break
    os.chdir("output/"+udf)
    os.remove("outfile.txt")
    os.chdir(sys.path[0])
    print("-"*20)
if(result):
    print("All the Test Cases have passed Successfully")
    print("MapReduce Works as Expected!")
else:
    print("Test Case Failed: ",udf_failed)


# In[ ]: