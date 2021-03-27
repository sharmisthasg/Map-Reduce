#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os


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
    return True


# In[ ]:





# In[3]:


os.system("javac -cp src/ src/Main.java src/Nodes/Worker.java src/TestCases/*.java")


# In[4]:


udfList = ["WordCount", "DistributedGrep", "InvertedIndex"]


# In[5]:


result = True
udf_failed = ''
for udf in udfList:
    f = open("resources/config.properties","w")
    f.write("N=1\n")
    f.write("input_file_path=data/demo.txt\n")
    f.write("output_file_path=output/output-" + udf + ".txt\n")
    f.write("udf_class="+udf)
    f.close()
    os.system("java -cp src/ Main")
    result &= compare("output/output-" + udf + ".txt","python/" + udf+".txt")
    if not result:
        udf_failed = udf
        break

if(result):
    print("All the Test Cases have passed Successfully")
    print("MapReduce Works as Expected!")
else:
    print("Test Case Failed: ",udf_failed)


# In[ ]:




