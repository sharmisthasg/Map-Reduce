# Map Reduce Framework
p1_mapreduce-team-9 created by GitHub Classroom

Directory Structure:
1. src/ - contains Map Reduce Java code
2. data/ - contains input files (we have one input file - demo.txt for all test cases)
3. output/ - contains output files from Map Reduce in the respective udfclass folders. THe files are named using the convention reducer_id.txt where every reducer generates one output file
4. intermediate/ - contains intermediate files generated during Map Reduce (output of Mapper) in the respective udfclass folders. The files are named using the convention mapper_id-offset_value-hashkey.txt
5. test_scripts/ - contains Python scripts and their output files for each Map Reduce testcase
6. test_script_automated.py - script for performing automated testing
    - Compiles Java code to generate .class files
    - Executes Java Map Reduce to generate output files for each of the testcase
    - Compares the output of Java Map Reduce and Python scripts for verification

Test cases:
1. Word Count
2. Inverted Index
3. Distributed Grep

All UDFs are inside the folder src/TestCases/

Command to run in root directory for automated testing: </br>
**python3 test_script_automated.py** </br>
The user will then be prompted to enter the number of desired mappers and reducers, N. 

Note:
- For all testcases, we assume the line index in the input file as the document id
- For Distributed grep the pattern to be matched is currently "map" (variable name: pattern). In case this has to be changed, the changes need to be made in src/TestCases/DistributedGrep.java and test_scripts/distributed_grep.py
- In case the input file (data/demo.txt) needs to be changed for different inputs, the python scripts in test_scripts/ must be re-executed to generate the updated outputs for comparison with the Java Map Reduce outputs (Command (to be run from test_script directory): python3 word_count.py, python3 inverted_index.py, python3 distributed_grep.py)
- We use a hash function to map the intermediate keys to the respective reducers. The hash function we have implemented adds the ascii value of the characters of the key and returns its mod N value as the hash key (where N is the number of reducers). This ensures that each key is assigned to one reducer only
- It is possible that a reducer has no key to reduce because of the hash value of the keys
