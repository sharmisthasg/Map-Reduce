# Map Reduce Framework
p1_mapreduce-team-9 created by GitHub Classroom

Directory Structure:
1. src/ - contains Map Reduce Java code
2. data/ - contains input files (we have one input file - demo.txt for all test cases)
3. output/ - contains output files from Map Reduce
4. intermediate/ - contains intermediate files generated during Map Reduce (output of Mapper)
5. test_scripts/ - contains Python scripts and their output files for each Map Reduce testcase
6. test_script_automated.py/ - script for performing automated testing
    - Compiles Java code to generate .class files
    - Executes Java Map Reduce to generate output files for each of the testcase
    - Executes Python test scripts for the same input for each of the testcase
    - Compares the output of Java Map Reduce and Python scripts for verification

Test cases:
1. Word Count
2. Inverted Index
3. Distributed Grep

Command to run in root directory for automated testing:
python3 test_script_automated.py

Note:
- For all testcases, we assume the line index in the input file as the document id
- For Distributed grep the pattern to be matched is currently "map" (variable name: pattern). In case this has to be changed, the changes need to be made in src/TestCases/DistributedGrep.java and test_scripts/distributed_grep.py
- In case the input file (data/demo.txt) needs to be changed for different inputs, the python scripts in test_scripts/ must be re-executed to generate the updated outputs for comparison with the Java Map Reduce outputs (Command: python3 word_count.py, python3 inverted_index.py, python3 distributed_grep.py)
