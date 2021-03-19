package TestCases;

import DataType.IntComp;
import DataType.StringComp;
import Model.Output;
import Service.UDFInterface;

public class WordCount implements UDFInterface<StringComp, StringComp, StringComp, IntComp> {

    @Override
    public void map(StringComp key, StringComp value, Output output) {
        //String[] words = split(value
    }

    @Override
    public void reduce(StringComp key, Iterable<IntComp> valueIter, Output output) {

    }
}


/*
1. Write code for Master
2. Write code for Mapper BE
3. Write code for Reducer BE

4. Writing WordCount MR Job
5. Writing InvertedIndex MR Job
6. Writing DistributedGrep MR Job

7. Writing Automated Test Script which will:
 a)Trigger Main.java for MR Job
 b)compare MR(4-6) output files with Spark(8) output files

8. Writing Spark jobs for each of 4,5,6
 */