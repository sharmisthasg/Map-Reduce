package Nodes;

import Constants.MRConstant;
import DataType.KeyValuePair;

import java.io.FileWriter;
import java.util.ArrayList;

public class Worker{



}

abstract class Mapper{

    int startLine;
    int offset;
    String inputpath;
    String outputpath;
    ArrayList<KeyValuePair> kvpairs;

    public Mapper(int startLine, int offset, String inputpath, String outputpath)
    {
        this.startLine=startLine;
        this.offset=offset;
        this.inputpath=inputpath;
        this.outputpath=outputpath;
    }

    /*
    * 1. Read input file from start to start+offset
    * 2. Generate <k,v> pairs for data:
    *       1. Read one line of input
    *       2. Apply user defined function to that line
    *       3. Aggregate for other lines
    * 3. Write to intermediate file
    * */

    public abstract void map_execute();

    public void write()
    {
        try {
            String filename = "intermediate-mapper-"+String.valueOf(startLine)+"-"+String.valueOf(offset)+".txt";
            FileWriter fileWriter = new FileWriter("intermediate/"+filename);

            for(KeyValuePair kp: kvpairs)
            {
                fileWriter.write(kp.toKeyValueString());
            }

            fileWriter.close();
        }catch (Exception e){
            System.out.println(MRConstant.FILE_WRITE_EXCEPTION);
        }
    }

}