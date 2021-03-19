package Nodes;

import Config.MapReduceProperties;
import Constants.MRConstant;

import java.io.IOException;
import java.util.Properties;

public class Master {

    String numOfWorkers;
    String inputFilePath;
    String outputFilePath;
    String udfClass;

    public Master(){}

    public void start() throws IOException {
        MapReduceProperties mrProp = new MapReduceProperties();
        Properties prop = mrProp.getProperties();
        this.numOfWorkers = prop.getProperty("N");
        this.inputFilePath = prop.getProperty("input_file_path");
        this.outputFilePath = prop.getProperty("output_file_path");
        this.udfClass = prop.getProperty("udf_class");
    }

    public void execute(){
        /*TODO:
    1. Read from config file using Properties which creates a map of sorts
    2. Read number of lines in the file 'K'-> K/N is the offset, startline = startline + offset
    2. Create N mapper (Worker class) processes and update ActiveWorkers
    3. Wait for N mapper process to get completed
    4. While(True) for 2 and 3
    5. On completion create N reducer (Worker class) processes and update ActiveWorkers
    6. Wait for N reducer process to get completed
    7. While(True) for 5 and 6
    8. Terminate
    */

    }
}
