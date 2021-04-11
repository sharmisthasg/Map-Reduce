package Nodes;

import Constants.MRConstant;
import DataType.KeyValuePair;
import Factory.WorkerFactory;
import Service.MRService;
import Service.Mapper;
import Service.Reducer;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Worker{

    public static void main(String[] args) {
        /*
         * This is the main method of Worker that calls Mapper or Reducer.
         */
        try {
            System.out.println("Initiating Worker Process");
            int workerId = Integer.parseInt(args[0]);
            int ioPort = Integer.parseInt(args[1]);
            String workerType = args[2];
            String inputFilePathStr = args[3];
            List<String> inputFilePath = inputFilePathStr.isEmpty()? new ArrayList<>(): Arrays.asList(inputFilePathStr.split(","));
            String udfClass = args[4];
            String startLine = args[5];
            String offset = args[6];
            String outputFilePath = args[7];
            String numberOfWorkers = args[8];
            WorkerFactory workerFactory = new WorkerFactory();
            MRService mapperReducerService = workerFactory.getMapperReducerFactory(workerId, workerType, ioPort, inputFilePath, udfClass, outputFilePath, startLine, offset, numberOfWorkers);
            mapperReducerService.toString();
            mapperReducerService.execute();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
