package Nodes;

import Config.MapReduceProperties;
import Constants.MRConstant;
import CustomException.MapReduceException;
import Model.WorkerStatus;
import ProcessStates.ActiveWorkers;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketOption;
import java.util.*;
import java.util.stream.Collectors;

public class Master {

    String numOfWorkers;
    String inputFilePath;
    String outputFilePath;
    String udfClass;
    int ioPort = 5001;

    public Master(){}

    public void start() throws MapReduceException {
        try {
            System.out.println("Loading properties from config file....");
            MapReduceProperties mrProp = new MapReduceProperties();
            Properties prop = mrProp.getProperties();
            this.numOfWorkers = prop.getProperty("N");
            this.inputFilePath = prop.getProperty("input_file_path");
            this.outputFilePath = prop.getProperty("output_file_path");
            this.udfClass = prop.getProperty("udf_class");
            System.out.println("Properties Loaded => " + prop.values());
            setup(this.udfClass,this.outputFilePath);
            cleanUp(this.udfClass,this.outputFilePath);
            execute();
        }catch(IOException e){
            throw new MapReduceException(e.getMessage());
        }
    }

    public void execute() throws MapReduceException{
        try {
            ServerSocket server = new ServerSocket(this.ioPort);
            System.out.println("Socket Server started");
            ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
            Map<String,List<String>> reducerInputFiles = new HashMap<>();
            System.out.println("Running Mapper!");
            runMapper(activeWorkers, server, reducerInputFiles);
            System.out.println("Mapper Processes are Done!");
            System.out.println("Running Reducer!");
            //System.out.println(reducerInputFiles);
            runReducer(activeWorkers, server, reducerInputFiles);
        }catch(IOException e){
            throw new MapReduceException(e.getMessage());
        }
    }

    private void runMapper(ActiveWorkers activeWorkers, ServerSocket server, Map<String,List<String>> reducerInputFiles) throws MapReduceException {
        try {
            //counting number of lines in input File and splitting lines between number of workers
            int numberOfLines = countLinesFile();
            // Calculating the offset i.e. the max number of lines that each worker should read which is equal to rounding down to the nearest integer of (number of lines)/(number of Workers)
            int offset = (int) Math.ceil((double)numberOfLines/(double) Integer.parseInt(this.numOfWorkers));
            // When the particular Worker should start reading the file
            int startLine = 0;
            int workerId = 0;
            System.out.println("Spawning Mapper Processes");
            while (startLine < numberOfLines) {
                //Last worker should read all the files so increasing the offset value
                if(workerId+1 == Integer.parseInt(this.numOfWorkers)){
                    offset += numberOfLines % Integer.parseInt(this.numOfWorkers);
                }
                String commandList[] = {"java", "-cp", "src/",
                        MRConstant.WORKER_JAVA_LOCATION,
                        String.valueOf(workerId),
                        String.valueOf(this.ioPort),
                        MRConstant.MAPPER,
                        this.inputFilePath,
                        this.udfClass,
                        String.valueOf(startLine),
                        String.valueOf(offset),
                        "",
                        this.numOfWorkers
                };
                // creating worker node process with workerType = Mapper
                ProcessBuilder processBuilder = new ProcessBuilder(commandList);
                processBuilder.inheritIO();
                //starting process
                Process process = processBuilder.start();
                activeWorkers.isActiveWorker.add(workerId);
                startLine += offset;
                workerId++;
            }
            ObjectInputStream ois = null;
            String line = "";
            Map<String,String> outputFileMap = null;
            Socket socket=null;
            while (!activeWorkers.isActiveWorker.isEmpty()) {
                // takes input from the client socket
                socket = server.accept();
                ois = new ObjectInputStream(socket.getInputStream());
                WorkerStatus status = (WorkerStatus) ois.readObject();
                System.out.println("Received From Mapper ==> " + status);
                if (MRConstant.SUCCESS.equals(status.getStatus())) {
                    activeWorkers.isActiveWorker.remove(status.getWorkerId());
                    //Getting intermediate File Path from Mapper which is a map containing the reducer_id that the file is expected to read from
                    outputFileMap = status.getFilePath();
                    for (Map.Entry<String, String> outputFileEntry : outputFileMap.entrySet()) {
                        if (!reducerInputFiles.containsKey(outputFileEntry.getKey())) {
                            reducerInputFiles.put(outputFileEntry.getKey(), new ArrayList<>());
                        }
                        reducerInputFiles.get(outputFileEntry.getKey()).add(outputFileEntry.getValue());
                    }
                }
            }
            socket.close();
        }catch (Exception e){
            e.printStackTrace();
            throw new MapReduceException(e.getMessage());
        }
    }

    private void runReducer(ActiveWorkers activeWorkers, ServerSocket server, Map<String,List<String>> reducerInputFiles) throws MapReduceException {
        try {
            int workerId = 0;
            String reducerFilesStr = null;
            int reducers = 0;
            while (reducers < Integer.parseInt(this.numOfWorkers)) {
                /*Sending the location of the files that a particular reducer should read.
                As the reducerInputFiles map data structure contains the reducer_id as the key which will always be between 0 and N-1,
                we can use the getOrDefault() function to get the corresponding intermediate files for the reducer. The hash function in the Mapper class
                makes sure that each key is matched to exactly one Reducer
                */
                reducerFilesStr = String.join(",",reducerInputFiles.getOrDefault(String.valueOf(workerId),new ArrayList<>()));
                //System.out.println("reducerFilesStr===>"+reducerFilesStr);
                String commandList[] = {"java", "-cp", "src/",
                        MRConstant.WORKER_JAVA_LOCATION,
                        String.valueOf(workerId),
                        String.valueOf(this.ioPort),
                        MRConstant.REDUCER,
                        reducerFilesStr,
                        this.udfClass,
                        "",
                        "",
                        this.outputFilePath,
                        this.numOfWorkers
                };
                // creating worker node process with workerType = Reducer
                ProcessBuilder processBuilder = new ProcessBuilder(commandList);
                processBuilder.inheritIO();
                Process process = processBuilder.start();
                activeWorkers.isActiveWorker.add(workerId);
                workerId++;
                reducers++;
            }
            ObjectInputStream ois = null;
            Socket socket = null;
            while (!activeWorkers.isActiveWorker.isEmpty()) {
                // takes input from the client socket which is the reducer node in this case
                socket = server.accept();
                ois = new ObjectInputStream(socket.getInputStream());
                Object obj = ois.readObject();
                if(obj!=null) {
                    WorkerStatus status = (WorkerStatus) obj;
                    System.out.println("Received From Reducer ==> " + status);
                    if (MRConstant.SUCCESS.equals(status.getStatus())) {
                        activeWorkers.isActiveWorker.remove(status.getWorkerId());
                    }
                }
            }

        }catch(Exception e){
            e.printStackTrace();
            throw new MapReduceException(e.getMessage());
        }
    }

    private int countLinesFile() throws FileNotFoundException {
        int count = 0;
        File file = new File(this.inputFilePath);
        Scanner sc = new Scanner(file);
        while(sc.hasNextLine()) {
            sc.nextLine();
            count++;
        }
        sc.close();
        return count;
    }

    private void setup(String udfClass, String outputFilePath) {
        createOutputFolder(udfClass,outputFilePath);
        createIntermediateFolder(udfClass,outputFilePath);
    }

    private void createOutputFolder(String udfClass, String outputFilePath) {
        File newDir = new File(outputFilePath+"/"+udfClass);
        if(!newDir.exists()){
            newDir.mkdirs();
        }
    }

    private void createIntermediateFolder(String udfClass, String outputFilePath) {
        File newDir = new File(MRConstant.MAP_OUTPUT_DIR + udfClass);
        if(!newDir.exists()){
            newDir.mkdirs();
        }
    }

    private void cleanUp(String udfClass, String outputFilePath) {
        deleteIntermediateFiles(udfClass,outputFilePath);
        deleteOutputFiles(udfClass,outputFilePath);
    }

    private void deleteOutputFiles(String udfClass, String outputFilePath) {
        File dir = new File(outputFilePath+"/"+udfClass);
        File[] listOfFiles = dir.listFiles();
        if(listOfFiles==null || listOfFiles.length==0){
            return;
        }
        for (File file: listOfFiles) {
            if (!file.isDirectory()) {
                file.delete();
            }
        }
    }

    private void deleteIntermediateFiles(String udfClass, String outputFilePath) {
        File dir = new File(MRConstant.MAP_OUTPUT_DIR + udfClass);
        File[] listOfFiles = dir.listFiles();
        if(listOfFiles==null || listOfFiles.length==0){
            return;
        }
        for (File file: listOfFiles) {
            if (!file.isDirectory()) {
                file.delete();
            }
        }
    }



}
